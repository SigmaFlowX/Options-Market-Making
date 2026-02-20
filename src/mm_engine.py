import asyncio
import os
import aiohttp
import json
from datetime import datetime, timedelta

class BrokerClient:
    def __init__(self, token):
        self.refresh_token = token
        self.session = None
        self.access_token = None

        self.q_inventory = asyncio.Queue()
        self.q_orderbooks = asyncio.Queue()

    async def start(self):
        self.session = aiohttp.ClientSession()
        await self.authorize()

    async def close(self):
        await self.session.close()

    async def authorize(self):
        url = "https://be.broker.ru/trade-api-keycloak/realms/tradeapi/protocol/openid-connect/token"\

        payload = {
            "client_id": "trade-api-write",
            "refresh_token": self.refresh_token,
            "grant_type": "refresh_token"
        }

        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json"
        }

        for attempt in range(4):
            try:
                async with self.session.post(url, headers=headers, data=payload, timeout=10) as resp:
                    if resp.status!= 200:
                        text = await resp.text()
                        print(f"Invalid response while authorizing \n {resp.status} \n {text}")
                        await asyncio.sleep(3 + 2*attempt)
                        attempt += 1
                        continue
                    data = await resp.json()
                    self.access_token = data['access_token']
                    print("Authorized")
                    return
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                print(f"Failed attempt {attempt+1} while authorizing: \n {e}")
                await asyncio.sleep(3 + 2*attempt)

        raise Exception("Failed to authorize with 4 attempts")

    async def start_order_book_ws(self, ticker, depth, class_code):
        url = "wss://ws.broker.ru/trade-api-market-data-connector/api/v1/market-data/ws"
        headers = {"Authorization": f"Bearer {self.access_token}"}

        attempt = 0
        while True:
            try:
                async with self.session.ws_connect(url, headers=headers) as ws:
                    subscribe_message = {
                        "subscribeType": 0,
                        "dataType": 0,
                        "depth": depth,
                        "instruments": [
                            {
                                "classCode": class_code,
                                "ticker": ticker
                            }
                        ]
                    }
                    await ws.send_json(subscribe_message)
                    print(f"connected ws for {ticker}")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                            except Exception as e:
                                print("Invalid json")
                                continue
                            print("orderbook updated")
                            await self.q_orderbooks.put(data)
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            print(f"Websocket message error: \n {ws.exception()}")
                            break
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING):
                            print("Websocket closed by server")
                            break

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                print(f"Failed attempt {attempt + 1} while opening order book websocket: \n {e}")
                await asyncio.sleep(min(3 + 2 * attempt, 60))
                attempt += 1

    async def get_inventory(self):
        url = "https://be.broker.ru/trade-api-bff-portfolio/api/v1/portfolio"

        payload = {}
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {self.access_token}'
        }
        attempt = 0
        while True:
            try:
                async with self.session.get(url, headers=headers, data=payload) as resp:
                    if resp.status!= 200:
                        text = await resp.text()
                        print(f"Invalid response while updating inventory \n {resp.status} \n {text}")
                        await asyncio.sleep(3 + 2*attempt)
                        attempt += 1
                        continue
                    data = await resp.json()
                    inventory = {}
                    for position in data:
                        ticker = position['ticker']
                        if ticker in inventory:
                            continue
                        size = position['quantity']
                        inventory[ticker] = size
                    await self.q_inventory.put(inventory)
                    return inventory

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                print(f"Failed attempt {attempt + 1} while opening updating inventory: \n {e}")
                await asyncio.sleep(min(3 + 2 * attempt, 60))
                attempt += 1

    async def start_inventory_refresher(self):
        while True:
            try:
                await self.get_inventory()
                await asyncio.sleep(5)
            except Exception as e:
                print(f"Failed to update inventory \n {e}")
                await asyncio.sleep(5)

    async def start_orders_ws(self):  # Apparently not working currently
        url = "wss://ws.broker.ru/trade-api-bff-operations/api/v1/orders/execution/ws"
        headers = {"Authorization": f"Bearer {self.access_token}"}

        async with self.session.ws_connect(url, headers=headers) as ws:
            print("Connected?")
            await ws.send_json({"type": "ping"})
            async for ms in ws:
                data = json.loads(ms.data)
                print(data)

    async def get_all_active_orders(self):
        url = "https://be.broker.ru/trade-api-bff-order-details/api/v1/orders/search"

        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f'Bearer {self.access_token}'
        }

        payload = {
            "StartDateTime":(datetime.now() - timedelta(days=1)).isoformat(),
            "EndDateTime": (datetime.now() + timedelta(days=1)).isoformat(),
            "orderStatus": [3] #active
        }

        attempt = 0
        while True:
            try:
                async with self.session.post(url, headers=headers, json=payload) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        print(f"Invalid response while updating inventory \n {resp.status} \n {text}")
                        await asyncio.sleep(3 + 2 * attempt)
                        attempt += 1
                        continue
                    data = await resp.json()
                    print(data['records'])
                    break
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                print(f"Failed attempt {attempt + 1} while getting active orders: \n {e}")
                await asyncio.sleep(min(3 + 2 * attempt, 60))
                attempt += 1



class MVPStrategy:
    def __init__(self, client, ticker, spread, order_size, inventory_limit, inventory_k):
        self.client = client
        self.ticker = ticker
        self.spread = spread
        self.order_size = order_size
        self.inventory_limit = inventory_limit
        self.inventory_k = inventory_k

        self.inventory = None
        self.best_bid = None
        self.best_ask = None

    async def run(self):
        while True:
            done, pending = await asyncio.wait(
                [
                    asyncio.create_task(self.client.q_orderbooks.get()),
                    asyncio.create_task(self.client.q_inventory.get())
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in done:
                data = task.result()

                if "depth" in data:
                    self.best_ask = data['asks'][0]['price']
                    self.best_bid = data['bids'][0]['price']
                else:
                    self.inventory = data.get(self.ticker, 0)

            orders = self.generate_orders()
            if orders:
                print("NEW ORDERS:", orders)

    def generate_orders(self):
        if self.best_bid is None or self.best_ask is None:
            return None

        mid = (self.best_bid + self.best_ask) / 2
        half_spread = self.spread / 2

        inventory_shift = self.inventory_k * self.inventory
        center = mid - inventory_shift

        bid = center - half_spread
        ask = center + half_spread

        bid = min(bid, self.best_bid)
        ask = max(ask, self.best_ask)

        bid_size = self.order_size
        ask_size = self.order_size

        if self.inventory > 0:
            bid_size *= max(0.1, 1 - abs(self.inventory) / self.inventory_limit)

        if self.inventory < 0:
            ask_size *= max(0.1, 1 - abs(self.inventory) / self.inventory_limit)

        return {
            "ticker": self.ticker,
            "bid_price": round(bid, 4),
            "bid_size": round(bid_size, 2),
            "ask_price": round(ask, 4),
            "ask_size": round(ask_size, 2),
        }



async def main():
    token = os.getenv("BKS_TOKEN")
    client = BrokerClient(token)
    await client.start()

    task1 = client.get_all_active_orders()
    await asyncio.gather(task1)

    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
