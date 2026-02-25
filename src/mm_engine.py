import asyncio
import os
import aiohttp
import json
from datetime import datetime, timedelta
import uuid




class BrokerClient:
    def __init__(self, token):
        self.refresh_token = token
        self.session = None
        self.access_token = None
        self.active_orders = {}

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
                            print("Orderbook updated")
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
                print("Inventory updated")
                await asyncio.sleep(5)
            except Exception as e:
                print(f"Failed to update inventory \n {e}")
                await asyncio.sleep(5)

    async def start_orders_ws(self):
        url = "wss://ws.broker.ru/trade-api-bff-operations/api/v1/orders/execution/ws"
        headers = {"Authorization": f"Bearer {self.access_token}"}

        attempt = 0
        while True:
            try:
                async with self.session.ws_connect(url, headers=headers) as ws:
                    print("Connected orders ws")
                    async for ms in ws:
                        data = json.loads(ms.data)
                        order_id = data['clientOrderId']
                        order_status = data['data']['orderStatus']

                        if order_status in ['2', '4', '6', '8']:
                            self.active_orders.pop(order_id, None)
                        elif order_status == '1':
                            if order_id in self.active_orders:
                                self.active_orders[order_id]['quantity'] = data['data']['remainedQuantity']
                                self.active_orders[order_id]['status'] = order_status
                        else:
                            if order_id in self.active_orders:
                                self.active_orders[order_id]['status'] = order_status

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                print(f"Failed attempt {attempt + 1} while opening orders websocket order: \n {e}")
                await asyncio.sleep(min(3 + 2 * attempt, 60))
                attempt += 1

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

    async def place_limit_order(self, ticker, class_code, side, price, quantity):
        url = "https://be.broker.ru/trade-api-bff-operations/api/v1/orders"


        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": f"Bearer {self.access_token}"
        }

        price = round(price, 2)

        attempt = 0
        while True:
            client_order_id = str(uuid.uuid4())
            payload = {
                "clientOrderId": client_order_id,
                "side": str(side),
                "orderType": "2",
                "orderQuantity": quantity,
                "ticker": ticker,
                "classCode": class_code,
                "price": price
            }
            try:
                async with self.session.post(url, headers=headers, json=payload) as resp:

                    if resp.status != 200:
                        text = await resp.text()
                        print(f"Invalid response while while placing order \n {resp.status} \n {text}")
                        await asyncio.sleep(3 + 2 * attempt)
                        attempt += 1
                        continue

                    data = await resp.json()
                    client_order_id = data['clientOrderId']

                    self.active_orders[client_order_id] = {
                        "ticker": ticker,
                        "class_code": class_code,
                        "side": side,
                        "price": price,
                        "quantity": quantity,
                        "status": '0'
                    }
                    print(f"Placed order for {ticker} at price {price} with quantity {quantity} and id {client_order_id}")

                    break
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                print(f"Failed attempt {attempt + 1} while placing order: \n {e}")
                await asyncio.sleep(min(3 + 2 * attempt, 60))
                attempt += 1

    async def cancel_order(self, id):
        url = f"https://be.broker.ru/trade-api-bff-operations/api/v1/orders/{id}/cancel"
        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f'Bearer {self.access_token}'
        }

        attempt = 0
        while True:
            new_id = str(uuid.uuid4())
            payload = {
                "clientOrderId": new_id
            }
            try:
                async with self.session.post(url, headers=headers, json=payload) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        print(f"Invalid response while canceling order \n {resp.status} \n {text}")
                        await asyncio.sleep(3 + 2 * attempt)
                        attempt += 1
                        continue
                    print(f"Canceled order {id}")
                    self.active_orders.pop(id, None)
                    break
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                print(f"Failed attempt {attempt + 1} while canceling order: \n {e}")
                await asyncio.sleep(min(3 + 2 * attempt, 60))
                attempt += 1

    async def get_order_status(self, id):
        url = f"https://be.broker.ru/trade-api-bff-operations/api/v1/orders/{id}"
        payload = {
            "originalClientOrderId": id
        }
        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {self.access_token}'
        }

        attempt = 0
        while True:
            try:
                async with self.session.get(url, headers=headers, data=payload) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        print(f"Invalid response while while placing order \n {resp.status} \n {text}")
                        await asyncio.sleep(3 + 2 * attempt)
                        attempt += 1
                        continue
                    data = await resp.json()
                    return data
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                print(f"Failed attempt {attempt + 1} while getting order status: \n {e}")
                await asyncio.sleep(min(3 + 2 * attempt, 60))
                attempt += 1

    async def force_update_orders_dict_status(self):

        for order_id in list(self.active_orders.keys()):
            order_status = await self.get_order_status(id=order_id)

            if order_status['data']['orderStatus'] in ['2', '4', '6', '8']:
                if order_id in self.active_orders:
                    self.active_orders.pop(order_id, None)
            elif order_status['data']['orderStatus'] == '1':
                if order_id in self.active_orders:
                    self.active_orders[order_id]['quantity'] = order_status['data']['remainedQuantity']
            else:
                if order_id in self.active_orders:
                    self.active_orders[order_id]['status'] = order_status['data']['orderStatus']

    async def edit_order(self, id, price, quantity):
        url = f"https://be.broker.ru/trade-api-bff-operations/api/v1/orders/{id}"

        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f'Bearer {self.access_token}'
        }
        price = round(price, 2)

        attempt = 0
        while True:
            new_id = str(uuid.uuid4())
            payload = {
                "clientOrderId": new_id,
                "price": price,
                "orderQuantity": quantity
            }

            try:
                async with self.session.post(url, headers=headers, json=payload) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        print(f"Invalid response while editing order order \n {resp.status} \n {text}")
                        await asyncio.sleep(3 + 2 * attempt)
                        attempt += 1
                        continue

                    side, ticker, class_code = self.active_orders[id]['side'], self.active_orders[id]['ticker'], self.active_orders[id]['class_code']
                    self.active_orders.pop(id, None)
                    self.active_orders[new_id] = {
                        "ticker": ticker,
                        "class_code": class_code,
                        "side": side,
                        "price": price,
                        "quantity": quantity,
                        "status": '0'
                    }
                    print(f"edited order {id} with ticker {ticker} \n new price {price}, quantity = {quantity}")
                    break
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                print(f"Failed attempt {attempt + 1} while canceling order: \n {e}")
                await asyncio.sleep(min(3 + 2 * attempt, 60))
                attempt += 1

    async def start_forced_orders_dict_refresher(self):
        while True:
            await self.force_update_orders_dict_status()
            await asyncio.sleep(10)





class MVPStrategy:
    def __init__(self, client, order_manager, ticker, class_code,  spread, order_size, inventory_limit, inventory_k):
        self.client = client
        self.order_manager = order_manager
        self.ticker = ticker
        self.class_code = class_code
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
                    asyncio.create_task(self.client.q_orderbooks.get()), # have to add ticker comparasion in the future for multiple assets trading
                    asyncio.create_task(self.client.q_inventory.get())
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )

            for task in pending:
                task.cancel()

            for task in done:
                data = task.result()

                if "depth" in data:
                    self.best_ask, self.best_bid = self.get_best_bid_and_asks_from_orderbook(data)
                else:
                    self.inventory = data.get(self.ticker, 0)

            if self.inventory is None:
                print("Inventory missing(")
                continue
            orders = self.generate_orders()
            print(f"Current desired orders \n {orders}")
            if orders:
                await self.order_manager.submit_orders(orders)

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

        bid_size = max(1, bid_size)
        ask_size = max(1, ask_size)

        if self.inventory >= self.inventory_limit:
            bid_size = 0
        elif self.inventory <= -self.inventory_limit:
            ask_size = 0

        ask_order = {
            "ticker": self.ticker,
            "class_code": self.class_code,
            "side": '0',
            "price": round(ask,2),
            "quantity": ask_size
        }

        bid_order = {
            "ticker": self.ticker,
            "class_code": self.class_code,
            "side": '1',
            "price": round(bid,2),
            "quantity": bid_size
        }

        orders = []
        if bid_size > 0:
            orders.append(bid_order)
        if ask_size > 0:
            orders.append(ask_order)

        return orders if orders else None

    def get_best_bid_and_asks_from_orderbook(self, orderbook): #we have to exclude our own orders from orderbook to find real best bid and ask
        bids = orderbook.get("bids", [])
        asks = orderbook.get("asks", [])

        my_bid_volume_by_price = {}
        my_ask_volume_by_price = {}

        for order in self.client.active_orders.values():
            if order["ticker"] != self.ticker:
                continue

            price = order["price"]
            price = round(price, 2)
            qty = order["quantity"]

            if order["side"] == '1':
                my_bid_volume_by_price[price] = my_bid_volume_by_price.get(price, 0) + qty
            else:
                my_ask_volume_by_price[price] = my_ask_volume_by_price.get(price, 0) + qty

        external_best_bid = None
        external_best_ask = None

        for level in bids:
            price = level["price"]
            price = round(price, 2)
            size = level["quantity"]

            my_size = my_bid_volume_by_price.get(price, 0)
            external_size = size - my_size

            if external_size > 0:
                external_best_bid = price
                break

        for level in asks:
            price = level["price"]
            price = round(price, 2)
            size = level["quantity"]

            my_size = my_ask_volume_by_price.get(price, 0)
            external_size = size - my_size

            if external_size > 0:
                external_best_ask = price
                break

        return external_best_bid, external_best_ask

class OrderManager:
    def __init__(self, client):
        self.client = client
        self.q_desired_orders = asyncio.Queue()

    async def submit_orders(self, desired_orders):
        await self.q_desired_orders.put(desired_orders)

    async def run(self):
        while True:
            desired_orders = await self.q_desired_orders.get()
            current_orders = self.client.active_orders
            for desired_order in desired_orders:

                order_to_edit = None
                order_id_to_edit = None

                for client_id, order in current_orders.items():
                    if order["ticker"] == desired_order["ticker"] and order["side"] == desired_order["side"]:
                        order_to_edit = order
                        order_id_to_edit = client_id
                        break

                if order_to_edit is None:
                    await self.client.place_limit_order(
                        ticker=desired_order['ticker'],
                        class_code=desired_order['class_code'],
                        side=desired_order['side'],
                        price=desired_order['price'],
                        quantity=desired_order['quantity']
                    )
                else:
                    if abs(desired_order['price'] - current_orders[order_id_to_edit]['price']) >= 0.1:
                        await self.client.edit_order(id=order_id_to_edit, price=desired_order['price'], quantity=desired_order['quantity'])

            await asyncio.sleep(5)

async def main():
    token = os.getenv("BKS_TOKEN")
    client = BrokerClient(token)
    await client.start()

    order_manager = OrderManager(client=client)
    strategy = MVPStrategy(client, order_manager, "SR310CC6A", "OPTSPOT",  0.3, 1, 5, 0.1)

    task0 = asyncio.create_task(client.start_orders_ws())
    task1 = asyncio.create_task(client.start_order_book_ws(ticker="SR310CC6A", class_code="OPTSPOT", depth=5))
    task2 = asyncio.create_task(client.start_inventory_refresher())
    task3 = asyncio.create_task(strategy.run())
    task4 = asyncio.create_task(order_manager.run())
    task5 = asyncio.create_task(client.start_forced_orders_dict_refresher())
    await asyncio.gather(task0, task1, task2, task3, task4, task5)
    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
