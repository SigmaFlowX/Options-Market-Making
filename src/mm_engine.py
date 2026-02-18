import asyncio
import os
import aiohttp

class MarketData:
    def __init__(self, token):
        self.refresh_token = token
        self.access_token = None

        self.q_orderbooks = asyncio.Queue()

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
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, headers=headers, data=payload, timeout=10) as resp:
                        if resp.status!= 200:
                            text = await resp.text()
                            print(f"Invalid response while authorizing \n {resp.status} \n {text}")
                            await asyncio.sleep(3 + 2*attempt)
                            continue
                        data = await resp.json()
                        self.access_token = data['access_token']
                        return
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                print(f"Failed attempt {attempt+1} while authorizing: \n {e}")
                await asyncio.sleep(3 + 2*attempt)

        raise Exception("Failed to authorize with 4 attempts")




#env variable
bks_token = os.getenv("BKS_TOKEN")
market_data = MarketData(bks_token)

asyncio.run(market_data.authorize())

