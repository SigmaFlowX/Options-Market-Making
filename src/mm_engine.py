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

        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, data=payload, timeout=10) as resp:
                data = await resp.json()
                self.refresh_token = data['access_token']
                print("O")






#env variable
bks_token = os.getenv("BKS_TOKEN")
market_data = MarketData(bks_token)

asyncio.run(market_data.authorize())
