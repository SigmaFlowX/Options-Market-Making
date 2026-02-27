import websocket
import requests
from bks_api_func import authorize
import os
import time


def get_tickers(instrument_type, access_token, class_code):
    url = "https://be.broker.ru/trade-api-information-service/api/v1/instruments/by-type"

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {access_token}"
    }

    payload = {
        "type": instrument_type
    }


    tickers = []
    page = 0
    while True:
        print(f"page: {page}")
        payload = {
            "type": instrument_type,
            "page": page
        }
        while True:
            try:
                resp = requests.get(url, headers=headers, params=payload)
                resp.raise_for_status()
                break
            except Exception as e:
                print(f"Failed at page {page}")
                print(e)
                time.sleep(10)

        data = resp.json()
        if not data:
            break
        new_tickers = [
            item["ticker"]
            for item in data
            if item.get("primaryBoard") == class_code
        ]
        tickers = tickers + new_tickers
        page += 1
    return tickers

def get_spread_by_ticker():
    pass

if __name__ == "__main__":
    refresh_token = os.getenv("BKS_TOKEN")
    token = authorize(refresh_token)
    all_tickers = get_tickers("STOCK", token, "TQBR")