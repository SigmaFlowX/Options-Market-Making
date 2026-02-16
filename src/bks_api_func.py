import requests
from datetime import datetime, timedelta, timezone
import uuid
import websocket
import json
import threading
import os
import pandas as pd
import time

BASE_DIR = os.path.join(os.path.dirname(__file__), "..")
DATA_DIR = os.path.join(BASE_DIR, "data")
TOKEN_FILE = os.path.join(DATA_DIR, "bks_token.txt")

last_candles = {}
order_books = {}

def get_last_bid_and_ask(ticker):
    if not order_books:
        raise ValueError("No data")

    order_book = order_books[ticker]
    bid = order_book['bids'][0]['price']
    ask = order_book['asks'][0]['price']

    return {'ask':ask, 'bid':bid}

def get_option_maturity_date(token, stock_ticker, option_ticker, sleep_time=5, size=100):
    url = "https://be.broker.ru/trade-api-information-service/api/v1/instruments/by-type"

    session = requests.Session()
    session.headers.update({
        "Accept": "application/json",
        "Authorization": f"Bearer {token}"
    })

    page = 0
    results = []
    while True:
        params = {
            "type": "OPTIONS",
            "baseAssetTicker": stock_ticker,
            "size": size,
            "page":page
        }

        response = session.get(url, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()
        if not data:
            break

        results.extend(data)
        page += 1
        print(page, end=' ')
        time.sleep(sleep_time)
    print("")
    df = pd.DataFrame(results)
    settlement_date = df.loc[df["ticker"] == option_ticker, "maturityDate"].values[0]
    date = datetime.strptime(settlement_date, "%Y%m%d")
    return date

def get_token_from_txt_file():
    file = open(TOKEN_FILE)
    token = file.read()
    return token

def authorize(refresh_token):
    url = "https://be.broker.ru/trade-api-keycloak/realms/tradeapi/protocol/openid-connect/token"

    payload = {
        "client_id": "trade-api-write",
        "refresh_token": refresh_token,
        "grant_type": "refresh_token"
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "application/json"
    }

    response = requests.post(url, headers=headers, data=payload)

    if response.status_code == 200:
        tokens = response.json()
        token = tokens["access_token"]
        return token
    else:
        print("Error while trying  to authorize:", response.status_code, response.text)
        return None

def get_current_price(token, ticker, class_code = "TQBR"):
    url = "https://be.broker.ru/trade-api-market-data-connector/api/v1/candles-chart"

    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=40)
    start_date_str = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date_str = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}"
    }

    payload = {
        "classCode": class_code,
        "ticker": ticker,
        "startDate": start_date_str,
        "endDate": end_date_str,
        "timeFrame": "MN"
    }

    response = requests.get(url, headers=headers, params=payload)

    if response.status_code == 200:
        data = response.json()
        candles = data.get("bars", [])
        if candles:
            return candles[0]['close']
        else:
            print("No candles obtained")
            return None
    else:
        print("Failed to get current price:", response.status_code, response.text)
        return None

def get_candles(token, ticker, start_date, end_date, class_code="TQBR", timeframe = "M1"):
    url = "https://be.broker.ru/trade-api-market-data-connector/api/v1/candles-chart"

    start_date = datetime.fromisoformat(start_date).astimezone(timezone.utc)
    start_date = start_date.isoformat(timespec="milliseconds")

    end_date = datetime.fromisoformat(end_date).astimezone(timezone.utc)
    end_date = end_date.isoformat(timespec="milliseconds")

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}"
    }

    payload = {
        "classCode": class_code,
        "ticker": ticker,
        "startDate": start_date,
        "endDate": end_date,
        "timeFrame": timeframe
    }

    response = requests.get(url, headers=headers, params=payload)

    return response.json()['bars']

def place_order(token, ticker, quantity, direction,  order_type, class_code="TQBR", price=None):  # 1 - buy, 2 - sell, 1 - market, 2 - limit

    url = "https://be.broker.ru/trade-api-bff-operations/api/v1/orders"

    client_order_id = str(uuid.uuid4())

    headers = {
      "Content-Type": "application/json",
      "Accept": "application/json",
      "Authorization": f"Bearer {token}"
    }

    payload = {
        "clientOrderId": client_order_id,
        "side": str(direction),
        "orderType": str(order_type),
        "orderQuantity": quantity,
        "ticker": ticker,
        "classCode": class_code,
    }
    if order_type == 2:
        payload["price"] = float(price)

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 200:
        print("Order placed")
        return client_order_id
    else:
        print("Failed to place the order:", response.status_code, response.text)
        return None

def get_order_status(token, id):
    url = f"https://be.broker.ru/trade-api-bff-operations/api/v1/orders/{id}"

    payload = {
        "originalClientOrderId": id
    }

    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    return response.json()['data']['orderStatus']

def start_last_candle_ws(token, ticker, class_code="TQBR"):
    def last_candle_ws():
        def on_open(ws):
            print("WebSocket connected")
            subscribe_message = {
                "subscribeType": 0,
                "dataType": 1,
                "timeFrame": "M1",
                "instruments": [
                    {
                        "classCode": class_code,
                        "ticker": ticker
                    }
                ]
            }
            ws.send(json.dumps(subscribe_message))

        def on_message(ws, message):
            data = json.loads(message)

            if data.get("responseType") == "CandleStickSuccess":
                print("Subscribed successfully:", data)

            elif data.get("responseType") == "CandleStick":
                handle_candle(data)

        def handle_candle(candle):
            last_candles[ticker] = candle

            print(
                f"[{candle['dateTime']}] "
                f"O={candle['open']} "
                f"H={candle['high']} "
                f"L={candle['low']} "
                f"C={candle['close']} "
                f"V={candle['volume']}"
            )

        def on_error(ws, error):
            print("WebSocket error:", error)

        ws = websocket.WebSocketApp(
            "wss://ws.broker.ru/trade-api-market-data-connector/api/v1/market-data/ws",
            header=[f"Authorization: Bearer {token}"],
            on_open=on_open,
            on_message=on_message,
            on_error=on_error
        )
        ws.run_forever()

    thread = threading.Thread(target=last_candle_ws)
    thread.start()

def start_order_book_ws(token, ticker, class_code="TQBR", depth=20):
    def order_book_ws():

        def on_open(ws):
            print(f"WebSocket connected for {ticker}")
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

            ws.send(json.dumps(subscribe_message))

        def on_message(ws, message):
            data = json.loads(message)

            if data.get("responseType") == "OrderBookSuccess":
                print(f"Subscribed successfully to Order Book for {ticker}")

            elif data.get("responseType") == "OrderBook":
                order_books[ticker] = data
                handle_order_book(data)

        def on_error(ws, error):
            print("WebSocket error:", error)

        def handle_order_book(order_book):
            pass

        ws = websocket.WebSocketApp(
            "wss://ws.broker.ru/trade-api-market-data-connector/api/v1/market-data/ws",
            header=[f"Authorization: Bearer {token}"],
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
        )

        ws.run_forever()

    thread = threading.Thread(target=order_book_ws)
    thread.start()

def get_raw_positions(token):
    url = "https://be.broker.ru/trade-api-bff-portfolio/api/v1/portfolio"

    payload = {}
    headers = {
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    return response.json()

def get_current_inventory(token):
    inventory = {}
    positions = get_raw_positions(token)

    for position in positions:
        ticker = position['ticker']
        if ticker in inventory:
            break
        size = position['quantity']
        inventory[ticker] = size

    return inventory

def price_option_using_bs(token, spot_ticker, option_ticker):
    try:
        from black_scholes import solve_black_scholes
    except:
        print("Couldnt find BS script, NONE is returned")
        return None

    spot_price = get_current_price(token, spot_ticker, class_code="TQBR")
    expiry = get_option_maturity_date(token, spot_ticker, option_ticker)
    strike_price = float(option_ticker[2:5])
    eval_date = datetime.now()

    solution = solve_black_scholes(spot_price, strike_price, 0.15, 0.2, expiry, eval_date)

    return solution


# Состояние заявки:
# 0 — Новая
# 1 — Частично исполнена
# 2 — Полностью исполнена
# 4 — Отменена
# 5 — Заменена
# 6 — Отменяется (в процессе отмены)
# 8 — Отклонена
# 9 — Заменяется (например, если вы изменяли заявку)
# 10 — Ожидает подтверждения новой заявки

if __name__ == "__main__":
    access_token = authorize(get_token_from_txt_file())

    print(price_option_using_BS(access_token, "SBER", "SR300CB6"))

#
#     #
#     # print(get_candles(access_token, "SR300CB6", "2026-02-10", "2026-02-11", "OPTSPOT", "H1"))
#     # SR - SBER
#     # 300 - strike price
#     # C - call
#     # B - month (A - January, B - February)
#     # 0 - last digit of year (6 for 2026)





