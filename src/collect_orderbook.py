from mm_engine import BrokerClient
import os
import json
import asyncio


TICKERS = [
    ("SR320CC6", "OPTSPOT")
]
DEPTH = 5


DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")



async def save_orderbook_data(client):
    while True:
        try:
            data = await client.q_orderbooks.get()
            if data['responseType'] == "OrderBook":
                ticker = data['ticker']
                output_file = os.path.join(DATA_DIR, f"{ticker}.jsonl")
                with open(output_file, "a") as f:
                    json.dump(data, f)
                    f.write("\n")

                print(f"Updated {ticker}")
        except Exception as e:
            print(f"Error while saving: {e}")
            await asyncio.sleep(10)

async def main():
    token = os.getenv("BKS_TOKEN")
    client = BrokerClient(token)

    while True:
        try:
            await client.start()
            break
        except Exception as e:
            print(f"Exception while starting a client {e}")
            await asyncio.sleep(10)

    save_task = asyncio.create_task(save_orderbook_data(client))
    ws_tasks = []
    for ticker, class_code in TICKERS:
        ws_tasks.append(asyncio.create_task(client.start_order_book_ws(ticker, DEPTH, class_code)))

    try:
        await asyncio.gather(save_task, *ws_tasks)
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())

