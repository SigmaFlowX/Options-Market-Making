from mm_engine import BrokerClient
import os
import json
import asyncio

TICKER = "SR320CC6"
DEPTH = 5
CLASS_CODE = "OPTSPOT"

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
OUTPUT_FILE = os.path.join(DATA_DIR, f"{TICKER}.jsonl")


async def save_orderbook_data(client):
    while True:
        data = await client.q_orderbooks.get()
        if data['responseType'] == "OrderBook":
            with open(OUTPUT_FILE, "a") as f:
                json.dump(data, f)
                f.write("\n")

async def main():
    while True:
        try:
            token = os.getenv("BKS_TOKEN")
            client = BrokerClient(token)

            await client.start()
            task_save = asyncio.create_task(save_orderbook_data(client))
            task_ws = asyncio.create_task(client.start_order_book_ws(TICKER, DEPTH, CLASS_CODE))
            await asyncio.gather(task_save, task_ws)


        except Exception as e:
            print(f"Exception {e} occurred")
            await asyncio.sleep(10)



if __name__ == "__main__":
    asyncio.run(main())

