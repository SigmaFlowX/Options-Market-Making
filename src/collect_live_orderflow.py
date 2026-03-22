from mm_engine import BrokerClient
import os
import json
import asyncio
import asyncpg
from datetime import datetime

RESTART_TIME = 7200
INSTRUMENTS = [
    {"ticker": "SR320CC6D", "classCode": "OPTSPOT"},
    {"ticker": "SR310CC6D", "classCode": "OPTSPOT"},
    {"ticker": "SR300CC6D", "classCode": "OPTSPOT"},
    {"ticker": "SR320CO6D", "classCode": "OPTSPOT"},
    {"ticker": "SR310CO6D", "classCode": "OPTSPOT"},
    {"ticker": "SR300CO6D", "classCode": "OPTSPOT"},
    {"ticker": "SR320CD6A", "classCode": "OPTSPOT"},
    {"ticker": "SR310CD6A", "classCode": "OPTSPOT"},
    {"ticker": "SR300CD6A", "classCode": "OPTSPOT"},
    {"ticker": "SR320CP6A", "classCode": "OPTSPOT"},
    {"ticker": "SR310CP6A", "classCode": "OPTSPOT"},
    {"ticker": "SR300CP6A", "classCode": "OPTSPOT"},
    {"ticker": "SR320CD6", "classCode": "OPTSPOT"},
    {"ticker": "SR310CD6", "classCode": "OPTSPOT"},
    {"ticker": "SR300CD6", "classCode": "OPTSPOT"},
    {"ticker": "SR320CP6", "classCode": "OPTSPOT"},
    {"ticker": "SR310CP6", "classCode": "OPTSPOT"},
    {"ticker": "SR300CP6", "classCode": "OPTSPOT"},

]


DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

async def connect_db():
    conn = await asyncpg.connect(os.getenv("ORDER_DATABASE_URL"))
    return conn

async def save_orderbook_data(client, conn):
    while True:
        try:
            data = await client.q_q_orderflow.get()
            if data['responseType'] == "LastTrades":
                # ticker = data['ticker']
                # output_file = os.path.join(DATA_DIR, f"{ticker}.jsonl")
                # with open(output_file, "a") as f:
                #     json.dump(data, f)
                #     f.write("\n")
                #
                # print(f"Updated {ticker}")
                timestamp = datetime.fromisoformat(data["dateTime"].replace("Z", "+00:00")
                )
                await conn.execute(
                    """
                    INSERT INTO orders (
                        ticker,
                        class_code,
                        timestamp,
                        side,
                        volume,
                        price,
                        quantity
                    )
                    VALUES ($1,$2,$3,$4,$5,$6,$7)
                    """,
                    data["ticker"],
                    data["classCode"],
                    timestamp,
                    data['side'],
                    data['volume'],
                    data["price"],
                    data["quantity"]
                )

                print(f"Updated {data['ticker']}")
        except Exception as e:
            print(f"Error while saving: {e}")
            await asyncio.sleep(10)

async def run():
    token = os.getenv("BKS_TOKEN")
    client = BrokerClient(token)

    conn = await connect_db()

    while True:
        try:
            await client.start()
            break
        except Exception as e:
            print(f"Exception while starting a client {e}")
            await asyncio.sleep(10)

    save_task = asyncio.create_task(save_orderbook_data(client, conn))


    ws_task = asyncio.create_task(client.start_orderflow_ws(instruments=INSTRUMENTS))

    try:
        await asyncio.gather(save_task, ws_task)
    finally:
        await client.close()

async def main():
    while True:
        try:
            print("Started")
            await asyncio.wait_for(run(),timeout=RESTART_TIME)

        except Exception as e:
            print(f"Exception in main loop {e}")
            await asyncio.sleep(10)

if __name__ == "__main__":
    asyncio.run(main())

