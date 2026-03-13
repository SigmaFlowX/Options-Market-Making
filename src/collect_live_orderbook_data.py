from mm_engine import BrokerClient
import os
import json
import asyncio
import asyncpg
from datetime import datetime


TICKERS = [
    ("SR320CC6", "OPTSPOT"),
    ("SR310CC6", "OPTSPOT"),
    ("SR300CC6", "OPTSPOT"),
    ("SR320C06", "OPTSPOT"),
    ("SR310C06", "OPTSPOT"),
    ("SR300C06", "OPTSPOT")
]
DEPTH = 5


DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

async def refresh_client(client):
    await asyncio.sleep(60)
    while True:
        while True:
            try:
                await client.close()
                await client.start()
                print("client refreshed")
                break
            except Exception as e:
                print(f"Exception {e}")
                await asyncio.sleep(10)



async def connect_db():
    conn = await asyncpg.connect(os.getenv("DATABASE_URL"))
    return conn

async def save_orderbook_data(client, conn):
    while True:
        try:
            data = await client.q_orderbooks.get()
            if data['responseType'] == "OrderBook":
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
                    INSERT INTO orderbooks (
                        ticker,
                        class_code,
                        timestamp,
                        bids,
                        asks,
                        bid_volume,
                        ask_volume
                    )
                    VALUES ($1,$2,$3,$4,$5,$6,$7)
                    """,
                    data["ticker"],
                    data["classCode"],
                    timestamp,
                    json.dumps(data["bids"]),
                    json.dumps(data["asks"]),
                    data["bidVolume"],
                    data["askVolume"]
                )

                print(f"Updated {data['ticker']}")
        except Exception as e:
            print(f"Error while saving: {e}")
            await asyncio.sleep(10)

async def main():
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
    refresh_task = asyncio.create_task(refresh_client(client))
    ws_tasks = []
    for ticker, class_code in TICKERS:
        ws_tasks.append(asyncio.create_task(client.start_order_book_ws(ticker, DEPTH, class_code)))

    try:
        await asyncio.gather(save_task, *ws_tasks, refresh_task)
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())

