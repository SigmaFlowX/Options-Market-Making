from mm_engine import BrokerClient
import os
import json
import asyncio
import asyncpg
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")
RESTART_TIME = 7200
DEPTH = 5

postfix_list = [
    "CF6B", #06-10
    "CR6B",
    "CF6", #06-17
    "CR6",
    "CF6D", #06-24
    "CR6D",
    "CG6A",
    "CS6A"
]

ticker = "SR"
min_strike = 270
max_strike = 370
strike_step = 10

strike = min_strike
INSTRUMENTS = []
while strike < max_strike:
    for postfix in postfix_list:
        option_ticker = ticker + str(strike) + postfix
        INSTRUMENTS.append({"ticker": option_ticker, "classCode": "OPTSPOT"})
    strike += strike_step


async def connect_db():
    conn = await asyncpg.connect(os.getenv("DATABASE_URL"))
    return conn


async def save_orderbook(q_orderbooks, conn):

    while True:
        try:
            data = await q_orderbooks.get()
            if data['responseType'] == "OrderBook":

                timestamp = datetime.fromisoformat(data["dateTime"].replace("Z", "+00:00"))

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

                print(f"Updated order book {data['ticker']}")

        except Exception as e:
            print(f"Error while saving: orderbook {e}")
            await asyncio.sleep(10)

async def save_orderflow(q_orderflow, conn):
    while True:
        try:
            data = await q_orderflow.get()
            if data['responseType'] == "LastTrades":
                timestamp = datetime.fromisoformat(data["dateTime"].replace("Z", "+00:00"))

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

                print(f"Updated orderflow{data['ticker']}")


        except Exception as e:
            print(f"Error while saving orderflow: {e}")
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

    save_orderflow_task = asyncio.create_task(save_orderflow(client.q_orderflow, conn))
    save_orderbook_task = asyncio.create_task(save_orderbook(client.q_orderbooks, conn))

    order_flow_task = asyncio.create_task(client.start_orderflow_ws(instruments=INSTRUMENTS))
    order_book_task = asyncio.create_task(client.start_order_book_ws(instruments=INSTRUMENTS, depth=DEPTH))

    try:
        await asyncio.gather(
            save_orderflow_task,
            save_orderbook_task,
            order_flow_task,
            order_book_task
        )

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

