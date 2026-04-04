import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
import os
import psycopg2

def load_datasets(db_url, ticker):
    try:
        conn = psycopg2.connect(db_url, sslmode="require")
        cur = conn.cursor()
    except Exception as e:
        print("Exception while connecting to db")
        raise

    columns = ["id", "ticker", "timestamp", "bids", "asks"]

    query = f"""
    SELECT {', '.join(columns)}
    FROM orderbooks
    WHERE ticker = '{ticker}'
    """

    option_df = pd.read_sql_query(query, conn)
    option_df['timestamp'] = pd.to_datetime(option_df['timestamp'])
    option_df.set_index('timestamp', inplace=True)

    option_df['best_bid'] = option_df['bids'].apply(lambda bids: bids[0]['price'] if bids else None)
    option_df['best_ask'] = option_df['asks'].apply(lambda asks: asks[0]['price'] if asks else None)
    option_df['mid_price'] = option_df['best_bid'] + (option_df['best_ask'] - option_df['best_bid']) / 2
    option_df['spread'] = option_df['best_ask'] - option_df['best_bid']

    columns = ["timestamp", "side", "volume", "price", "quantity"]

    query = f"""
    SELECT {', '.join(columns)}
    FROM orders
    WHERE ticker = '{ticker}'
    """

    orders_df = pd.read_sql_query(query, conn)
    orders_df['timestamp'] = pd.to_datetime(orders_df['timestamp'])
    orders_df.set_index('timestamp', inplace=True)

    return option_df, orders_df

def main():
    load_dotenv()
    url = os.getenv("DATABASE_URL")

    option_df, orders_df = load_datasets(url, "SR310CD6B")


if __name__ == "__main__":
    main()
