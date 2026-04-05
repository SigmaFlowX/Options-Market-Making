import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
import os
import psycopg2
from nbclient.client import timestamp


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

def generate_orders_simple(best_ask, best_bid, order_size, inventory, inventory_limit, inventory_k=0):

    mid = (best_bid + best_ask) / 2
    half_spread = abs((best_ask - best_bid)) / 2

    inventory_shift = inventory_k * inventory
    center = mid - inventory_shift

    bid = center - half_spread + 0.01
    ask = center + half_spread - 0.01

    bid = min(bid, best_bid)
    ask = max(ask, best_ask)

    bid_size = order_size
    ask_size = order_size

    if inventory > 0:
        bid_size *= max(0.1, 1 - abs(inventory) / inventory_limit)

    if inventory < 0:
        ask_size *= max(0.1, 1 - abs(inventory) / inventory_limit)

    bid_size = max(1, bid_size)
    ask_size = max(1, ask_size)

    if ask_size > inventory:
        ask_size = inventory

    if inventory == 0.0:
        ask_size = 0
    if inventory >= inventory_limit:
        bid_size = 0
    elif inventory <= -inventory_limit:
        ask_size = 0

    ask_order = {
        "side": '2',
        "price": round(ask,2),
        "quantity": round(ask_size)
    }

    bid_order = {
        "side": '1',
        "price": round(bid,2),
        "quantity": round(bid_size)
    }

    orders = []
    if bid_size > 0 and inventory < inventory_limit:
        orders.append(bid_order)
    if ask_size > 0 and inventory > 0:
        orders.append(ask_order)
    return orders if orders else None

def run_backtest(option_df, orders_df):
    orders_df = orders_df.sort_index()
    option_df = option_df.sort_index()
    option_df = option_df.groupby(level=0).last()

    df = pd.merge_asof(
        orders_df.reset_index(),
        option_df.reset_index(),
        on="timestamp",
        direction="backward"
        )
    df.set_index("timestamp", inplace=True)

    inventory = 0
    balance = 10000
    balance_arr = []
    timestamp_arr = []
    for row in df.itertuples():
        best_ask = row.best_ask
        best_bid = row.best_bid
        executed_price = row.price
        executed_volume = row.volume

        orders = generate_orders_simple(
            best_ask,
            best_bid,
            order_size = 5,
            inventory=inventory,
            inventory_limit=100,
        )

        if len(orders) == 0:
            continue

        if row.side == "BUY":
            sell_orders = [order for order in orders if order['side'] == "2"]
            if sell_orders:
                ask_order = sell_orders[0]
            else:
                continue
            ask_order_price = ask_order["price"]
            ask_order_quantity = ask_order["quantity"]

            if ask_order_price <= executed_price:
                if executed_volume >= ask_order_quantity:
                    inventory -= ask_order_quantity
                    balance -= ask_order_quantity * ask_order_price

                    balance_arr.append(balance)
                    timestamp_arr.append(row.index)
                elif executed_volume < ask_order_quantity:
                    inventory -= executed_volume
                    balance -= executed_volume * ask_order_price

                    balance_arr.append(balance)
                    timestamp_arr.append(row.index)
        elif row.side == "SELL":
            buy_orders = [order for order in orders if order['side'] == "1"]
            if buy_orders:
                bid_order = buy_orders[0]
            else:
                continue
            bid_order_price = bid_order["price"]
            bid_order_quantity = bid_order["quantity"]

            if bid_order_price >= executed_price:
                if executed_volume >= bid_order_quantity:
                    inventory += bid_order_quantity
                    balance += bid_order_quantity * bid_order_price

                    balance_arr.append(balance)
                    timestamp_arr.append(row.index)
                elif executed_volume < bid_order_quantity:
                    inventory += executed_volume
                    balance += executed_volume * bid_order_price

                    balance_arr.append(balance)
                    timestamp_arr.append(row.index)



def main():
    load_dotenv()
    url = os.getenv("DATABASE_URL")

    option_df, orders_df = load_datasets(url, "SR310CD6B")
    run_backtest(option_df, orders_df)
    #print(orders_df.head())


if __name__ == "__main__":
    main()
