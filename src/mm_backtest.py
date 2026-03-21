import pandas as pd

def generate_orders_simple(mid, best_bid, best_ask, inventory, inventory_k, order_size, inventory_limit):
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
    elif inventory <= inventory_limit:
        ask_size = 0

    ask_order = {
        "price": round(ask, 2),
        "quantity": round(ask_size)
    }

    bid_order = {
        "price": round(bid, 2),
        "quantity": round(bid_size)
    }

    orders = {}
    if bid_size > 0 and inventory < inventory_limit:
        orders['ask'] = ask_order
    if ask_size > 0 and inventory > 0:
        orders['bid'] = bid_order
    return orders if orders else None

def run_backtest(df, inventory_k, order_size, inventory_limit): #df: timestamp, mid, best_ask, best_bid):
    best_ask_arr = df['best_ask'].values
    best_bid_arr = df['best_bid'].values
    mid_arr = df['mid'].values

    inventory = 0
    cash = 0
    curr_orders = {}
    inventory_series = []
    cash_series = []
    pnl_series = []

    for i in range(len(mid_arr)):
        best_ask = best_ask_arr[i]
        best_bid = best_bid_arr[i]
        mid = mid_arr[i]

        if curr_orders:
            if "bid" in curr_orders:
                if curr_orders["bid"]["price"] >= best_bid:
                    inventory += curr_orders["bid"]["quantity"]
                    cash -= curr_orders["bid"]["price"] * curr_orders["bid"]["quantity"]
                    curr_orders.pop("bid")
            if "ask" in curr_orders:
                if curr_orders["ask"]["price"] <= best_ask:
                    inventory -= curr_orders["ask"]["quantity"]
                    cash += curr_orders["ask"]["price"] * curr_orders["ask"]["quantity"]
                    curr_orders.pop("ask")

        desired_orders = generate_orders_simple(mid, best_bid, best_ask, inventory, inventory_k, order_size, inventory_limit)
        if desired_orders:
            if "bid" in desired_orders:
                curr_orders["bid"] = desired_orders["bid"]
            if "ask" in desired_orders:
                curr_orders["ask"] = desired_orders["ask"]

        inventory_series.append(inventory)
        cash_series.append(cash)
        pnl_series.append(cash + inventory * mid)

    return {
        "pnl_series": pnl_series,
        "cash_series": cash_series,
        "inventory_series": inventory_series
    }


