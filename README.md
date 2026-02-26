## disclaimer
This project is purely for educational purposes and I personally do not exepct it to be profitable for 2 key reasons: <br>
1) I don't have market maker fees on the exchange
2) I am not hunting super low latencies with my code but that is aboslutely necessary for market making

Options market is chosen beacuse of its high spreads and low liquidity thanks to which I won't burn a ton of moeny in fees while testing and can really not care about latency. 
   
Then what is it for? <br>
Well, first of all it's my first ever LIVE trading bot experience, so I can learn to work with brokers api and asyncio. <br>
Secondly, I can get familiar with basic market making. 

## What has been done as of today 
All the main work is in src/MM_engine.py
1) Most of the necessary broker interactions such as placing orders, editing them, cancelling, updating inventory, opening websockets and so on.
2) Simplest MM strategy that generates ask and bid orders based on market data.
3) OrderManager that compares desired orders from the strategy and current live orders and makes adjustments.

It can already trade live and right now I am actively testing key components. 

## What is left to do 

1) Cancelling redundant orders and overall having some kind of database would be great.
2) Active delta hedging with underlying asset or a futures
3) Improving the strategy itself 
