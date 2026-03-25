## Disclaimer
This project is purely for educational purposes and I personally do not exepct it to be profitable for 2 key reasons: <br>
1) I don't have market maker fees on the exchange (In fact my fee rate is as high as 2%)
2) I am not hunting super low latencies with my code but that is aboslutely necessary for market making

Options market is chosen beacuse of its high spreads and low liquidity thanks to which I won't burn a ton of moeny in fees while testing and can really not care about latency. 
   
Then what is it for? <br>
Well, first of all it's my first ever LIVE trading bot experience, so I can learn to work with brokers api and asyncio. <br>
Secondly, I can get familiar with basic market making. 

## Tools used
1) Asyncio, aiohttp for broker API interactions
2) Pandas for historical data analysis
3) PostgreSQL db to store historical data
4) Quantlib to solve Black-Scholes equation and determine option's fair prices and Greeks

## What has been done as of today 
All the main work is in src/MM_engine.py
1) Most of the necessary broker interactions such as placing orders, editing them, cancelling, updating inventory, opening websockets and so on.
2) Simplest MM strategy that generates ask and bid orders based on market data.
3) OrderManager that compares desired orders from the strategy and current live orders and makes adjustments.
4) A script to collect live orderbook data and write it to postgres database (deployed to Railway).
5) As a side work: evaluating greeks and fair price using Black-Scholes model.
6) I also gathered some live orderbook data what I will analyse and may use it to optimize model parameters

It can already trade live and right now I am actively testing key components. 

## What is left to do 

1) Cancelling redundant orders and overall having some kind of database would be great. (+)
2) Active delta hedging with underlying asset or a future.
3) Improving the strategy itself.
4) Backtesting using historical data

## What I am going right now 
(25/03/25) <br> After building basics of broker interaction and a simple mm strategy, I reailized how badly I am lacking some historical orderbbok and orderflow data. <br> 
There is no publicly available data of this sort, so I have to collect it myself. <br>
Therefore, I have deployed live data scrapper on Railway and currently waiting for couple of weeks to accumulate enough data for some analysis and backtests. <br>
