import QuantLib as ql

def solve_black_scholes(spot_price, strike_price, risk_free_rate, volatility, expiry_date):
    calendar = ql.TARGET()
    day_count = ql.Actual365Fixed()
    today = ql.Date().todaysDate()

    spot_handle = ql.QuoteHandle(ql.SimpleQuote(spot_price))
    flat_ts = ql.YieldTermStructureHandle(ql.FlatForward(ql.Date().todaysDate(), risk_free_rate, day_count))

    flat_vol_ts = ql.BlackVolTermStructureHandle(ql.BlackConstantVol(ql.Date().todaysDate(), calendar, volatility, day_count))
    dividend_ts = ql.YieldTermStructureHandle(ql.FlatForward(today, 0.0, day_count)
    )
    process = ql.BlackScholesMertonProcess(
        spot_handle,
        dividend_ts,
        flat_ts,
        flat_vol_ts
    )

    payoff = ql.PlainVanillaPayoff(ql.Option.Call, strike_price)
    exercise = ql.AmericanExercise(ql.Date().todaysDate(), expiry_date)

    american_option = ql.VanillaOption(payoff, exercise)

    engine = ql.BinomialVanillaEngine(process, "crr", 500)
    american_option.setPricingEngine(engine)

    price = american_option.NPV()
    #delta = american_option.delta()
    #gamma = american_option.gamma()

    return price

if __name__ == "__main__":
    price = 300
    strike = 290
    expiry = ql.Date(20, 2, 2026)
    rf = 0.15
    volatility = 0.2

    print(solve_black_scholes(price, strike, rf, volatility, expiry))