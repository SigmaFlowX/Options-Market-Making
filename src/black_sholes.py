import QuantLib as ql

def solve_black_scholes(spot_price, strike_price, risk_free_rate, volatility, expiry_date):
    calendar = ql.TARGET()
    day_count = ql.Actual365Fixed()

    spot_handle = ql.QuoteHandle(ql.SimpleQuote(spot_price))
    flat_ts = ql.YieldTermStructureHandle(ql.FlatForward(ql.Date().todaysDate(), risk_free_rate, day_count))

    flat_vol_ts = ql.BlackVolTermStructureHandle(ql.BlackConstantVol(ql.Date().todaysDate(), calendar, volatility, day_count))

    payoff = ql.PlainVanillaPayoff(ql.Option.Call, strike_price)
    exercise = ql.AmericanExercise(ql.Date().todaysDate(), expiry_date)

    american_option = ql.VanillaOption(payoff, exercise)

    engine = ql.BinomialVanillaEngine(american_option, "crr", 500)
    american_option.setPricingEngine(engine)

    price = american_option.NPV()
    #delta = american_option.delta()
    #gamma = american_option.gamma()

    return price
