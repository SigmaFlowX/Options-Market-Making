import QuantLib as ql

def solve_black_scholes(spot_price, strike_price, risk_free_rate, volatility, expiry_date, eval_date, option_type):

    expiry_date = ql.Date(expiry_date.day, expiry_date.month, expiry_date.year)
    eval_date = ql.Date(eval_date.day, eval_date.month, eval_date.year)

    calendar = ql.TARGET()
    day_count = ql.Actual365Fixed()
    ql.Settings.instance().evaluationDate = eval_date

    spot_handle = ql.QuoteHandle(ql.SimpleQuote(spot_price))
    flat_ts = ql.YieldTermStructureHandle(ql.FlatForward(eval_date, risk_free_rate, day_count))

    flat_vol_ts = ql.BlackVolTermStructureHandle(ql.BlackConstantVol(eval_date, calendar, volatility, day_count))
    dividend_ts = ql.YieldTermStructureHandle(ql.FlatForward(eval_date, 0.0, day_count)
    )
    process = ql.BlackScholesMertonProcess(
        spot_handle,
        dividend_ts,
        flat_ts,
        flat_vol_ts
    )

    if option_type == "call":
        payoff = ql.PlainVanillaPayoff(ql.Option.Call, strike_price)
    elif option_type == "put":
        payoff = ql.PlainVanillaPayoff(ql.Option.Put, strike_price)
    else:
        print("Incorrect option type, None is returned")
        return None
    exercise = ql.AmericanExercise(eval_date, expiry_date)

    american_option = ql.VanillaOption(payoff, exercise)

    engine = ql.BinomialVanillaEngine(process, "crr", 500)
    american_option.setPricingEngine(engine)

    price = american_option.NPV()
    delta = american_option.delta()
    gamma = american_option.gamma()

    dict = {
        'price':price,
        'delta':delta,
        'gamma':gamma
    }

    return dict

if __name__ == "__main__":
    price = 300
    strike = 290
    expiry = ql.Date(20, 2, 2026)
    rf = 0.15
    volatility = 0.2
    eval_date = ql.Date(12, 2, 2026)

    print(solve_black_scholes(price, strike, rf, volatility, expiry, eval_date))