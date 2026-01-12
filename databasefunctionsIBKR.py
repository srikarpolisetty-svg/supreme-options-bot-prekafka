def get_closest_strike_ibkr(strikes, target):
    """
    strikes: iterable of numeric strikes from IBKR (securityDefinitionOptionParameter)
    target: desired strike (float)

    returns: closest listed strike (float)
    """
    strikes = list(strikes)
    if not strikes:
        raise ValueError("No strikes provided (empty chain).")

    return min(strikes, key=lambda s: abs(float(s) - float(target)))







def get_option_quote_ibkr(self, conId: int):
    """
    Returns the latest cached quote for a given option conId.

    Output matches your yfinance keys as closely as possible:
      last_price, bid, ask, mid,
      volume, iv, oi,
      spread, spread_pct

    Note:
    - IV comes from tickOptionComputation (if you have options data permission)
    - OI is NOT provided via standard streaming mkt data; you usually need a different source
      (or treat as None).
    """
    q = self.quote_by_conid.get(conId)
    if not q:
        return None

    bid = q.get("bid")
    ask = q.get("ask")

    mid = None
    spread = None
    spread_pct = None
    if bid is not None and ask is not None:
        mid = (bid + ask) / 2
        spread = ask - bid
        spread_pct = (spread / mid) * 100 if mid else 0.0

    return {
        "last_price": q.get("last"),
        "bid": bid,
        "ask": ask,
        "mid": mid,
        "volume": q.get("volume"),
        "iv": q.get("iv"),          # may be None if not available
        "oi": q.get("oi"),          # likely None unless you source it elsewhere
        "spread": spread,
        "spread_pct": spread_pct,
    }



