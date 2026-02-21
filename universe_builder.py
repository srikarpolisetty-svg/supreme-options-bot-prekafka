import duckdb 




DB_PATH = "definitioncache.duckdb"
con = duckdb.connect(DB_PATH)

symbols = con.execute("""
SELECT DISTINCT symbol
FROM definition_cache
""").fetchdf()["symbol"].tolist()

print(len(symbols))

for symbol in symbols:
    df = con.execute("""
    SELECT strike_price
    FROM definition_cache
    WHERE symbol = ?
    """, [symbol]).fetchdf()

    strikes = df["strike_price"].tolist()
    print(strikes)


def build_raw_symbol_universe(
    symbols: list[str],
    underlying_px: dict[str, float],
) -> tuple[list[str], list[tuple], dict[str, dict]]:
    hist = db.Historical(DATABENTO_API_KEY)
    today_utc = dt.datetime.now(tz=pytz.UTC).date()
    ts_refresh = utc_now_naive()

    all_raw_symbols: list[str] = []
    strike_rows: list[tuple] = []
    meta: dict[str, dict] = {}

    for sym in symbols:
        px = underlying_px.get(sym)
        if px is None or px <= 0:
            continue



        if chain_df is None or chain_df.empty:
            continue

        chain_df["exp_yyyymmdd"] = chain_df["expiration"].dt.strftime("%Y%m%d")
        expirations = sorted(chain_df["exp_yyyymmdd"].unique())
        exp = get_friday_within_4_days(expirations, today_utc)
        if exp is None:
            continue

        sub = chain_df[chain_df["exp_yyyymmdd"] == exp]
        if sub.empty:
            continue

        strikes = sorted(sub["strike_price"].astype(float).unique())
        if not strikes:
            continue

        strike_map = pick_strikes(px, strikes)

        atm = strike_map["ATM"]
        c1 = strike_map["C1"]
        p1 = strike_map["P1"]
        c2 = strike_map["C2"]
        p2 = strike_map["P2"]

        raw_atm_c = pick_raw_symbol(sub, atm, "C")
        raw_atm_p = pick_raw_symbol(sub, atm, "P")
        raw_c1 = pick_raw_symbol(sub, c1, "C")
        raw_p1 = pick_raw_symbol(sub, p1, "P")
        raw_c2 = pick_raw_symbol(sub, c2, "C")
        raw_p2 = pick_raw_symbol(sub, p2, "P")

        raws = [raw_atm_c, raw_atm_p, raw_c1, raw_p1, raw_c2, raw_p2]
        if any(x is None for x in raws):
            continue

        exp_date = datetime.datetime.strptime(exp, "%Y%m%d").date()

        strike_rows.extend(
            [
                (sym, "ATM", "C", exp, exp_date, float(atm), raw_atm_c, float(px), ts_refresh),
                (sym, "ATM", "P", exp, exp_date, float(atm), raw_atm_p, float(px), ts_refresh),
                (sym, "C1", "C", exp, exp_date, float(c1), raw_c1, float(px), ts_refresh),
                (sym, "P1", "P", exp, exp_date, float(p1), raw_p1, float(px), ts_refresh),
                (sym, "C2", "C", exp, exp_date, float(c2), raw_c2, float(px), ts_refresh),
                (sym, "P2", "P", exp, exp_date, float(p2), raw_p2, float(px), ts_refresh),
            ]
        )

        all_raw_symbols.extend(raws)
        meta[sym] = {"exp": exp, "underlying_price": float(px), "strike_map": strike_map, "raws": raws}

    seen = set()
    uniq = []
    for r in all_raw_symbols:
        if r not in seen:
            seen.add(r)
            uniq.append(r)

    return uniq, strike_rows, meta