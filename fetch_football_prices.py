"""
Fetch NFL & CFB opening prices from Polymarket CLOB API.
Then run Team B analysis on the fetched data.
"""

import os
import pandas as pd
import requests
import time

os.environ["PYTHONUNBUFFERED"] = "1"

CLOB_BASE = "https://clob.polymarket.com"
RATE_LIMIT_DELAY = 0.05
CHECKPOINT_EVERY = 100

session = requests.Session()
LOG = open("football_fetch_output.log", "w")

def P(msg=""):
    print(msg, flush=True)
    LOG.write(msg + "\n")
    LOG.flush()


def fetch_market_tokens(condition_id):
    try:
        r = session.get(f"{CLOB_BASE}/markets/{condition_id}", timeout=20)
        if r.status_code != 200:
            return None
        tokens = r.json().get("tokens", [])
        return tokens if len(tokens) == 2 else None
    except Exception:
        return None


def fetch_opening_price(token_id):
    try:
        r = session.get(
            f"{CLOB_BASE}/prices-history",
            params={"market": token_id, "interval": "max", "fidelity": 60},
            timeout=20,
        )
        if r.status_code != 200:
            return None
        hist = r.json().get("history", [])
        return hist[0]["p"] if hist else None
    except Exception:
        return None


def fetch_prices_for_sport(sport, cache_file):
    df = pd.read_parquet("db_markets.parquet")
    sdf = df[(df["sport"] == sport) & (df["winning_outcome"] != "Pending")].copy()
    sdf = sdf.sort_values("game_start_time", ascending=False).reset_index(drop=True)
    total = len(sdf)

    if os.path.exists(cache_file):
        cached = pd.read_parquet(cache_file)
        done_ids = set(cached["condition_id"].values)
        rows = cached.to_dict("records")
    else:
        done_ids = set()
        rows = []

    remaining = sdf[~sdf["condition_id"].isin(done_ids)]
    P(f"Resolved {sport.upper()}: {total} | Cached: {len(done_ids)} | To fetch: {len(remaining)}")

    new = 0
    no_hist = 0
    errors = 0

    for idx, (_, mkt) in enumerate(remaining.iterrows(), 1):
        cond_id = mkt["condition_id"]
        tokens = fetch_market_tokens(cond_id)
        if not tokens:
            errors += 1
            continue
        time.sleep(RATE_LIMIT_DELAY)

        team_a = mkt["outcome_team_a"]
        t_map = {t["outcome"]: t["token_id"] for t in tokens}
        token_a_id = t_map.get(team_a)
        if not token_a_id:
            errors += 1
            continue

        price_a = fetch_opening_price(token_a_id)
        time.sleep(RATE_LIMIT_DELAY)

        if price_a is None:
            no_hist += 1
            if idx % 100 == 0:
                P(f"  [{len(done_ids)+idx}/{total}] fetched={new} no_hist={no_hist} err={errors}")
            continue

        rows.append({
            "condition_id": cond_id,
            "match_title": mkt["match_title"],
            "game_start_time": mkt["game_start_time"],
            "outcome_team_a": team_a,
            "outcome_team_b": mkt["outcome_team_b"],
            "price_a_open": round(price_a, 6),
            "price_b_open": round(1.0 - price_a, 6),
            "winning_outcome": mkt["winning_outcome"],
        })
        new += 1

        if new % 25 == 0:
            P(f"  [{len(done_ids)+idx}/{total}] fetched={new} no_hist={no_hist} err={errors}")

        if new % CHECKPOINT_EVERY == 0:
            pd.DataFrame(rows).to_parquet(cache_file, index=False)

    prices_df = pd.DataFrame(rows)
    if len(prices_df):
        prices_df.to_parquet(cache_file, index=False)
    P(f"Done {sport.upper()}! Cached {len(prices_df)} ({new} new, {no_hist} no history, {errors} errors)")
    P()
    return prices_df


def analyze_team_b(prices_df, sport_name):
    if len(prices_df) == 0:
        P(f"  No data for {sport_name}")
        return

    df = prices_df.copy()
    df["fav_price"] = df[["price_a_open", "price_b_open"]].max(axis=1)
    df["team_b_won"] = df["winning_outcome"] == df["outcome_team_b"]
    df["team_a_won"] = df["winning_outcome"] == df["outcome_team_a"]

    P(f"  {sport_name} ANALYSIS ({len(df)} markets):")
    P("  " + "-" * 55)

    # Overall Team B
    bw = int(df["team_b_won"].sum())
    P(f"    Overall Team B wins: {bw}/{len(df)} ({bw/len(df)*100:.1f}%)")

    # Coin flips
    coin = df[(df["fav_price"] >= 0.40) & (df["fav_price"] < 0.60)]
    if len(coin):
        cbw = int(coin["team_b_won"].sum())
        P(f"    Coin flip (40-60%) Team B wins: {cbw}/{len(coin)} ({cbw/len(coin)*100:.1f}%)")

        # P&L
        BET = 50
        shares = BET / coin["price_b_open"]
        pnl = shares.where(coin["team_b_won"], 0) - BET
        total_pnl = pnl.sum()
        wagered = len(coin) * BET
        roi = total_pnl / wagered * 100
        P(f"    Coin flip $50 on B: {len(coin)} bets | P&L: ${total_pnl:+,.2f} | ROI: {roi:+.1f}%")

    # Team B underdog 45-50%
    dog = df[(df["fav_price"] >= 0.50) & (df["fav_price"] < 0.55) & (df["price_b_open"] < df["price_a_open"])]
    if len(dog):
        dbw = int(dog["team_b_won"].sum())
        P(f"    B underdog (45-50%) wins: {dbw}/{len(dog)} ({dbw/len(dog)*100:.1f}%)")

    # Fade A
    BET = 50
    shares = BET / df["price_b_open"]
    pnl = shares.where(df["team_b_won"], 0) - BET
    total_pnl = pnl.sum()
    wagered = len(df) * BET
    roi = total_pnl / wagered * 100
    P(f"    Fade A (all markets): {len(df)} bets | P&L: ${total_pnl:+,.2f} | ROI: {roi:+.1f}%")
    P()


if __name__ == "__main__":
    P("=" * 65)
    P("  FETCHING NFL & CFB OPENING PRICES")
    P("=" * 65)
    P()

    nfl_df = fetch_prices_for_sport("nfl", "db_nfl_prices.parquet")
    cfb_df = fetch_prices_for_sport("cfb", "db_cfb_prices.parquet")

    P("=" * 65)
    P("  TEAM B ANALYSIS — FOOTBALL")
    P("=" * 65)
    P()

    analyze_team_b(nfl_df, "NFL")
    analyze_team_b(cfb_df, "CFB")

    LOG.close()
