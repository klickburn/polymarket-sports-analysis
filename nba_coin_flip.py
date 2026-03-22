"""
NBA Coin Flip Analysis: Fetch opening prices and test the same
Team B / underdog pattern we found in CBB.
"""

import os
import sys
import pandas as pd
import requests
import time

os.environ["PYTHONUNBUFFERED"] = "1"

CLOB_BASE = "https://clob.polymarket.com"
PRICES_CACHE = "db_nba_prices.parquet"
RATE_LIMIT_DELAY = 0.05
CHECKPOINT_EVERY = 100

session = requests.Session()
LOG = open("nba_backtest_output.log", "w")

def P(msg):
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


def fetch_all_prices():
    df = pd.read_parquet("db_markets.parquet")
    nba = df[(df["sport"] == "nba") & (df["winning_outcome"] != "Pending")].copy()
    nba = nba.sort_values("game_start_time", ascending=False).reset_index(drop=True)
    total = len(nba)

    if os.path.exists(PRICES_CACHE):
        cached = pd.read_parquet(PRICES_CACHE)
        done_ids = set(cached["condition_id"].values)
        rows = cached.to_dict("records")
    else:
        done_ids = set()
        rows = []

    remaining = nba[~nba["condition_id"].isin(done_ids)]
    P(f"Resolved NBA: {total} | Cached: {len(done_ids)} | To fetch: {len(remaining)}")

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
            if idx % 200 == 0:
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

        if new % 50 == 0:
            P(f"  [{len(done_ids)+idx}/{total}] fetched={new} no_hist={no_hist} err={errors}")

        if new % CHECKPOINT_EVERY == 0:
            pd.DataFrame(rows).to_parquet(PRICES_CACHE, index=False)

    prices_df = pd.DataFrame(rows)
    if len(prices_df):
        prices_df.to_parquet(PRICES_CACHE, index=False)
    P(f"Done! Cached {len(prices_df)} NBA markets ({new} new, {no_hist} no history, {errors} errors)")
    return prices_df


def analyze(prices_df):
    P("")
    P("=" * 65)
    P("  NBA COIN FLIP ANALYSIS")
    P("=" * 65)
    P("")

    df = prices_df.copy()
    df["fav_price"] = df[["price_a_open", "price_b_open"]].max(axis=1)
    df["dog_price"] = df[["price_a_open", "price_b_open"]].min(axis=1)
    df["favorite"] = df.apply(
        lambda r: r["outcome_team_a"] if r["price_a_open"] >= r["price_b_open"] else r["outcome_team_b"], axis=1)
    df["underdog"] = df.apply(
        lambda r: r["outcome_team_a"] if r["price_a_open"] < r["price_b_open"] else r["outcome_team_b"], axis=1)
    df["fav_won"] = df["winning_outcome"] == df["favorite"]
    df["dog_won"] = df["winning_outcome"] == df["underdog"]

    P(f"  Total NBA markets with price data: {len(df)}")
    P("")

    # Coin flips
    q = df[(df["fav_price"] >= 0.40) & (df["fav_price"] < 0.60)]
    P(f"  Coin flip markets (40-60%): {len(q)}")
    if len(q) == 0:
        P("  No coin flip markets found.")
        return

    # Team A vs B
    a_wins = int((q["winning_outcome"] == q["outcome_team_a"]).sum())
    b_wins = int((q["winning_outcome"] == q["outcome_team_b"]).sum())
    P(f"  Team A (listed first) wins:  {a_wins}/{len(q)} ({a_wins/len(q)*100:.1f}%)")
    P(f"  Team B (listed second) wins: {b_wins}/{len(q)} ({b_wins/len(q)*100:.1f}%)")
    P("")

    # Underdog win rate
    dw = int(q["dog_won"].sum())
    P(f"  Underdog win rate: {dw}/{len(q)} ({dw/len(q)*100:.1f}%)")
    P("")

    # 50.5% check
    exactly_505 = q[q["fav_price"] == 0.505]
    not_505 = q[q["fav_price"] != 0.505]
    P(f"  Markets at exactly 50.5%: {len(exactly_505)}")
    P(f"  Markets with real prices:  {len(not_505)}")
    if len(not_505):
        dw_real = int(not_505["dog_won"].sum())
        P(f"  Real-priced underdog win rate: {dw_real}/{len(not_505)} ({dw_real/len(not_505)*100:.1f}%)")
    P("")

    # P&L
    BET = 50
    q2 = q.copy()
    q2["shares"] = BET / q2["dog_price"]
    q2["pnl"] = q2.apply(lambda r: round(r["shares"] - BET, 2) if r["dog_won"] else -BET, axis=1)
    wins = int(q2["dog_won"].sum())
    losses = len(q2) - wins
    wagered = len(q2) * BET
    pnl = q2["pnl"].sum()
    roi = pnl / wagered * 100 if wagered else 0

    P(f"  $50 on underdog in coin flips:")
    P(f"    Bets: {len(q2)} | W: {wins} L: {losses} ({wins/len(q2)*100:.1f}%)")
    P(f"    Wagered: ${wagered:,.0f} | P&L: ${pnl:+,.2f} | ROI: {roi:+.1f}%")
    P("")

    # Price breakdown
    P("  BREAKDOWN BY PRICE TIER:")
    P("  " + "-" * 55)
    for lo, hi in [(0.40, 0.45), (0.45, 0.50), (0.50, 0.505), (0.505, 0.51), (0.51, 0.55), (0.55, 0.60)]:
        b = q[(q["fav_price"] >= lo) & (q["fav_price"] < hi)]
        if not len(b):
            continue
        bdw = int(b["dog_won"].sum())
        P(f"    fav@{lo*100:.1f}-{hi*100:.0f}%: {len(b):>4} games | dog wins: {bdw} ({bdw/len(b)*100:.1f}%)")

    # By month
    P("")
    P("  BY MONTH:")
    P("  " + "-" * 55)
    q3 = q.copy()
    q3["month"] = pd.to_datetime(q3["game_start_time"]).dt.to_period("M")
    for month, grp in q3.groupby("month"):
        gdw = int((grp["winning_outcome"] == grp["underdog"]).sum())
        P(f"    {month}: {len(grp):>4} games | dog wins: {gdw} ({gdw/len(grp)*100:.1f}%)")


if __name__ == "__main__":
    prices_df = fetch_all_prices()
    analyze(prices_df)
    LOG.close()
