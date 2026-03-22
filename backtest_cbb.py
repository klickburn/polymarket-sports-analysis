"""
CBB Backtest: $100 flat bets on 80%+ opening favorites
========================================================

Phase 1: Fetch & cache ALL opening prices to db_cbb_prices.parquet
Phase 2: Run backtest from cached data (instant, re-runnable)
"""

import os
import sys
import pandas as pd
import requests
import time

os.environ["PYTHONUNBUFFERED"] = "1"

# Write directly to log file for live tailing
_LOG_FILE = None

def P(*args, **kwargs):
    """Print to both stdout and log file with immediate flush."""
    msg = " ".join(str(a) for a in args)
    print(msg, flush=True)
    if _LOG_FILE:
        _LOG_FILE.write(msg + "\n")
        _LOG_FILE.flush()

# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------
CLOB_BASE = "https://clob.polymarket.com"
PRICES_CACHE = "db_cbb_prices.parquet"
BACKTEST_CSV = "backtest_cbb_results.csv"
BET_AMOUNT = 100.0
FAVORITE_THRESHOLD = 0.80
RATE_LIMIT_DELAY = 0.05
CHECKPOINT_EVERY = 100

session = requests.Session()


# --------------------------------------------------------------------------
# API helpers
# --------------------------------------------------------------------------

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


# --------------------------------------------------------------------------
# Phase 1: Fetch & cache
# --------------------------------------------------------------------------

def fetch_all_prices():
    df = pd.read_parquet("db_markets.parquet")
    cbb = df[(df["sport"] == "cbb") & (df["winning_outcome"] != "Pending")].copy()
    cbb = cbb.sort_values("game_start_time", ascending=False).reset_index(drop=True)
    total = len(cbb)

    # Load cache
    if os.path.exists(PRICES_CACHE):
        cached = pd.read_parquet(PRICES_CACHE)
        done_ids = set(cached["condition_id"].values)
        rows = cached.to_dict("records")
    else:
        done_ids = set()
        rows = []

    remaining = cbb[~cbb["condition_id"].isin(done_ids)]
    P(f"Resolved CBB: {total} | Cached: {len(done_ids)} | To fetch: {len(remaining)}")
    P()

    new = 0
    no_hist = 0
    errors = 0

    for idx, (_, mkt) in enumerate(remaining.iterrows(), 1):
        cond_id = mkt["condition_id"]

        tokens = fetch_market_tokens(cond_id)
        if not tokens:
            errors += 1
            P(f"  [{len(done_ids)+idx}/{total}] ERR no tokens | new={new} no_hist={no_hist} err={errors}")
            continue

        time.sleep(RATE_LIMIT_DELAY)

        team_a = mkt["outcome_team_a"]
        team_b = mkt["outcome_team_b"]
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
                P(f"  [{len(done_ids)+idx}/{total}] no history | new={new} no_hist={no_hist} err={errors}")
            continue

        price_b = round(1.0 - price_a, 6)
        row = {
            "condition_id": cond_id,
            "match_title": mkt["match_title"],
            "game_start_time": mkt["game_start_time"],
            "outcome_team_a": team_a,
            "outcome_team_b": team_b,
            "price_a_open": round(price_a, 6),
            "price_b_open": price_b,
            "winning_outcome": mkt["winning_outcome"],
        }
        rows.append(row)
        new += 1

        # Show every fetched market with running backtest stats
        fav_p = max(price_a, 1 - price_a)
        fav = team_a if price_a >= (1 - price_a) else team_b
        won = mkt["winning_outcome"] == fav
        tag = "✓" if won else "✗"

        # Running stats on qualifying bets only
        qual = [r for r in rows if max(r["price_a_open"], r["price_b_open"]) >= FAVORITE_THRESHOLD]
        qw = sum(1 for r in qual if r["winning_outcome"] == (r["outcome_team_a"] if r["price_a_open"] >= r["price_b_open"] else r["outcome_team_b"]))
        ql = len(qual) - qw

        if fav_p >= FAVORITE_THRESHOLD:
            P(f"  BET #{len(qual):>4} {tag} {mkt['match_title'][:42]:<42} fav@{fav_p:.0%} | W:{qw} L:{ql} ({qw/max(len(qual),1)*100:.1f}%)")
        elif new % 50 == 0:
            P(f"  [{len(done_ids)+idx}/{total}] fetched={new} | 80%+ bets: {len(qual)} (W:{qw} L:{ql}) | no_hist={no_hist}")

        # Checkpoint
        if new % CHECKPOINT_EVERY == 0:
            pd.DataFrame(rows).to_parquet(PRICES_CACHE, index=False)

    # Final save
    prices_df = pd.DataFrame(rows)
    if len(prices_df):
        prices_df.to_parquet(PRICES_CACHE, index=False)
    P()
    P(f"Done! Cached {len(prices_df)} markets ({new} new, {no_hist} no history, {errors} errors)")
    return prices_df


# --------------------------------------------------------------------------
# Phase 2: Backtest
# --------------------------------------------------------------------------

def run_backtest(prices_df=None):
    if prices_df is None:
        if not os.path.exists(PRICES_CACHE):
            print("No cache. Run fetch first.")
            return
        prices_df = pd.read_parquet(PRICES_CACHE)

    P()
    P("=" * 65)
    P(f"  CBB BACKTEST: ${BET_AMOUNT:.0f} flat bet on {FAVORITE_THRESHOLD:.0%}+ opening favorites")
    P("=" * 65)
    P()

    prices_df["fav_price"] = prices_df[["price_a_open", "price_b_open"]].max(axis=1)
    q = prices_df[prices_df["fav_price"] >= FAVORITE_THRESHOLD].copy()

    q["favorite"] = q.apply(
        lambda r: r["outcome_team_a"] if r["price_a_open"] >= r["price_b_open"] else r["outcome_team_b"], axis=1)
    q["underdog"] = q.apply(
        lambda r: r["outcome_team_b"] if r["price_a_open"] >= r["price_b_open"] else r["outcome_team_a"], axis=1)
    q["shares"] = BET_AMOUNT / q["fav_price"]
    q["won"] = q["winning_outcome"] == q["favorite"]
    q["pnl"] = q.apply(lambda r: round(r["shares"] - BET_AMOUNT, 2) if r["won"] else -BET_AMOUNT, axis=1)

    out = q[["match_title", "game_start_time", "favorite", "underdog",
             "fav_price", "winning_outcome", "won", "pnl"]].copy()
    out.columns = ["match", "game_date", "favorite", "underdog",
                   "opening_price", "winner", "won", "pnl"]
    out["bet_amount"] = BET_AMOUNT
    out = out.sort_values("game_date")
    out.to_csv(BACKTEST_CSV, index=False)

    n = len(out)
    wins = int(out["won"].sum())
    losses = n - wins
    wr = wins / n * 100 if n else 0
    wagered = n * BET_AMOUNT
    total_pnl = out["pnl"].sum()
    roi = total_pnl / wagered * 100 if wagered else 0
    avg_w = out[out["won"]]["pnl"].mean() if wins else 0
    avg_l = out[~out["won"]]["pnl"].mean() if losses else 0

    P(f"  Markets with price data: {len(prices_df)}")
    P(f"  Qualifying bets (80%+): {n}")
    P(f"  Wins:                   {wins}")
    P(f"  Losses:                 {losses}")
    P(f"  Win Rate:               {wr:.1f}%")
    P()
    P(f"  Total Wagered:          ${wagered:,.2f}")
    P(f"  Total P&L:              ${total_pnl:+,.2f}")
    P(f"  ROI:                    {roi:+.2f}%")
    P()
    P(f"  Avg Win Profit:         ${avg_w:+,.2f}")
    P(f"  Avg Loss:               ${avg_l:+,.2f}")
    P()
    P("  BREAKDOWN BY OPENING PRICE:")
    P("  " + "-" * 60)
    for lo, hi in [(0.80, 0.85), (0.85, 0.90), (0.90, 0.95), (0.95, 1.01)]:
        b = out[(out["opening_price"] >= lo) & (out["opening_price"] < hi)]
        if not len(b):
            continue
        bw = int(b["won"].sum())
        bl = len(b) - bw
        bwr = bw / len(b) * 100
        bpnl = b["pnl"].sum()
        P(f"    {lo*100:.0f}%-{hi*100:.0f}%: {len(b):>4} bets | {bw}W-{bl}L ({bwr:.1f}%) | P&L: ${bpnl:>+10,.2f}")

    P()
    P(f"  Results saved to: {BACKTEST_CSV}")


if __name__ == "__main__":
    _LOG_FILE = open("backtest_output.log", "w")
    prices_df = fetch_all_prices()
    run_backtest(prices_df)
    _LOG_FILE.close()
