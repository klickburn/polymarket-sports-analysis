"""
Hybrid Strategy: Combine all best edges into a tiered betting system.
=====================================================================
Tier 1: Team B favorite at 75-90% → $100 bet (~87% WR)
Tier 2: CBB coin flips (fav 40-55%) → $50 bet on Team B (~61% WR)
Tier 3: Team B underdog at 45-50% in CBB → $75 bet (~66% WR)
Tier 4: NBA exact 50/50 → $50 bet on Team B (~65% WR)
Tier 5: UCL/EPL coin flips → $50 bet on Team B (~67% WR)

Markets assigned to HIGHEST qualifying tier (no double counting).
"""

import os
import pandas as pd
import numpy as np

os.environ["PYTHONUNBUFFERED"] = "1"
LOG = open("hybrid_strategy_output.log", "w")

def P(msg=""):
    print(msg, flush=True)
    LOG.write(msg + "\n")
    LOG.flush()


def load_all():
    frames = []
    # EPL/UCL excluded: soccer markets are 3-way (win/lose/draw) with Yes/No outcomes
    # so "Team B" analysis was invalid there (No wins 67% by definition in 3-outcome markets)
    for sport, f in [("cbb", "db_cbb_prices.parquet"), ("nba", "db_nba_prices.parquet")]:
        if not os.path.exists(f):
            continue
        df = pd.read_parquet(f)
        df["sport"] = sport
        frames.append(df)
        P(f"  Loaded {sport}: {len(df)} markets")
    all_df = pd.concat(frames, ignore_index=True)
    all_df["game_dt"] = pd.to_datetime(all_df["game_start_time"])
    all_df["fav_price"] = all_df[["price_a_open", "price_b_open"]].max(axis=1)
    all_df["team_b_won"] = all_df["winning_outcome"] == all_df["outcome_team_b"]
    all_df["is_coin_flip"] = (all_df["fav_price"] >= 0.40) & (all_df["fav_price"] < 0.60)
    all_df["team_b_is_fav"] = all_df["price_b_open"] > all_df["price_a_open"]
    all_df["team_b_is_dog"] = all_df["price_b_open"] < all_df["price_a_open"]
    return all_df.sort_values("game_dt").reset_index(drop=True)


def assign_tier(row):
    sport = row["sport"]
    fav_p = row["fav_price"]
    pb = row["price_b_open"]
    b_is_fav = row["team_b_is_fav"]
    b_is_dog = row["team_b_is_dog"]
    is_coin = row["is_coin_flip"]

    # Tier 1: Team B favorite at 75-90% (any sport)
    if b_is_fav and 0.75 <= pb < 0.90:
        return 1, 100

    # Tier 3: CBB Team B underdog, fav at 50-55% (tightest edge)
    if sport == "cbb" and b_is_dog and 0.50 <= fav_p < 0.55:
        return 3, 75

    # Tier 4: NBA exact 50/50 (fav at 50-50.5%)
    if sport == "nba" and fav_p < 0.505 + 0.001:
        return 4, 50

    # Tier 2: CBB coin flips (broader)
    if sport == "cbb" and is_coin:
        return 2, 50

    return 0, 0


def run():
    P("=" * 70)
    P("  HYBRID STRATEGY BACKTEST")
    P("=" * 70)
    P()

    all_df = load_all()
    P(f"  Total markets: {len(all_df)}")
    P()

    # Assign tiers
    tier_data = all_df.apply(assign_tier, axis=1, result_type="expand")
    all_df["tier"] = tier_data[0]
    all_df["bet_amount"] = tier_data[1]

    # Filter to qualifying bets only
    bets = all_df[all_df["tier"] > 0].copy()
    P(f"  Qualifying bets: {len(bets)}")
    P()

    # Calculate P&L
    bets["shares"] = bets["bet_amount"] / bets["price_b_open"]
    bets["pnl"] = bets.apply(
        lambda r: round(r["shares"] - r["bet_amount"], 2) if r["team_b_won"] else -r["bet_amount"], axis=1)
    bets["cum_pnl"] = bets["pnl"].cumsum()

    # Per-tier breakdown
    tier_names = {1: "T1: B fav 75-90% ($100)", 2: "T2: CBB coin flip ($50)",
                  3: "T3: CBB B dog 50-55% ($75)", 4: "T4: NBA 50/50 ($50)"}

    P("  PER-TIER BREAKDOWN:")
    P("  " + "-" * 85)
    P(f"  {'Tier':<30} | {'Bets':>5} | {'W':>4} | {'L':>4} | {'WR%':>6} | {'Wagered':>10} | {'P&L':>10} | {'ROI':>7}")
    P("  " + "-" * 85)

    for t in sorted(bets["tier"].unique()):
        tb = bets[bets["tier"] == t]
        w = int(tb["team_b_won"].sum())
        l = len(tb) - w
        wr = w / len(tb) * 100
        wag = tb["bet_amount"].sum()
        pnl = tb["pnl"].sum()
        roi = pnl / wag * 100 if wag else 0
        P(f"  {tier_names.get(t, f'Tier {t}'):<30} | {len(tb):>5} | {w:>4} | {l:>4} | {wr:>5.1f}% | ${wag:>9,.0f} | ${pnl:>+9,.2f} | {roi:>+6.1f}%")

    # Overall
    P("  " + "-" * 85)
    total_bets = len(bets)
    total_wins = int(bets["team_b_won"].sum())
    total_losses = total_bets - total_wins
    total_wr = total_wins / total_bets * 100
    total_wagered = bets["bet_amount"].sum()
    total_pnl = bets["pnl"].sum()
    total_roi = total_pnl / total_wagered * 100
    P(f"  {'TOTAL':<30} | {total_bets:>5} | {total_wins:>4} | {total_losses:>4} | {total_wr:>5.1f}% | ${total_wagered:>9,.0f} | ${total_pnl:>+9,.2f} | {total_roi:>+6.1f}%")
    P()

    # Max drawdown
    peak = 0
    max_dd = 0
    for cp in bets["cum_pnl"]:
        if cp > peak:
            peak = cp
        dd = peak - cp
        if dd > max_dd:
            max_dd = dd
    P(f"  Peak P&L:      ${bets['cum_pnl'].max():>+,.2f}")
    P(f"  Max Drawdown:  ${max_dd:>,.2f}")
    P(f"  Final P&L:     ${total_pnl:>+,.2f}")
    P()

    # Monthly P&L
    P("  MONTHLY P&L:")
    P("  " + "-" * 60)
    bets["month"] = bets["game_dt"].dt.to_period("M")
    for month, grp in bets.groupby("month"):
        mw = int(grp["team_b_won"].sum())
        ml = len(grp) - mw
        mpnl = grp["pnl"].sum()
        P(f"    {month}: {len(grp):>4} bets | {mw}W-{ml}L | P&L: ${mpnl:>+,.2f}")

    # Equity curve (every 25 bets)
    P()
    P("  EQUITY CURVE (cumulative P&L):")
    P("  " + "-" * 40)
    for i in range(0, len(bets), 25):
        row = bets.iloc[min(i + 24, len(bets) - 1)]
        P(f"    Bet #{i+1:>4}-{min(i+25, len(bets)):>4}: cum P&L = ${row['cum_pnl']:>+,.2f}")

    # Save results
    out = bets[["match_title", "game_dt", "sport", "tier", "outcome_team_b",
                "price_b_open", "bet_amount", "team_b_won", "pnl", "cum_pnl"]].copy()
    out.columns = ["match", "date", "sport", "tier", "team_b", "price_b", "bet", "won", "pnl", "cum_pnl"]
    out.to_csv("hybrid_strategy_results.csv", index=False)
    P(f"\n  Results saved to hybrid_strategy_results.csv")


if __name__ == "__main__":
    run()
    LOG.close()
