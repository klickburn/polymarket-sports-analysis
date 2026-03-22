"""
Hybrid Strategy Backtest with 3% Compounding
=============================================
All 4 tiers running together, betting 3% of bankroll on each qualifying market.
Starting bankroll: $100.
"""

import os
import pandas as pd
import numpy as np

os.environ["PYTHONUNBUFFERED"] = "1"


def P(msg=""):
    print(msg, flush=True)


def load_all():
    frames = []
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
    all_df["team_b_is_fav"] = all_df["price_b_open"] > all_df["price_a_open"]
    all_df["team_b_is_dog"] = all_df["price_b_open"] < all_df["price_a_open"]
    all_df["is_coin_flip"] = (all_df["fav_price"] >= 0.40) & (all_df["fav_price"] < 0.60)
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
        return 1

    # Tier 3: CBB Team B underdog, fav at 50-55%
    if sport == "cbb" and b_is_dog and 0.50 <= fav_p < 0.55:
        return 3

    # Tier 4: NBA exact 50/50 (fav at 50-50.5%)
    if sport == "nba" and fav_p < 0.506:
        return 4

    # Tier 2: CBB coin flips (broader)
    if sport == "cbb" and is_coin:
        return 2

    return 0


def run():
    P("=" * 75)
    P("  HYBRID 4-TIER BACKTEST — 3% COMPOUNDING FROM $100")
    P("=" * 75)
    P()

    all_df = load_all()
    all_df["tier"] = all_df.apply(assign_tier, axis=1)
    bets = all_df[all_df["tier"] > 0].copy().reset_index(drop=True)
    P(f"  Total qualifying bets: {len(bets)}")
    P()

    # ── Simulate with compounding ──────────────────────────────────
    BANKROLL_START = 100.0
    BET_PCT = 0.03
    MIN_BET = 0.50

    bankroll = BANKROLL_START
    peak = bankroll
    max_dd = 0
    max_dd_pct = 0

    tier_stats = {1: {"w": 0, "l": 0, "pnl": 0}, 2: {"w": 0, "l": 0, "pnl": 0},
                  3: {"w": 0, "l": 0, "pnl": 0}, 4: {"w": 0, "l": 0, "pnl": 0}}

    results = []
    monthly = {}

    for _, row in bets.iterrows():
        if bankroll < MIN_BET:
            break

        tier = row["tier"]
        pb = row["price_b_open"]
        won = row["team_b_won"]
        month = row["game_dt"].strftime("%Y-%m")

        bet = bankroll * BET_PCT
        bet = max(MIN_BET, bet)
        bet = min(bet, bankroll)

        shares = bet / pb

        if won:
            pnl = shares - bet  # shares * $1 - cost
            tier_stats[tier]["w"] += 1
        else:
            pnl = -bet
            tier_stats[tier]["l"] += 1

        tier_stats[tier]["pnl"] += pnl
        bankroll += pnl

        if bankroll > peak:
            peak = bankroll
        dd = peak - bankroll
        dd_pct = dd / peak * 100 if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd
        if dd_pct > max_dd_pct:
            max_dd_pct = dd_pct

        results.append({
            "bet_num": len(results) + 1,
            "match": row["match_title"],
            "sport": row["sport"],
            "tier": tier,
            "team_b": row["outcome_team_b"],
            "price_b": pb,
            "bet": round(bet, 2),
            "won": won,
            "pnl": round(pnl, 2),
            "bankroll": round(bankroll, 2),
        })

        if month not in monthly:
            monthly[month] = {"bets": 0, "w": 0, "l": 0, "pnl": 0, "start_bank": round(bankroll - pnl, 2)}
        monthly[month]["bets"] += 1
        monthly[month]["pnl"] += pnl
        if won:
            monthly[month]["w"] += 1
        else:
            monthly[month]["l"] += 1

    # ── Results ────────────────────────────────────────────────────
    total_bets = len(results)
    total_wins = sum(1 for r in results if r["won"])
    total_losses = total_bets - total_wins
    total_return = (bankroll - BANKROLL_START) / BANKROLL_START * 100

    P("  PER-TIER BREAKDOWN (3% compounding):")
    P("  " + "-" * 85)
    tier_names = {1: "T1: B fav 75-90%", 2: "T2: CBB coin flip", 3: "T3: CBB B dog 50-55%", 4: "T4: NBA 50/50"}
    P(f"  {'Tier':<25} | {'Bets':>5} | {'W':>4} | {'L':>4} | {'WR%':>6} | {'P&L':>12}")
    P("  " + "-" * 85)
    for t in [1, 2, 3, 4]:
        s = tier_stats[t]
        total = s["w"] + s["l"]
        wr = s["w"] / total * 100 if total else 0
        P(f"  {tier_names[t]:<25} | {total:>5} | {s['w']:>4} | {s['l']:>4} | {wr:>5.1f}% | ${s['pnl']:>+11,.2f}")

    P("  " + "-" * 85)
    P(f"  {'TOTAL':<25} | {total_bets:>5} | {total_wins:>4} | {total_losses:>4} | {total_wins/total_bets*100:>5.1f}% | ${bankroll - BANKROLL_START:>+11,.2f}")
    P()

    P("  BANKROLL SUMMARY:")
    P("  " + "-" * 50)
    P(f"  Starting bankroll:   ${BANKROLL_START:>12,.2f}")
    P(f"  Final bankroll:      ${bankroll:>12,.2f}")
    P(f"  Total return:        {total_return:>+11.1f}%")
    P(f"  Peak bankroll:       ${peak:>12,.2f}")
    P(f"  Max drawdown:        ${max_dd:>12,.2f} ({max_dd_pct:.1f}%)")
    P(f"  Total bets:          {total_bets:>12}")
    P(f"  Bet sizing:          {BET_PCT*100:.0f}% of bankroll")
    P()

    # Monthly breakdown
    P("  MONTHLY GROWTH:")
    P("  " + "-" * 75)
    P(f"  {'Month':<10} | {'Bets':>5} | {'W-L':>7} | {'WR%':>6} | {'Month P&L':>12} | {'End Bankroll':>14}")
    P("  " + "-" * 75)
    running_bank = BANKROLL_START
    for month in sorted(monthly.keys()):
        m = monthly[month]
        wr = m["w"] / m["bets"] * 100
        running_bank += m["pnl"]
        P(f"  {month:<10} | {m['bets']:>5} | {m['w']}W-{m['l']}L{'':<1} | {wr:>5.1f}% | ${m['pnl']:>+11,.2f} | ${running_bank:>13,.2f}")
    P()

    # Equity curve milestones
    P("  EQUITY CURVE (bankroll at milestones):")
    P("  " + "-" * 50)
    milestones = [1, 10, 25, 50, 100, 200, 300, 400, 500, 600, 700, 800, 900, len(results)]
    for m in milestones:
        if m <= len(results):
            r = results[m - 1]
            P(f"    Bet #{m:>4}: ${r['bankroll']:>12,.2f}")
    P()

    # Worst losing streaks
    P("  WORST LOSING STREAKS:")
    P("  " + "-" * 50)
    streak = 0
    max_streak = 0
    streak_start = 0
    streaks = []
    for i, r in enumerate(results):
        if not r["won"]:
            if streak == 0:
                streak_start = i
            streak += 1
        else:
            if streak >= 4:
                streaks.append((streak, streak_start, i - 1))
            streak = 0
    if streak >= 4:
        streaks.append((streak, streak_start, len(results) - 1))

    streaks.sort(reverse=True)
    for length, start, end in streaks[:5]:
        bank_before = results[start]["bankroll"] - results[start]["pnl"]  # approx
        bank_after = results[end]["bankroll"]
        loss = bank_before - bank_after
        P(f"    {length} losses in a row (bets #{start+1}-#{end+1}): lost ${loss:,.2f}")
    P()

    # Compare with flat betting
    P("  COMPARISON: 3% Compounding vs Flat $3 bet:")
    P("  " + "-" * 50)
    flat_bank = BANKROLL_START
    for _, row in bets.iterrows():
        pb = row["price_b_open"]
        flat_bet = min(3.0, flat_bank)
        if flat_bank < 0.50:
            break
        shares = flat_bet / pb
        if row["team_b_won"]:
            flat_bank += shares - flat_bet
        else:
            flat_bank -= flat_bet

    P(f"  3% Compounding:  ${BANKROLL_START:>8,.2f} → ${bankroll:>12,.2f}  ({total_return:>+.1f}%)")
    P(f"  Flat $3 bet:     ${BANKROLL_START:>8,.2f} → ${flat_bank:>12,.2f}  ({(flat_bank-BANKROLL_START)/BANKROLL_START*100:>+.1f}%)")
    P()

    # Save
    pd.DataFrame(results).to_csv("hybrid_compounding_results.csv", index=False)
    P(f"  Results saved to hybrid_compounding_results.csv")


if __name__ == "__main__":
    run()
