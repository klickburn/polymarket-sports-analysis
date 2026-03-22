"""
Strategy Optimizer: Find the best combo of signals across all sports data.
==========================================================================
Combines: Team B bias, price tiers, sport, month, exact-50/50 detection.

Reads from all cached price parquets and tests every combination.
"""

import os
import pandas as pd
import numpy as np
from itertools import product

os.environ["PYTHONUNBUFFERED"] = "1"

LOG = open("strategy_optimizer_output.log", "w")

def P(msg=""):
    print(msg, flush=True)
    LOG.write(msg + "\n")
    LOG.flush()


def load_all_data():
    """Load and unify all cached price data across sports."""
    frames = []
    sport_files = {
        "cbb": "db_cbb_prices.parquet",
        "nba": "db_nba_prices.parquet",
        "epl": "db_epl_prices.parquet",
        "ucl": "db_ucl_prices.parquet",
    }
    for sport, fname in sport_files.items():
        path = fname
        if not os.path.exists(path):
            continue
        df = pd.read_parquet(path)
        df["sport"] = sport
        frames.append(df)
        P(f"  Loaded {sport}: {len(df)} markets")

    if not frames:
        P("No data found!")
        return None

    all_df = pd.concat(frames, ignore_index=True)
    all_df["game_dt"] = pd.to_datetime(all_df["game_start_time"])
    all_df["month"] = all_df["game_dt"].dt.to_period("M")
    all_df["fav_price"] = all_df[["price_a_open", "price_b_open"]].max(axis=1)
    all_df["dog_price"] = all_df[["price_a_open", "price_b_open"]].min(axis=1)
    all_df["favorite"] = all_df.apply(
        lambda r: r["outcome_team_a"] if r["price_a_open"] >= r["price_b_open"] else r["outcome_team_b"], axis=1)
    all_df["underdog"] = all_df.apply(
        lambda r: r["outcome_team_a"] if r["price_a_open"] < r["price_b_open"] else r["outcome_team_b"], axis=1)
    all_df["team_b"] = all_df["outcome_team_b"]
    all_df["fav_won"] = all_df["winning_outcome"] == all_df["favorite"]
    all_df["dog_won"] = all_df["winning_outcome"] == all_df["underdog"]
    all_df["team_b_won"] = all_df["winning_outcome"] == all_df["outcome_team_b"]
    all_df["team_a_won"] = all_df["winning_outcome"] == all_df["outcome_team_a"]
    all_df["is_exact_5050"] = all_df["fav_price"] == 0.505
    all_df["team_b_is_underdog"] = all_df["price_b_open"] < all_df["price_a_open"]
    all_df["team_b_is_favorite"] = all_df["price_b_open"] > all_df["price_a_open"]
    all_df["is_coin_flip"] = (all_df["fav_price"] >= 0.40) & (all_df["fav_price"] < 0.60)

    P(f"\n  Total unified dataset: {len(all_df)} markets")
    return all_df


def calc_pnl(subset, bet_col, win_col, price_col):
    """Calculate P&L for a subset. Returns dict with stats."""
    if len(subset) == 0:
        return None
    BET = 50
    wins = int(subset[win_col].sum())
    losses = len(subset) - wins
    wr = wins / len(subset) * 100

    # shares = bet / price_of_what_we_bet_on
    shares = BET / subset[price_col]
    pnl_series = shares.where(subset[win_col], 0) - BET
    total_pnl = pnl_series.sum()
    wagered = len(subset) * BET
    roi = total_pnl / wagered * 100 if wagered else 0

    return {
        "bets": len(subset),
        "wins": wins,
        "losses": losses,
        "win_rate": round(wr, 1),
        "wagered": wagered,
        "pnl": round(total_pnl, 2),
        "roi": round(roi, 1),
    }


def test_strategies(all_df):
    """Test a wide range of strategy combinations."""
    results = []

    # =====================================================================
    # STRATEGY 1: Team B in coin flips by sport
    # =====================================================================
    P("\n" + "=" * 70)
    P("  STRATEGY GROUP 1: BET ON TEAM B (listed second)")
    P("=" * 70)

    for sport in ["cbb", "nba", "epl", "ucl", "ALL"]:
        sdf = all_df if sport == "ALL" else all_df[all_df["sport"] == sport]
        for lo, hi, label in [
            (0.40, 0.505, "exact 50/50 only"),
            (0.40, 0.51, "40-51% (tight coin flip)"),
            (0.49, 0.515, "49-51.5% (super tight)"),
            (0.40, 0.55, "40-55%"),
            (0.40, 0.60, "40-60% (all coin flips)"),
            (0.45, 0.55, "45-55%"),
        ]:
            sub = sdf[(sdf["fav_price"] >= lo) & (sdf["fav_price"] < hi)]
            if len(sub) < 5:
                continue
            stats = calc_pnl(sub, "team_b", "team_b_won", "price_b_open")
            if stats:
                stats["strategy"] = f"Team B | {sport.upper()} | fav@{label}"
                results.append(stats)
                P(f"  {stats['strategy']:<55} | {stats['bets']:>4} bets | WR: {stats['win_rate']:>5.1f}% | ROI: {stats['roi']:>+6.1f}%")

    # =====================================================================
    # STRATEGY 2: Team B UNDERDOG only (B is listed second AND is the dog)
    # =====================================================================
    P("\n" + "=" * 70)
    P("  STRATEGY GROUP 2: TEAM B WHEN UNDERDOG")
    P("=" * 70)

    for sport in ["cbb", "nba", "epl", "ucl", "ALL"]:
        sdf = all_df if sport == "ALL" else all_df[all_df["sport"] == sport]
        for lo, hi, label in [
            (0.50, 0.55, "dog@45-50%"),
            (0.50, 0.60, "dog@40-50%"),
            (0.505, 0.55, "dog@45-49.5% (skip 50/50)"),
            (0.505, 0.60, "dog@40-49.5% (skip 50/50)"),
        ]:
            sub = sdf[(sdf["fav_price"] >= lo) & (sdf["fav_price"] < hi) & sdf["team_b_is_underdog"]]
            if len(sub) < 5:
                continue
            stats = calc_pnl(sub, "team_b", "team_b_won", "price_b_open")
            if stats:
                stats["strategy"] = f"Team B dog | {sport.upper()} | fav@{label}"
                results.append(stats)
                P(f"  {stats['strategy']:<55} | {stats['bets']:>4} bets | WR: {stats['win_rate']:>5.1f}% | ROI: {stats['roi']:>+6.1f}%")

    # =====================================================================
    # STRATEGY 3: Team B FAVORITE (B is listed second AND is the favorite)
    # =====================================================================
    P("\n" + "=" * 70)
    P("  STRATEGY GROUP 3: TEAM B WHEN FAVORITE (home team advantage)")
    P("=" * 70)

    for sport in ["cbb", "nba", "epl", "ucl", "ALL"]:
        sdf = all_df if sport == "ALL" else all_df[all_df["sport"] == sport]
        for lo, hi, label in [
            (0.50, 0.55, "fav@50-55%"),
            (0.55, 0.65, "fav@55-65%"),
            (0.60, 0.75, "fav@60-75%"),
            (0.50, 0.60, "fav@50-60%"),
            (0.50, 0.75, "fav@50-75%"),
            (0.60, 0.80, "fav@60-80%"),
        ]:
            sub = sdf[(sdf["fav_price"] >= lo) & (sdf["fav_price"] < hi) & sdf["team_b_is_favorite"]]
            if len(sub) < 5:
                continue
            stats = calc_pnl(sub, "team_b", "team_b_won", "price_b_open")
            if stats:
                stats["strategy"] = f"Team B fav | {sport.upper()} | {label}"
                results.append(stats)
                P(f"  {stats['strategy']:<55} | {stats['bets']:>4} bets | WR: {stats['win_rate']:>5.1f}% | ROI: {stats['roi']:>+6.1f}%")

    # =====================================================================
    # STRATEGY 4: Heavy favorites (high win %, lower ROI)
    # =====================================================================
    P("\n" + "=" * 70)
    P("  STRATEGY GROUP 4: HEAVY FAVORITES")
    P("=" * 70)

    for sport in ["cbb", "nba", "epl", "ucl", "ALL"]:
        sdf = all_df if sport == "ALL" else all_df[all_df["sport"] == sport]
        for lo, hi, label in [
            (0.80, 0.90, "80-90%"),
            (0.85, 0.95, "85-95%"),
            (0.85, 0.90, "85-90%"),
            (0.90, 0.95, "90-95%"),
            (0.90, 1.01, "90%+"),
            (0.95, 1.01, "95%+"),
        ]:
            sub = sdf[(sdf["fav_price"] >= lo) & (sdf["fav_price"] < hi)]
            if len(sub) < 5:
                continue
            stats = calc_pnl(sub, "favorite", "fav_won", "fav_price")
            if stats:
                stats["strategy"] = f"Favorite | {sport.upper()} | {label}"
                results.append(stats)
                P(f"  {stats['strategy']:<55} | {stats['bets']:>4} bets | WR: {stats['win_rate']:>5.1f}% | ROI: {stats['roi']:>+6.1f}%")

    # =====================================================================
    # STRATEGY 5: Combo — Team B favorite in specific price ranges
    # =====================================================================
    P("\n" + "=" * 70)
    P("  STRATEGY GROUP 5: COMBO STRATEGIES")
    P("=" * 70)

    # 5a: Team B + favorite + 60-80%
    for sport in ["cbb", "nba", "epl", "ucl", "ALL"]:
        sdf = all_df if sport == "ALL" else all_df[all_df["sport"] == sport]

        # Team B is favorite and priced 60-80%
        for lo, hi, label in [
            (0.60, 0.80, "B fav 60-80%"),
            (0.65, 0.80, "B fav 65-80%"),
            (0.70, 0.85, "B fav 70-85%"),
            (0.75, 0.90, "B fav 75-90%"),
            (0.80, 0.95, "B fav 80-95%"),
        ]:
            sub = sdf[(sdf["price_b_open"] >= lo) & (sdf["price_b_open"] < hi)]
            if len(sub) < 5:
                continue
            stats = calc_pnl(sub, "team_b", "team_b_won", "price_b_open")
            if stats:
                stats["strategy"] = f"Bet B direct | {sport.upper()} | {label}"
                results.append(stats)
                P(f"  {stats['strategy']:<55} | {stats['bets']:>4} bets | WR: {stats['win_rate']:>5.1f}% | ROI: {stats['roi']:>+6.1f}%")

    # =====================================================================
    # STRATEGY 6: Fade Team A (bet against first-listed team)
    # =====================================================================
    P("\n" + "=" * 70)
    P("  STRATEGY GROUP 6: FADE TEAM A (always bet Team B regardless)")
    P("=" * 70)

    for sport in ["cbb", "nba", "epl", "ucl", "ALL"]:
        sdf = all_df if sport == "ALL" else all_df[all_df["sport"] == sport]
        for lo, hi, label in [
            (0.0, 1.01, "ALL markets"),
            (0.50, 0.65, "A fav 50-65%"),
            (0.60, 0.75, "A fav 60-75%"),
            (0.65, 0.80, "A fav 65-80%"),
            (0.75, 0.90, "A fav 75-90%"),
        ]:
            if lo == 0.0:
                sub = sdf.copy()
            else:
                sub = sdf[(sdf["price_a_open"] >= lo) & (sdf["price_a_open"] < hi)]
            if len(sub) < 5:
                continue
            stats = calc_pnl(sub, "team_b", "team_b_won", "price_b_open")
            if stats:
                stats["strategy"] = f"Fade A | {sport.upper()} | {label}"
                results.append(stats)
                P(f"  {stats['strategy']:<55} | {stats['bets']:>4} bets | WR: {stats['win_rate']:>5.1f}% | ROI: {stats['roi']:>+6.1f}%")

    # =====================================================================
    # STRATEGY 7: Contrarian — bet the opposite of what the market says
    # (when fav is barely favored, bet the dog)
    # =====================================================================
    P("\n" + "=" * 70)
    P("  STRATEGY GROUP 7: UNDERDOG IN CLOSE GAMES")
    P("=" * 70)

    for sport in ["cbb", "nba", "epl", "ucl", "ALL"]:
        sdf = all_df if sport == "ALL" else all_df[all_df["sport"] == sport]
        for lo, hi, label in [
            (0.50, 0.55, "fav 50-55%"),
            (0.55, 0.60, "fav 55-60%"),
            (0.50, 0.60, "fav 50-60%"),
            (0.60, 0.65, "fav 60-65%"),
            (0.55, 0.65, "fav 55-65%"),
        ]:
            sub = sdf[(sdf["fav_price"] >= lo) & (sdf["fav_price"] < hi)]
            if len(sub) < 5:
                continue
            stats = calc_pnl(sub, "underdog", "dog_won", "dog_price")
            if stats:
                stats["strategy"] = f"Bet dog | {sport.upper()} | {label}"
                results.append(stats)
                P(f"  {stats['strategy']:<55} | {stats['bets']:>4} bets | WR: {stats['win_rate']:>5.1f}% | ROI: {stats['roi']:>+6.1f}%")

    return results


def rank_strategies(results):
    """Rank all strategies by a combined score."""
    P("\n\n")
    P("=" * 90)
    P("  TOP STRATEGIES RANKED")
    P("=" * 90)

    df = pd.DataFrame(results)

    # Filter: need at least 10 bets for significance
    df = df[df["bets"] >= 10].copy()

    # Combined score: we want BOTH high win rate AND high ROI
    # Score = win_rate * 0.4 + roi * 0.6 (weighted toward profitability)
    # But also penalize tiny sample sizes
    df["sample_factor"] = np.minimum(df["bets"] / 50, 1.0)  # full credit at 50+ bets
    df["score"] = (df["win_rate"] * 0.4 + df["roi"] * 0.6) * df["sample_factor"]
    df = df.sort_values("score", ascending=False)

    P(f"\n  {'Strategy':<55} | {'Bets':>5} | {'WR%':>6} | {'ROI%':>7} | {'P&L':>10} | {'Score':>6}")
    P("  " + "-" * 100)

    for _, r in df.head(30).iterrows():
        P(f"  {r['strategy']:<55} | {r['bets']:>5} | {r['win_rate']:>5.1f}% | {r['roi']:>+6.1f}% | ${r['pnl']:>+9,.2f} | {r['score']:>6.1f}")

    # Also show: best by pure win rate (min 15 bets)
    P("\n\n  TOP 15 BY WIN RATE (min 15 bets):")
    P("  " + "-" * 100)
    wr_df = df[df["bets"] >= 15].sort_values("win_rate", ascending=False)
    for _, r in wr_df.head(15).iterrows():
        P(f"  {r['strategy']:<55} | {r['bets']:>5} | {r['win_rate']:>5.1f}% | {r['roi']:>+6.1f}% | ${r['pnl']:>+9,.2f}")

    # Best by pure ROI (min 15 bets)
    P("\n\n  TOP 15 BY ROI (min 15 bets):")
    P("  " + "-" * 100)
    roi_df = df[df["bets"] >= 15].sort_values("roi", ascending=False)
    for _, r in roi_df.head(15).iterrows():
        P(f"  {r['strategy']:<55} | {r['bets']:>5} | {r['win_rate']:>5.1f}% | {r['roi']:>+6.1f}% | ${r['pnl']:>+9,.2f}")

    # Best "sweet spot" — both WR > 60% AND ROI > 15%
    P("\n\n  SWEET SPOT: WR > 60% AND ROI > 15% (min 10 bets):")
    P("  " + "-" * 100)
    sweet = df[(df["win_rate"] > 60) & (df["roi"] > 15)].sort_values("score", ascending=False)
    if len(sweet):
        for _, r in sweet.iterrows():
            P(f"  {r['strategy']:<55} | {r['bets']:>5} | {r['win_rate']:>5.1f}% | {r['roi']:>+6.1f}% | ${r['pnl']:>+9,.2f}")
    else:
        P("  None found.")

    # Best "unicorn" — WR > 70% AND ROI > 20%
    P("\n\n  UNICORN: WR > 70% AND ROI > 20% (min 5 bets):")
    P("  " + "-" * 100)
    uni = df[(df["win_rate"] > 70) & (df["roi"] > 20)].sort_values("score", ascending=False)
    if len(uni):
        for _, r in uni.iterrows():
            P(f"  {r['strategy']:<55} | {r['bets']:>5} | {r['win_rate']:>5.1f}% | {r['roi']:>+6.1f}% | ${r['pnl']:>+9,.2f}")
    else:
        P("  None found.")

    # Save full results
    df.to_csv("strategy_optimizer_results.csv", index=False)
    P(f"\n  Full results saved to strategy_optimizer_results.csv ({len(df)} strategies)")


if __name__ == "__main__":
    P("=" * 70)
    P("  POLYMARKET STRATEGY OPTIMIZER")
    P("  Loading all cached price data...")
    P("=" * 70)
    P()

    all_df = load_all_data()
    if all_df is None:
        P("No data to analyze.")
    else:
        results = test_strategies(all_df)
        rank_strategies(results)

    LOG.close()
