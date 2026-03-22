"""
Kelly Criterion Bankroll Simulation
====================================
Compare flat bets vs Kelly-sized bets on Team B in coin-flip markets.
"""

import os
import pandas as pd
import numpy as np

os.environ["PYTHONUNBUFFERED"] = "1"
LOG = open("kelly_simulation_output.log", "w")

def P(msg=""):
    print(msg, flush=True)
    LOG.write(msg + "\n")
    LOG.flush()


def load_coin_flips():
    frames = []
    # EPL/UCL excluded: soccer markets are 3-way Yes/No, not real Team A vs B
    sport_wr = {"cbb": 0.616, "nba": 0.589}
    for sport, f in [("cbb", "db_cbb_prices.parquet"), ("nba", "db_nba_prices.parquet")]:
        if not os.path.exists(f):
            continue
        df = pd.read_parquet(f)
        df["sport"] = sport
        df["est_win_prob"] = sport_wr[sport]
        frames.append(df)

    all_df = pd.concat(frames, ignore_index=True)
    all_df["game_dt"] = pd.to_datetime(all_df["game_start_time"])
    all_df["fav_price"] = all_df[["price_a_open", "price_b_open"]].max(axis=1)
    all_df["team_b_won"] = all_df["winning_outcome"] == all_df["outcome_team_b"]

    # Filter to coin flips only
    coin = all_df[(all_df["fav_price"] >= 0.40) & (all_df["fav_price"] < 0.60)].copy()
    coin = coin.sort_values("game_dt").reset_index(drop=True)
    P(f"  Loaded {len(coin)} coin-flip markets across {coin['sport'].nunique()} sports")
    return coin


def simulate(coin, strategy_name, kelly_fraction=1.0, fixed_bet=None, pct_bet=None,
             use_adaptive=False, global_p=0.61):
    """Run a single simulation."""
    bankroll = 1000.0
    start_bank = bankroll
    peak = bankroll
    max_dd = 0
    max_dd_pct = 0
    n_bets = 0
    n_wins = 0
    n_skip = 0
    curve = [(0, bankroll)]
    pnl_list = []

    for _, row in coin.iterrows():
        pb = row["price_b_open"]
        won = row["team_b_won"]

        if bankroll <= 1.0:
            break

        if fixed_bet is not None:
            bet = min(fixed_bet, bankroll)
        elif pct_bet is not None:
            bet = bankroll * pct_bet
        else:
            # Kelly sizing
            p = row["est_win_prob"] if use_adaptive else global_p
            b = (1.0 / pb) - 1.0  # net odds
            if b <= 0:
                n_skip += 1
                continue
            f_star = (b * p - (1 - p)) / b
            if f_star <= 0:
                n_skip += 1
                continue
            f_star *= kelly_fraction
            f_star = min(f_star, 0.25)  # cap at 25%
            bet = bankroll * f_star

        bet = round(bet, 2)
        if bet < 0.01:
            n_skip += 1
            continue

        shares = bet / pb
        if won:
            pnl = shares - bet
            n_wins += 1
        else:
            pnl = -bet

        bankroll += pnl
        n_bets += 1
        pnl_list.append(pnl)

        if bankroll > peak:
            peak = bankroll
        dd = peak - bankroll
        dd_pct = dd / peak * 100 if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd
        if dd_pct > max_dd_pct:
            max_dd_pct = dd_pct

        if n_bets % 50 == 0:
            curve.append((n_bets, round(bankroll, 2)))

    curve.append((n_bets, round(bankroll, 2)))

    wr = n_wins / n_bets * 100 if n_bets else 0
    total_return = (bankroll - start_bank) / start_bank * 100
    avg_pnl = np.mean(pnl_list) if pnl_list else 0
    std_pnl = np.std(pnl_list) if len(pnl_list) > 1 else 1
    sharpe = avg_pnl / std_pnl * np.sqrt(len(pnl_list)) if std_pnl > 0 else 0

    return {
        "strategy": strategy_name,
        "bets": n_bets,
        "wins": n_wins,
        "losses": n_bets - n_wins,
        "win_rate": round(wr, 1),
        "start": start_bank,
        "final": round(bankroll, 2),
        "peak": round(peak, 2),
        "total_return": round(total_return, 1),
        "max_dd": round(max_dd, 2),
        "max_dd_pct": round(max_dd_pct, 1),
        "sharpe": round(sharpe, 2),
        "skipped": n_skip,
        "curve": curve,
    }


def run():
    P("=" * 75)
    P("  KELLY CRITERION BANKROLL SIMULATION")
    P("  Starting bankroll: $1,000 | Bet on Team B in coin-flip markets")
    P("=" * 75)
    P()

    coin = load_coin_flips()
    P()

    strategies = [
        ("Full Kelly (p=0.61)", dict(kelly_fraction=1.0, global_p=0.61)),
        ("Half Kelly (p=0.61)", dict(kelly_fraction=0.5, global_p=0.61)),
        ("Quarter Kelly (p=0.61)", dict(kelly_fraction=0.25, global_p=0.61)),
        ("Adaptive Kelly (sport-specific)", dict(kelly_fraction=0.5, use_adaptive=True)),
        ("Flat $50 bet", dict(fixed_bet=50)),
        ("5% of bankroll", dict(pct_bet=0.05)),
        ("10% of bankroll", dict(pct_bet=0.10)),
        ("2% of bankroll", dict(pct_bet=0.02)),
    ]

    results = []
    for name, params in strategies:
        r = simulate(coin, name, **params)
        results.append(r)

    # Summary table
    P("  STRATEGY COMPARISON:")
    P("  " + "-" * 110)
    P(f"  {'Strategy':<35} | {'Bets':>5} | {'WR%':>6} | {'Final $':>10} | {'Return':>8} | {'Peak $':>10} | {'MaxDD':>8} | {'DD%':>5} | {'Sharpe':>6}")
    P("  " + "-" * 110)

    for r in results:
        P(f"  {r['strategy']:<35} | {r['bets']:>5} | {r['win_rate']:>5.1f}% | ${r['final']:>9,.2f} | {r['total_return']:>+7.1f}% | ${r['peak']:>9,.2f} | ${r['max_dd']:>7,.2f} | {r['max_dd_pct']:>4.1f}% | {r['sharpe']:>6.2f}")

    P()

    # Equity curves
    P("  EQUITY CURVES (bankroll over time):")
    P("  " + "-" * 80)
    for r in results:
        P(f"\n  {r['strategy']}:")
        for bets, bank in r["curve"]:
            bar = "█" * max(1, int(bank / 100))
            P(f"    Bet {bets:>4}: ${bank:>10,.2f} {bar}")

    # Best strategy analysis
    P()
    P("=" * 75)
    P("  ANALYSIS")
    P("=" * 75)
    P()

    best_return = max(results, key=lambda x: x["total_return"])
    best_sharpe = max(results, key=lambda x: x["sharpe"])
    lowest_dd = min(results, key=lambda x: x["max_dd_pct"])

    P(f"  Best Return:      {best_return['strategy']} → ${best_return['final']:,.2f} ({best_return['total_return']:+.1f}%)")
    P(f"  Best Risk-Adj:    {best_sharpe['strategy']} → Sharpe {best_sharpe['sharpe']:.2f}")
    P(f"  Lowest Drawdown:  {lowest_dd['strategy']} → {lowest_dd['max_dd_pct']:.1f}% max DD")
    P()
    P(f"  Recommendation: Use {best_sharpe['strategy']} for best risk-adjusted returns")

    # Save
    df = pd.DataFrame([{k: v for k, v in r.items() if k != "curve"} for r in results])
    df.to_csv("kelly_simulation_results.csv", index=False)
    P(f"\n  Results saved to kelly_simulation_results.csv")


if __name__ == "__main__":
    run()
    LOG.close()
