"""
Crypto Score Bot Weight Optimizer
=================================
Analyzes all trades (executed + skipped) to find optimal signal weights.
- Normalizes all PnL to 1-contract equivalent
- Simulates skipped trades as if they were taken
- Tests signal weight combos and skip-list configurations
"""

import json
import sys
from itertools import product
from collections import defaultdict

DATA_FILE = "crypto_trade_data.json"

def load_data():
    with open(DATA_FILE) as f:
        bets = json.load(f)
    resolved = [b for b in bets if b.get("result") in ("win", "loss")]
    print(f"Loaded {len(bets)} total bets, {len(resolved)} resolved")
    traded = [b for b in resolved if b.get("action") == "trade"]
    skipped = [b for b in resolved if b.get("action") == "skip"]
    print(f"  Traded: {len(traded)}, Skipped (with hypothetical): {len(skipped)}")
    return resolved, traded, skipped


def calc_pnl_per_contract(bet):
    """PnL for 1 contract at the given price."""
    price = bet.get("price", 0.5)
    won = bet.get("result") == "win"
    if won:
        return (1.0 - price)  # win pays $1 - cost
    else:
        return -price  # lose the cost


def get_signal_count(bet):
    bd = bet.get("score_breakdown", {})
    total = 0
    for key, val in bd.items():
        if key == "Signal Score":
            continue
        pts = val.get("points", 0) if isinstance(val, dict) else 0
        total += pts
    return total


def get_individual_signals(bet):
    """Extract which signals fired (1) or didn't (0)."""
    bd = bet.get("score_breakdown", {})
    signals = {}
    for key, val in bd.items():
        if key == "Signal Score":
            continue
        pts = val.get("points", 0) if isinstance(val, dict) else 0
        signals[key] = pts
    return signals


def print_section(title):
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}")


def analyze_current_performance(all_resolved):
    """Analyze performance of current strategy."""
    print_section("CURRENT PERFORMANCE (ALL RESOLVED BETS)")

    traded = [b for b in all_resolved if b.get("action") == "trade"]
    skipped = [b for b in all_resolved if b.get("action") == "skip"]

    # Traded performance (normalized to 1 contract)
    wins = sum(1 for b in traded if b["result"] == "win")
    losses = len(traded) - wins
    pnl = sum(calc_pnl_per_contract(b) for b in traded)
    wr = wins / len(traded) * 100 if traded else 0

    print(f"\n  TRADED ({len(traded)} bets):")
    print(f"    Win rate: {wr:.1f}% ({wins}W / {losses}L)")
    print(f"    PnL (per contract): ${pnl:.2f}")
    print(f"    Avg PnL/trade: ${pnl/len(traded):.4f}" if traded else "")

    # Actual PnL considering contract sizes
    actual_pnl = 0
    for b in traded:
        contracts = b.get("contracts", 1)
        actual_pnl += calc_pnl_per_contract(b) * contracts
    print(f"    Actual PnL (with contract sizing): ${actual_pnl:.2f}")

    # Skipped hypothetical
    skip_wins = sum(1 for b in skipped if b.get("result") == "win" or b.get("would_have_won"))
    skip_pnl = sum(calc_pnl_per_contract(b) for b in skipped)
    skip_wr = skip_wins / len(skipped) * 100 if skipped else 0

    print(f"\n  SKIPPED ({len(skipped)} hypothetical bets):")
    print(f"    Would-be win rate: {skip_wr:.1f}%")
    print(f"    Would-be PnL (per contract): ${skip_pnl:.2f}")
    print(f"    Missed gains: ${max(0, skip_pnl):.2f}")

    # Combined if we traded everything
    all_wins = wins + skip_wins
    all_total = len(traded) + len(skipped)
    all_pnl = pnl + skip_pnl
    all_wr = all_wins / all_total * 100 if all_total else 0

    print(f"\n  IF WE TRADED EVERYTHING ({all_total} bets):")
    print(f"    Win rate: {all_wr:.1f}%")
    print(f"    PnL (per contract): ${all_pnl:.2f}")


def analyze_by_signal_count(all_resolved):
    """Break down performance by signal count."""
    print_section("PERFORMANCE BY SIGNAL COUNT")

    by_count = defaultdict(list)
    for b in all_resolved:
        sc = get_signal_count(b)
        by_count[sc].append(b)

    print(f"\n  {'Sig#':>4} | {'Total':>5} | {'Wins':>5} | {'WR%':>6} | {'PnL/contract':>13} | {'Avg PnL':>9} | {'Status':>10}")
    print(f"  {'-'*70}")

    for sc in sorted(by_count.keys()):
        bets = by_count[sc]
        wins = sum(1 for b in bets if b["result"] == "win")
        wr = wins / len(bets) * 100
        pnl = sum(calc_pnl_per_contract(b) for b in bets)
        avg = pnl / len(bets)
        traded_count = sum(1 for b in bets if b.get("action") == "trade")
        skipped_count = len(bets) - traded_count
        status = "TRADE" if traded_count > skipped_count else "SKIP"
        print(f"  {sc:>4} | {len(bets):>5} | {wins:>5} | {wr:>5.1f}% | ${pnl:>+11.2f} | ${avg:>+7.4f} | {status:>10}")

    return by_count


def analyze_by_crypto(all_resolved):
    """Break down by crypto."""
    print_section("PERFORMANCE BY CRYPTO")

    by_crypto = defaultdict(list)
    for b in all_resolved:
        by_crypto[b.get("crypto", "?")].append(b)

    print(f"\n  {'Crypto':>6} | {'Total':>5} | {'Traded':>6} | {'Wins':>5} | {'WR%':>6} | {'PnL/contract':>13} | {'Avg PnL':>9}")
    print(f"  {'-'*70}")

    for crypto in sorted(by_crypto.keys()):
        bets = by_crypto[crypto]
        traded = [b for b in bets if b.get("action") == "trade"]
        wins = sum(1 for b in bets if b["result"] == "win")
        wr = wins / len(bets) * 100
        pnl = sum(calc_pnl_per_contract(b) for b in bets)
        avg = pnl / len(bets)
        print(f"  {crypto:>6} | {len(bets):>5} | {len(traded):>6} | {wins:>5} | {wr:>5.1f}% | ${pnl:>+11.2f} | ${avg:>+7.4f}")


def analyze_by_price_range(all_resolved):
    """Break down by entry price."""
    print_section("PERFORMANCE BY ENTRY PRICE")

    ranges = [(0.50, 0.60), (0.60, 0.65), (0.65, 0.70), (0.70, 0.75),
              (0.75, 0.80), (0.80, 0.85), (0.85, 0.90), (0.90, 1.00)]

    print(f"\n  {'Price Range':>12} | {'Total':>5} | {'Wins':>5} | {'WR%':>6} | {'PnL/contract':>13} | {'Avg PnL':>9}")
    print(f"  {'-'*65}")

    for lo, hi in ranges:
        bets = [b for b in all_resolved if lo <= b.get("price", 0) < hi]
        if not bets:
            continue
        wins = sum(1 for b in bets if b["result"] == "win")
        wr = wins / len(bets) * 100
        pnl = sum(calc_pnl_per_contract(b) for b in bets)
        avg = pnl / len(bets)
        print(f"  {lo:.2f}-{hi:.2f}  | {len(bets):>5} | {wins:>5} | {wr:>5.1f}% | ${pnl:>+11.2f} | ${avg:>+7.4f}")


def analyze_individual_signals(all_resolved):
    """Analyze each signal's contribution to win rate."""
    print_section("INDIVIDUAL SIGNAL ANALYSIS")

    signal_names = set()
    for b in all_resolved:
        for key in b.get("score_breakdown", {}):
            if key != "Signal Score":
                signal_names.add(key)

    print(f"\n  {'Signal':>20} | {'When ON':>40} | {'When OFF':>40}")
    print(f"  {'':>20} | {'Bets':>5} {'WR%':>6} {'PnL':>10} {'Avg':>8} | {'Bets':>5} {'WR%':>6} {'PnL':>10} {'Avg':>8}")
    print(f"  {'-'*105}")

    signal_stats = {}
    for sig in sorted(signal_names):
        on_bets = [b for b in all_resolved if b.get("score_breakdown", {}).get(sig, {}).get("points", 0) > 0]
        off_bets = [b for b in all_resolved if b.get("score_breakdown", {}).get(sig, {}).get("points", 0) == 0]

        on_wins = sum(1 for b in on_bets if b["result"] == "win") if on_bets else 0
        on_wr = on_wins / len(on_bets) * 100 if on_bets else 0
        on_pnl = sum(calc_pnl_per_contract(b) for b in on_bets)
        on_avg = on_pnl / len(on_bets) if on_bets else 0

        off_wins = sum(1 for b in off_bets if b["result"] == "win") if off_bets else 0
        off_wr = off_wins / len(off_bets) * 100 if off_bets else 0
        off_pnl = sum(calc_pnl_per_contract(b) for b in off_bets)
        off_avg = off_pnl / len(off_bets) if off_bets else 0

        lift = on_wr - off_wr
        signal_stats[sig] = {"lift": lift, "on_wr": on_wr, "off_wr": off_wr, "on_avg": on_avg, "off_avg": off_avg}

        print(f"  {sig:>20} | {len(on_bets):>5} {on_wr:>5.1f}% ${on_pnl:>+9.2f} ${on_avg:>+7.4f} | {len(off_bets):>5} {off_wr:>5.1f}% ${off_pnl:>+9.2f} ${off_avg:>+7.4f}  lift: {lift:>+5.1f}%")

    print(f"\n  Signal ranking by WR lift (ON vs OFF):")
    for sig, stats in sorted(signal_stats.items(), key=lambda x: x[1]["lift"], reverse=True):
        emoji = "+" if stats["lift"] > 0 else "-"
        print(f"    {emoji} {sig:>20}: {stats['lift']:>+5.1f}% WR lift, avg PnL lift ${stats['on_avg'] - stats['off_avg']:>+.4f}")

    return signal_stats


def optimize_skip_list(all_resolved, by_count):
    """Find the optimal set of signal counts to skip."""
    print_section("SKIP LIST OPTIMIZATION")
    print("\n  Testing all possible skip-list combinations (signal counts 0-8)...")

    best_configs = []
    counts = sorted(by_count.keys())

    # Test all subsets of signal counts to TRADE (rest are skipped)
    for r in range(1, len(counts) + 1):
        from itertools import combinations
        for trade_set in combinations(counts, r):
            trade_set = set(trade_set)
            bets_in = [b for b in all_resolved if get_signal_count(b) in trade_set]
            if len(bets_in) < 20:
                continue
            wins = sum(1 for b in bets_in if b["result"] == "win")
            wr = wins / len(bets_in) * 100
            pnl = sum(calc_pnl_per_contract(b) for b in bets_in)
            avg = pnl / len(bets_in)
            skip_set = set(counts) - trade_set
            best_configs.append({
                "trade": sorted(trade_set),
                "skip": sorted(skip_set),
                "bets": len(bets_in),
                "wins": wins,
                "wr": wr,
                "pnl": pnl,
                "avg_pnl": avg,
            })

    # Sort by PnL
    best_configs.sort(key=lambda x: x["pnl"], reverse=True)

    print(f"\n  TOP 20 BY TOTAL PnL (per contract):")
    print(f"  {'Trade Signals':>25} | {'Skip':>20} | {'Bets':>5} | {'WR%':>6} | {'PnL':>10} | {'Avg':>8}")
    print(f"  {'-'*85}")
    for c in best_configs[:20]:
        print(f"  {str(c['trade']):>25} | {str(c['skip']):>20} | {c['bets']:>5} | {c['wr']:>5.1f}% | ${c['pnl']:>+9.2f} | ${c['avg_pnl']:>+7.4f}")

    # Sort by avg PnL per trade (efficiency)
    best_configs.sort(key=lambda x: x["avg_pnl"], reverse=True)

    print(f"\n  TOP 20 BY AVG PnL PER TRADE (most efficient):")
    print(f"  {'Trade Signals':>25} | {'Skip':>20} | {'Bets':>5} | {'WR%':>6} | {'PnL':>10} | {'Avg':>8}")
    print(f"  {'-'*85}")
    for c in best_configs[:20]:
        print(f"  {str(c['trade']):>25} | {str(c['skip']):>20} | {c['bets']:>5} | {c['wr']:>5.1f}% | ${c['pnl']:>+9.2f} | ${c['avg_pnl']:>+7.4f}")

    # Best balanced (min 100 trades, best avg)
    balanced = [c for c in best_configs if c["bets"] >= 100]
    balanced.sort(key=lambda x: x["avg_pnl"], reverse=True)

    print(f"\n  TOP 10 BALANCED (100+ trades, best avg PnL):")
    print(f"  {'Trade Signals':>25} | {'Skip':>20} | {'Bets':>5} | {'WR%':>6} | {'PnL':>10} | {'Avg':>8}")
    print(f"  {'-'*85}")
    for c in balanced[:10]:
        print(f"  {str(c['trade']):>25} | {str(c['skip']):>20} | {c['bets']:>5} | {c['wr']:>5.1f}% | ${c['pnl']:>+9.2f} | ${c['avg_pnl']:>+7.4f}")


def analyze_by_side(all_resolved):
    """Performance by YES vs NO side."""
    print_section("PERFORMANCE BY SIDE")

    for side in ["yes", "no"]:
        bets = [b for b in all_resolved if b.get("side") == side]
        if not bets:
            continue
        wins = sum(1 for b in bets if b["result"] == "win")
        wr = wins / len(bets) * 100
        pnl = sum(calc_pnl_per_contract(b) for b in bets)
        avg = pnl / len(bets)
        print(f"\n  {side.upper():>4}: {len(bets)} bets | WR: {wr:.1f}% | PnL: ${pnl:+.2f} | Avg: ${avg:+.4f}")


def analyze_by_entry_minute(all_resolved):
    """Performance by entry minute in window."""
    print_section("PERFORMANCE BY ENTRY MINUTE")

    ranges = [(0, 2), (2, 4), (4, 6), (6, 8), (8, 10), (10, 12), (12, 15)]

    print(f"\n  {'Minute':>10} | {'Total':>5} | {'Wins':>5} | {'WR%':>6} | {'PnL':>10} | {'Avg':>8}")
    print(f"  {'-'*55}")

    for lo, hi in ranges:
        bets = [b for b in all_resolved if lo <= b.get("entry_minute", 0) < hi]
        if not bets:
            continue
        wins = sum(1 for b in bets if b["result"] == "win")
        wr = wins / len(bets) * 100
        pnl = sum(calc_pnl_per_contract(b) for b in bets)
        avg = pnl / len(bets)
        print(f"  {lo:>2}-{hi:<2} min  | {len(bets):>5} | {wins:>5} | {wr:>5.1f}% | ${pnl:>+9.2f} | ${avg:>+7.4f}")


def analyze_dynamic_sizing_impact(all_resolved):
    """Simulate the dynamic sizing strategy on historical data."""
    print_section("DYNAMIC CONTRACT SIZING SIMULATION")

    traded = [b for b in all_resolved if b.get("action") == "trade"]
    traded.sort(key=lambda b: b.get("timestamp", ""))

    # Simulate: start at 1, scale to 5 on 9+/10 wins, back to 1 on 4+/10 losses
    contracts = 1
    total_pnl_flat = 0  # always 1 contract
    total_pnl_dynamic = 0  # with dynamic sizing
    scale_ups = 0
    scale_downs = 0
    history = []

    for i, b in enumerate(traded):
        pnl_1 = calc_pnl_per_contract(b)
        total_pnl_flat += pnl_1
        total_pnl_dynamic += pnl_1 * contracts
        history.append({"pnl": pnl_1, "contracts": contracts, "result": b["result"]})

        # Check last 10
        if len(history) >= 10:
            last_10 = history[-10:]
            wins = sum(1 for h in last_10 if h["result"] == "win")
            losses = 10 - wins
            if wins >= 9 and contracts == 1:
                contracts = 5
                scale_ups += 1
            elif losses >= 4 and contracts == 5:
                contracts = 1
                scale_downs += 1

    print(f"\n  Flat (1 contract):    ${total_pnl_flat:>+.2f} over {len(traded)} trades")
    print(f"  Dynamic (1↔5):       ${total_pnl_dynamic:>+.2f} over {len(traded)} trades")
    print(f"  Difference:          ${total_pnl_dynamic - total_pnl_flat:>+.2f}")
    print(f"  Scale-ups to 5:      {scale_ups} times")
    print(f"  Scale-downs to 1:    {scale_downs} times")

    if total_pnl_flat != 0:
        mult = total_pnl_dynamic / total_pnl_flat
        print(f"  Multiplier:          {mult:.2f}x")


def analyze_signal_combos(all_resolved):
    """Test which 2-signal and 3-signal combos are most predictive."""
    print_section("SIGNAL COMBINATION ANALYSIS")

    signal_names = set()
    for b in all_resolved:
        for key in b.get("score_breakdown", {}):
            if key != "Signal Score":
                signal_names.add(key)
    signal_names = sorted(signal_names)

    from itertools import combinations

    # 2-signal combos
    print(f"\n  Best 2-signal combos (both ON):")
    print(f"  {'Combo':>45} | {'Bets':>5} | {'WR%':>6} | {'Avg PnL':>9}")
    print(f"  {'-'*72}")

    combo_results = []
    for s1, s2 in combinations(signal_names, 2):
        bets = [b for b in all_resolved
                if b.get("score_breakdown", {}).get(s1, {}).get("points", 0) > 0
                and b.get("score_breakdown", {}).get(s2, {}).get("points", 0) > 0]
        if len(bets) < 15:
            continue
        wins = sum(1 for b in bets if b["result"] == "win")
        wr = wins / len(bets) * 100
        pnl = sum(calc_pnl_per_contract(b) for b in bets)
        avg = pnl / len(bets)
        combo_results.append((f"{s1} + {s2}", len(bets), wr, avg))

    combo_results.sort(key=lambda x: x[3], reverse=True)
    for name, n, wr, avg in combo_results[:15]:
        print(f"  {name:>45} | {n:>5} | {wr:>5.1f}% | ${avg:>+7.4f}")

    # 3-signal combos
    print(f"\n  Best 3-signal combos (all ON):")
    print(f"  {'Combo':>55} | {'Bets':>5} | {'WR%':>6} | {'Avg PnL':>9}")
    print(f"  {'-'*82}")

    combo3_results = []
    for s1, s2, s3 in combinations(signal_names, 3):
        bets = [b for b in all_resolved
                if b.get("score_breakdown", {}).get(s1, {}).get("points", 0) > 0
                and b.get("score_breakdown", {}).get(s2, {}).get("points", 0) > 0
                and b.get("score_breakdown", {}).get(s3, {}).get("points", 0) > 0]
        if len(bets) < 10:
            continue
        wins = sum(1 for b in bets if b["result"] == "win")
        wr = wins / len(bets) * 100
        pnl = sum(calc_pnl_per_contract(b) for b in bets)
        avg = pnl / len(bets)
        combo3_results.append((f"{s1} + {s2} + {s3}", len(bets), wr, avg))

    combo3_results.sort(key=lambda x: x[3], reverse=True)
    for name, n, wr, avg in combo3_results[:15]:
        print(f"  {name:>55} | {n:>5} | {wr:>5.1f}% | ${avg:>+7.4f}")


def analyze_crypto_signal_cross(all_resolved):
    """Cross-analyze: which signals work best for which crypto."""
    print_section("CRYPTO x SIGNAL CROSS ANALYSIS")

    cryptos = sorted(set(b.get("crypto", "?") for b in all_resolved))
    signal_names = set()
    for b in all_resolved:
        for key in b.get("score_breakdown", {}):
            if key != "Signal Score":
                signal_names.add(key)
    signal_names = sorted(signal_names)

    print(f"\n  WR lift when signal ON (vs OFF) by crypto:")
    print(f"  {'Signal':>20}", end="")
    for c in cryptos:
        print(f" | {c:>6}", end="")
    print()
    print(f"  {'-' * (22 + 9 * len(cryptos))}")

    for sig in signal_names:
        print(f"  {sig:>20}", end="")
        for crypto in cryptos:
            crypto_bets = [b for b in all_resolved if b.get("crypto") == crypto]
            on = [b for b in crypto_bets if b.get("score_breakdown", {}).get(sig, {}).get("points", 0) > 0]
            off = [b for b in crypto_bets if b.get("score_breakdown", {}).get(sig, {}).get("points", 0) == 0]
            if len(on) < 5 or len(off) < 5:
                print(f" | {'N/A':>6}", end="")
                continue
            on_wr = sum(1 for b in on if b["result"] == "win") / len(on) * 100
            off_wr = sum(1 for b in off if b["result"] == "win") / len(off) * 100
            lift = on_wr - off_wr
            print(f" | {lift:>+5.1f}%", end="")
        print()


if __name__ == "__main__":
    print("=" * 80)
    print("  CRYPTO SCORE BOT — WEIGHT OPTIMIZER")
    print(f"  Data: {DATA_FILE}")
    print("=" * 80)

    all_resolved, traded, skipped = load_data()

    analyze_current_performance(all_resolved)
    by_count = analyze_by_signal_count(all_resolved)
    analyze_by_crypto(all_resolved)
    analyze_by_side(all_resolved)
    analyze_by_price_range(all_resolved)
    analyze_by_entry_minute(all_resolved)
    analyze_individual_signals(all_resolved)
    analyze_signal_combos(all_resolved)
    analyze_crypto_signal_cross(all_resolved)
    optimize_skip_list(all_resolved, by_count)
    analyze_dynamic_sizing_impact(all_resolved)

    print(f"\n{'=' * 80}")
    print("  OPTIMIZATION COMPLETE")
    print(f"{'=' * 80}")
