#!/usr/bin/env python3
"""
Polymarket 15-Minute Crypto Whale Finder

Finds the most profitable traders on Polymarket's 15-minute crypto prediction
markets (BTC, ETH, SOL, XRP up/down).

Data sources:
  - Gamma API:  https://gamma-api.polymarket.com  (market/event data)
  - Data API:   https://data-api.polymarket.com    (trade history, no auth needed)

Usage:
  python3 polymarket_whales.py [--events N] [--recent-hours H]
"""

import argparse
import json
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone

import requests

# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

GAMMA_BASE = "https://gamma-api.polymarket.com"
DATA_BASE = "https://data-api.polymarket.com"

SESSION = requests.Session()
SESSION.headers.update({"Accept": "application/json"})


def gamma_get(path, params=None):
    """GET request to the Gamma API with basic retry."""
    url = f"{GAMMA_BASE}{path}"
    for attempt in range(3):
        try:
            r = SESSION.get(url, params=params, timeout=20)
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, json.JSONDecodeError) as exc:
            if attempt == 2:
                print(f"  [warn] Gamma API error: {exc}")
                return []
            time.sleep(1)
    return []


def data_get(path, params=None):
    """GET request to the Data API with basic retry."""
    url = f"{DATA_BASE}{path}"
    for attempt in range(3):
        try:
            r = SESSION.get(url, params=params, timeout=20)
            r.raise_for_status()
            return r.json()
        except (requests.RequestException, json.JSONDecodeError) as exc:
            if attempt == 2:
                print(f"  [warn] Data API error: {exc}")
                return []
            time.sleep(1)
    return []


# ---------------------------------------------------------------------------
# Step 1 - Discover 15-minute crypto events
# ---------------------------------------------------------------------------

def fetch_15m_events(max_events=500, recent_hours=None):
    """
    Fetch recently-settled 15-minute crypto events from the Gamma API.

    The tag_slug=15m filter returns BTC/ETH/SOL/XRP up-or-down 15-minute
    markets.  We only care about *closed* (settled) events so we can
    determine winners and losers.
    """
    events = []
    offset = 0
    batch = 100

    # If user wants only recent events, compute a cutoff timestamp
    cutoff_iso = None
    if recent_hours:
        cutoff_ts = time.time() - recent_hours * 3600
        cutoff_iso = datetime.fromtimestamp(cutoff_ts, tz=timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

    print(f"[1/4] Fetching settled 15-minute crypto events (max {max_events})...")

    while len(events) < max_events:
        params = {
            "limit": batch,
            "offset": offset,
            "tag_slug": "15m",
            "closed": "true",
            "order": "endDate",
            "ascending": "false",
        }
        if cutoff_iso:
            params["end_date_min"] = cutoff_iso

        data = gamma_get("/events", params)
        if not data:
            break

        for ev in data:
            slug = (ev.get("slug") or "").lower()
            # Ensure it is actually a 15m crypto up/down market
            if "15m" not in slug:
                continue
            # Determine crypto
            crypto = None
            for sym in ["btc", "eth", "sol", "xrp"]:
                if sym in slug:
                    crypto = sym.upper()
                    break
            if not crypto:
                continue

            markets = ev.get("markets") or []
            if not markets:
                continue

            mkt = markets[0]
            outcomes = json.loads(mkt.get("outcomes") or "[]")
            outcome_prices = json.loads(mkt.get("outcomePrices") or "[]")

            # Determine winning outcome index (price == "1")
            winning_idx = None
            for i, p in enumerate(outcome_prices):
                if str(p) == "1":
                    winning_idx = i
                    break

            if winning_idx is None:
                continue  # not yet resolved

            events.append({
                "event_id": ev["id"],
                "title": ev.get("title", ""),
                "slug": slug,
                "crypto": crypto,
                "condition_id": mkt.get("conditionId", ""),
                "outcomes": outcomes,
                "winning_idx": winning_idx,
                "winning_outcome": outcomes[winning_idx] if winning_idx < len(outcomes) else "?",
                "volume": float(ev.get("volume") or 0),
            })

        if len(data) < batch:
            break
        offset += batch

    print(f"  Found {len(events)} settled events across "
          f"{len(set(e['crypto'] for e in events))} cryptos")
    return events


# ---------------------------------------------------------------------------
# Step 2 - Fetch trades for each event
# ---------------------------------------------------------------------------

def fetch_trades_for_event(event_id, max_trades=5000):
    """
    Fetch trades from the Data API for a given event.
    Returns a list of trade dicts.
    """
    trades = []
    offset = 0
    batch = 1000

    while len(trades) < max_trades:
        params = {
            "eventId": event_id,
            "limit": batch,
            "offset": offset,
        }
        data = data_get("/trades", params)
        if not data:
            break
        trades.extend(data)
        if len(data) < batch:
            break
        offset += batch

    return trades


def fetch_all_trades(events, progress_interval=25):
    """Fetch trades for all events, returning (event, trades) pairs."""
    print(f"\n[2/4] Fetching trades for {len(events)} events...")
    all_event_trades = []
    for i, ev in enumerate(events):
        trades = fetch_trades_for_event(ev["event_id"])
        all_event_trades.append((ev, trades))
        if (i + 1) % progress_interval == 0 or (i + 1) == len(events):
            total_trades = sum(len(t) for _, t in all_event_trades)
            print(f"  {i+1}/{len(events)} events fetched — {total_trades:,} trades so far")
    return all_event_trades


# ---------------------------------------------------------------------------
# Step 3 - Calculate per-wallet P&L
# ---------------------------------------------------------------------------

def compute_wallet_stats(all_event_trades):
    """
    For each wallet, compute:
      - total_volume (USDC spent)
      - trade_count
      - wins / losses (events where net position was correct)
      - estimated P&L
      - set of cryptos traded

    P&L logic for a settled binary market:
      Winning outcome resolves to $1, losing to $0.

      BUY winning-outcome shares at price p  ->  profit = size * (1 - p)
      BUY losing-outcome shares at price p   ->  loss   = size * p
      SELL winning-outcome shares at price p  ->  loss   = size * (1 - p)
      SELL losing-outcome shares at price p   ->  profit = size * p

    Because these are short-duration markets, most participants hold to
    settlement, so this approximation is reasonable.
    """
    print(f"\n[3/4] Computing per-wallet statistics...")

    # wallet -> stats
    wallets = defaultdict(lambda: {
        "pnl": 0.0,
        "volume": 0.0,
        "trade_count": 0,
        "wins": 0,
        "losses": 0,
        "cryptos": set(),
        "pseudonym": "",
        "events_traded": set(),
    })

    for ev, trades in all_event_trades:
        winning_idx = ev["winning_idx"]
        crypto = ev["crypto"]
        event_id = ev["event_id"]

        # Track per-wallet net P&L within this event
        event_wallet_pnl = defaultdict(float)

        for t in trades:
            wallet = t.get("proxyWallet", "").lower()
            if not wallet:
                continue

            side = t.get("side", "")        # BUY or SELL
            size = float(t.get("size", 0))  # number of shares
            price = float(t.get("price", 0))
            outcome_idx = t.get("outcomeIndex", -1)
            pseudonym = t.get("pseudonym", "")

            w = wallets[wallet]
            w["trade_count"] += 1
            w["volume"] += size * price
            w["cryptos"].add(crypto)
            w["events_traded"].add(event_id)
            if pseudonym and not w["pseudonym"]:
                w["pseudonym"] = pseudonym

            # Calculate P&L contribution
            is_winning_outcome = (outcome_idx == winning_idx)

            if side == "BUY":
                if is_winning_outcome:
                    pnl = size * (1.0 - price)   # bought winner cheap
                else:
                    pnl = -(size * price)          # bought loser
            elif side == "SELL":
                if is_winning_outcome:
                    pnl = -(size * (1.0 - price))  # sold winner
                else:
                    pnl = size * price              # sold loser (collected premium)
            else:
                pnl = 0.0

            w["pnl"] += pnl
            event_wallet_pnl[wallet] += pnl

        # Tally wins/losses per event per wallet
        for wallet, epnl in event_wallet_pnl.items():
            if epnl > 0.01:
                wallets[wallet]["wins"] += 1
            elif epnl < -0.01:
                wallets[wallet]["losses"] += 1

    print(f"  Found {len(wallets):,} unique wallets")
    return wallets


# ---------------------------------------------------------------------------
# Step 4 - Rank and display
# ---------------------------------------------------------------------------

def rank_and_display(wallets, top_n=30):
    """Sort wallets by P&L and print a table."""
    print(f"\n[4/4] Ranking top {top_n} wallets by estimated P&L...\n")

    # Convert to list for sorting
    ranked = []
    for addr, stats in wallets.items():
        total_events = stats["wins"] + stats["losses"]
        win_rate = (stats["wins"] / total_events * 100) if total_events > 0 else 0.0
        ranked.append({
            "wallet": addr,
            "pseudonym": stats["pseudonym"],
            "pnl": stats["pnl"],
            "win_rate": win_rate,
            "wins": stats["wins"],
            "losses": stats["losses"],
            "trade_count": stats["trade_count"],
            "volume": stats["volume"],
            "cryptos": sorted(stats["cryptos"]),
            "events_traded": len(stats["events_traded"]),
        })

    # Sort by P&L descending
    ranked.sort(key=lambda x: x["pnl"], reverse=True)
    top = ranked[:top_n]

    # Print table
    header = (
        f"{'#':>3}  {'Wallet':17} {'Pseudonym':22} {'P&L ($)':>12} "
        f"{'Win%':>6} {'W/L':>7} {'Trades':>7} {'Volume ($)':>12} {'Cryptos':>12}"
    )
    print("=" * len(header))
    print("  POLYMARKET 15-MINUTE CRYPTO — TOP TRADERS (by estimated P&L)")
    print("=" * len(header))
    print(header)
    print("-" * len(header))

    for i, w in enumerate(top, 1):
        addr_short = w["wallet"][:6] + "..." + w["wallet"][-4:]
        pseudo = (w["pseudonym"][:20] + "..") if len(w["pseudonym"]) > 20 else w["pseudonym"]
        cryptos_str = ",".join(w["cryptos"])
        wl = f"{w['wins']}/{w['losses']}"
        print(
            f"{i:>3}  {addr_short:17} {pseudo:22} {w['pnl']:>12,.2f} "
            f"{w['win_rate']:>5.1f}% {wl:>7} {w['trade_count']:>7,} "
            f"{w['volume']:>12,.2f} {cryptos_str:>12}"
        )

    print("-" * len(header))
    print()
    return ranked


def save_results(ranked, filename="polymarket_whales.json"):
    """Save full ranked results to JSON."""
    # Convert sets to lists for JSON serialization
    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "description": "Top traders on Polymarket 15-minute crypto prediction markets",
        "markets": "BTC, ETH, SOL, XRP — Up or Down (15-minute windows)",
        "total_wallets_analyzed": len(ranked),
        "top_traders": ranked[:100],
    }

    filepath = f"/Users/klickburn/.claude/projects/-Users-klickburn-client-db/polymarket-sports-analysis/{filename}"
    with open(filepath, "w") as f:
        json.dump(output, f, indent=2, default=str)
    print(f"Results saved to {filepath}")
    return filepath


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Find the most profitable traders on Polymarket 15-minute crypto markets"
    )
    parser.add_argument(
        "--events", type=int, default=200,
        help="Maximum number of settled events to analyze (default: 200)"
    )
    parser.add_argument(
        "--recent-hours", type=int, default=None,
        help="Only look at events from the last N hours (default: all available)"
    )
    parser.add_argument(
        "--top", type=int, default=30,
        help="Number of top traders to display (default: 30)"
    )
    args = parser.parse_args()

    print("=" * 70)
    print("  POLYMARKET 15-MINUTE CRYPTO WHALE FINDER")
    print("  https://polymarket.com/crypto/15M")
    print("=" * 70)
    print()

    # Step 1: Find settled 15-minute crypto events
    events = fetch_15m_events(
        max_events=args.events,
        recent_hours=args.recent_hours,
    )
    if not events:
        print("No settled events found. Try increasing --events or removing --recent-hours.")
        sys.exit(1)

    # Show crypto breakdown
    crypto_counts = defaultdict(int)
    for ev in events:
        crypto_counts[ev["crypto"]] += 1
    print("  Breakdown:", dict(crypto_counts))

    # Step 2: Fetch trades for all events
    all_event_trades = fetch_all_trades(events)

    total_trades = sum(len(t) for _, t in all_event_trades)
    print(f"  Total trades fetched: {total_trades:,}")

    if total_trades == 0:
        print("No trades found. The API may be rate-limiting or events have no trades.")
        sys.exit(1)

    # Step 3: Compute per-wallet stats
    wallets = compute_wallet_stats(all_event_trades)

    # Step 4: Rank and display
    ranked = rank_and_display(wallets, top_n=args.top)

    # Save to JSON
    save_results(ranked)

    # Summary stats
    profitable = sum(1 for w in ranked if w["pnl"] > 0)
    total_volume = sum(w["volume"] for w in ranked)
    print(f"\nSummary:")
    print(f"  Events analyzed:    {len(events)}")
    print(f"  Total trades:       {total_trades:,}")
    print(f"  Unique wallets:     {len(ranked):,}")
    print(f"  Profitable wallets: {profitable:,} ({profitable/len(ranked)*100:.1f}%)")
    print(f"  Total volume:       ${total_volume:,.2f}")


if __name__ == "__main__":
    main()
