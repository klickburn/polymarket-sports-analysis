"""
Kalshi Dashboard Refresh
========================
Fetches Kalshi bot data and rebuilds the Kalshi dashboard HTML.

Usage:
    python3 refresh_kalshi_dashboard.py
"""

import json
import os
import sys
from datetime import datetime, timezone


def P(msg=""):
    print(msg, flush=True)


P("  [1/3] Loading Kalshi bot data...")

# Load bot bets
bets_file = "kalshi_bets.json"
bot_bets = []
if os.path.exists(bets_file):
    with open(bets_file) as f:
        bot_bets = json.load(f)

# Load bot status
status_file = "kalshi_bot_status.json"
bot_status = {}
if os.path.exists(status_file):
    with open(status_file) as f:
        bot_status = json.load(f)

# Try to fetch balance from Kalshi API
balance_info = {}
KALSHI_KEY_ID = os.environ.get("KALSHI_KEY_ID", "")
KALSHI_PRIVATE_KEY = os.environ.get("KALSHI_PRIVATE_KEY", "")

if KALSHI_KEY_ID and KALSHI_PRIVATE_KEY:
    try:
        from kalshi_bot import get_balance, get_existing_positions, auth_get
        bal = get_balance()
        if bal:
            balance_info = bal

        # Try to get settlement info for placed bets
        P("  Checking bet outcomes...")
        positions = get_existing_positions()
        for bet in bot_bets:
            ticker = bet.get("ticker", "")
            if not ticker:
                continue
            # Check if market has settled
            if "result" not in bet or bet.get("result") == "pending":
                try:
                    from kalshi_bot import public_get
                    mkt = public_get(f"/markets/{ticker}")
                    market = mkt.get("market", {})
                    status = market.get("status", "")
                    result_val = market.get("result", "")
                    if status in ("settled", "finalized") and result_val:
                        won = (result_val == "yes" and bet.get("side") == "yes") or \
                              (result_val == "no" and bet.get("side") == "no")
                        bet["result"] = "win" if won else "loss"
                        bet["market_result"] = result_val
                        price = bet.get("price", 0)
                        amount = bet.get("bet_amount", 0)
                        contracts = int(amount / price) if price > 0 else 0
                        if won:
                            bet["pnl"] = round(contracts * (1.0 - price), 2)
                        else:
                            bet["pnl"] = round(-contracts * price, 2)
                    elif ticker in positions:
                        bet["result"] = "open"
                    elif status == "open":
                        bet["result"] = "open"
                    else:
                        bet["result"] = "pending"
                except Exception:
                    pass

        # Save updated bets with results
        with open(bets_file, "w") as f:
            json.dump(bot_bets, f, indent=2, default=str)

    except Exception as e:
        P(f"  WARNING: Could not fetch Kalshi data: {e}")

P(f"  Loaded {len(bot_bets)} bets")

# Step 2: Build report
P("  [2/3] Building report...")

resolved = [b for b in bot_bets if b.get("result") in ("win", "loss")]
open_bets = [b for b in bot_bets if b.get("result") == "open"]
pending = [b for b in bot_bets if b.get("result") not in ("win", "loss", "open")]

wins = [b for b in resolved if b["result"] == "win"]
losses = [b for b in resolved if b["result"] == "loss"]
total_pnl = sum(b.get("pnl", 0) for b in resolved)
total_wagered = sum(b.get("bet_amount", 0) for b in bot_bets)
open_cost = sum(b.get("bet_amount", 0) for b in open_bets)

report = {
    "total_bets": len(bot_bets),
    "resolved": len(resolved),
    "wins": len(wins),
    "losses": len(losses),
    "win_rate": round(len(wins) / len(resolved) * 100, 1) if resolved else 0,
    "total_pnl": round(total_pnl, 2),
    "total_wagered": round(total_wagered, 2),
    "open_count": len(open_bets),
    "open_cost": round(open_cost, 2),
    "pending_count": len(pending),
    "balance": balance_info.get("balance", 0),
    "portfolio_value": balance_info.get("portfolio_value", 0),
    "bets": bot_bets,
}

# Step 3: Build dashboard
P("  [3/3] Building Kalshi dashboard...")

template_file = "kalshi_dashboard_template.html"
if not os.path.exists(template_file):
    P(f"  No {template_file} found.")
    sys.exit(1)

with open(template_file) as f:
    html = f.read()

refresh_time = datetime.now(timezone.utc).isoformat()

html = html.replace("KALSHI_REPORT_PLACEHOLDER", json.dumps(report, default=str))
html = html.replace("KALSHI_STATUS_PLACEHOLDER", json.dumps(bot_status, default=str))
html = html.replace("DATA_REFRESHED_PLACEHOLDER", refresh_time)

with open("kalshi_dashboard.html", "w") as f:
    f.write(html)

P()
P(f"  Kalshi Dashboard refreshed!")
P(f"  Total bets: {len(bot_bets)}")
P(f"  Resolved: {len(resolved)} ({len(wins)}W-{len(losses)}L)")
P(f"  P&L: ${total_pnl:+.2f}")
P(f"  Open: {len(open_bets)} | Pending: {len(pending)}")
P(f"  -> Open kalshi_dashboard.html to view")
