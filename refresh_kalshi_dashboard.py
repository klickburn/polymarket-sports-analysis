"""
Kalshi Dashboard Refresh
========================
Fetches Kalshi bot data and rebuilds the Kalshi dashboard HTML.
Crypto bets are fetched directly from the Kalshi API (orders + market status).

Usage:
    python3 refresh_kalshi_dashboard.py
"""

import json
import os
import sys
import time
from datetime import datetime, timezone


def P(msg=""):
    print(msg, flush=True)


# Crypto series prefixes — used to identify crypto 15m bot orders
CRYPTO_SERIES = {
    "KXBTC15M": "BTC",
    "KXETH15M": "ETH",
    "KXSOL15M": "SOL",
    "KXXRP15M": "XRP",
    "KXDOGE15M": "DOGE",
}


P("  [1/4] Loading Kalshi bot data...")

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
        from kalshi_bot import get_balance, get_existing_positions, auth_get, public_get
        bal = get_balance()
        if bal:
            balance_info = bal

        # Try to get settlement info for placed bets
        P("  Checking sports bet outcomes...")
        positions = get_existing_positions()
        for bet in bot_bets:
            ticker = bet.get("ticker", "")
            if not ticker:
                continue
            # Check if market has settled
            if "result" not in bet or bet.get("result") == "pending":
                try:
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

P(f"  Loaded {len(bot_bets)} sports bets")

# ── Fetch crypto bets directly from Kalshi API ──────────────────────────
crypto_bets = []
crypto_status = {}

crypto_status_file = "crypto_15m_status.json"
if os.path.exists(crypto_status_file):
    with open(crypto_status_file) as f:
        crypto_status = json.load(f)

if KALSHI_KEY_ID and KALSHI_PRIVATE_KEY:
    try:
        from kalshi_bot import auth_get, public_get
        P("  [2/4] Fetching crypto orders from Kalshi API...")

        # Fetch all filled orders, paginating through results
        all_fills = []
        cursor = None
        while True:
            params = {"limit": 200}
            if cursor:
                params["cursor"] = cursor
            time.sleep(0.3)
            data = auth_get("/portfolio/fills", params=params)
            fills = data.get("fills", [])
            all_fills.extend(fills)
            cursor = data.get("cursor")
            if not cursor or not fills:
                break

        P(f"  Fetched {len(all_fills)} total fills from Kalshi")

        # Filter to crypto 15m fills only and group by ticker
        crypto_fills_by_ticker = {}
        for fill in all_fills:
            ticker = fill.get("ticker", "")
            # Match any crypto 15m series prefix
            matched_crypto = None
            for prefix, crypto_name in CRYPTO_SERIES.items():
                if ticker.startswith(prefix):
                    matched_crypto = crypto_name
                    break
            if not matched_crypto:
                continue

            if ticker not in crypto_fills_by_ticker:
                crypto_fills_by_ticker[ticker] = {
                    "ticker": ticker,
                    "crypto": matched_crypto,
                    "side": fill.get("side", ""),
                    "fills": [],
                    "total_count": 0,
                    "total_cost_dollars": 0,
                    "timestamp": fill.get("created_time", ""),
                }
            entry = crypto_fills_by_ticker[ticker]
            entry["fills"].append(fill)
            count = int(float(fill.get("count_fp", fill.get("count", 0))))
            # Prices are dollar strings like "0.8900"
            if entry["side"] == "yes":
                price = float(fill.get("yes_price_dollars", fill.get("yes_price_fixed", 0)))
            else:
                price = float(fill.get("no_price_dollars", fill.get("no_price_fixed", 0)))
            entry["total_count"] += count
            entry["total_cost_dollars"] += count * price
            # Use earliest fill time as timestamp
            fill_time = fill.get("created_time", "")
            if fill_time and (not entry["timestamp"] or fill_time < entry["timestamp"]):
                entry["timestamp"] = fill_time

        P(f"  Found {len(crypto_fills_by_ticker)} crypto 15m positions")

        # Build crypto bets list and check market outcomes
        for ticker, entry in sorted(crypto_fills_by_ticker.items(), key=lambda x: x[1]["timestamp"]):
            avg_price = entry["total_cost_dollars"] / entry["total_count"] if entry["total_count"] else 0
            bet = {
                "ticker": ticker,
                "crypto": entry["crypto"],
                "side": entry["side"],
                "price": round(avg_price, 4),
                "bet_amount": round(entry["total_cost_dollars"], 2),
                "contracts": entry["total_count"],
                "timestamp": entry["timestamp"],
                "result": "open",
            }

            # Check market outcome
            try:
                time.sleep(0.2)
                mkt = public_get(f"/markets/{ticker}")
                market = mkt.get("market", {})
                status = market.get("status", "")
                result_val = market.get("result", "")
                if status in ("settled", "finalized") and result_val:
                    won = (result_val == "yes" and bet["side"] == "yes") or \
                          (result_val == "no" and bet["side"] == "no")
                    bet["result"] = "win" if won else "loss"
                    bet["market_result"] = result_val
                    if won:
                        bet["pnl"] = round(entry["total_count"] * (1.0 - avg_price), 2)
                    else:
                        bet["pnl"] = round(-entry["total_count"] * avg_price, 2)
                elif status == "open":
                    bet["result"] = "open"
                else:
                    bet["result"] = "pending"
            except Exception:
                pass

            crypto_bets.append(bet)

        P(f"  Crypto: {len(crypto_bets)} bets fetched from API")

    except Exception as e:
        P(f"  WARNING: Could not fetch crypto data from API: {e}")
        import traceback
        traceback.print_exc()
else:
    P("  [2/4] No Kalshi API keys — skipping crypto fetch")

# Step 3: Build reports
P("  [3/4] Building reports...")


def build_report(bets):
    resolved = [b for b in bets if b.get("result") in ("win", "loss")]
    open_bets = [b for b in bets if b.get("result") == "open"]
    pending = [b for b in bets if b.get("result") not in ("win", "loss", "open")]
    wins = [b for b in resolved if b["result"] == "win"]
    losses = [b for b in resolved if b["result"] == "loss"]
    total_pnl = sum(b.get("pnl", 0) for b in resolved)
    total_wagered = sum(b.get("bet_amount", 0) for b in bets)
    open_cost = sum(b.get("bet_amount", 0) for b in open_bets)
    return {
        "total_bets": len(bets),
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
        "bets": bets,
    }


# Load whale follow data
whale_bets_file = "whale_trades.json"
whale_bets = []
if os.path.exists(whale_bets_file):
    with open(whale_bets_file) as f:
        whale_bets = json.load(f)
P(f"  Loaded {len(whale_bets)} whale follow trades")

whale_status_file = "whale_status.json"
whale_status = {}
if os.path.exists(whale_status_file):
    with open(whale_status_file) as f:
        whale_status = json.load(f)

# Check whale bet outcomes via Kalshi API
if KALSHI_KEY_ID and KALSHI_PRIVATE_KEY and whale_bets:
    try:
        from kalshi_bot import public_get
        P("  Checking whale bet outcomes...")
        for bet in whale_bets:
            ticker = bet.get("kalshi_ticker", "")
            if not ticker or bet.get("kalshi_action") != "buy":
                continue
            if bet.get("result") in ("win", "loss"):
                continue
            try:
                time.sleep(0.15)
                mkt = public_get(f"/markets/{ticker}")
                market = mkt.get("market", {})
                status = market.get("status", "")
                result_val = market.get("result", "")
                kalshi_side = bet.get("kalshi_side", "")
                kalshi_price = bet.get("kalshi_price", 0)
                if status in ("settled", "finalized") and result_val:
                    won = (result_val == "yes" and kalshi_side == "yes") or \
                          (result_val == "no" and kalshi_side == "no")
                    bet["result"] = "win" if won else "loss"
                    bet["market_result"] = result_val
                    contracts = max(1, int(0.50 / (kalshi_price + 0.02))) if kalshi_price else 1
                    if won:
                        bet["pnl"] = round(contracts * (1.0 - kalshi_price), 2)
                    else:
                        bet["pnl"] = round(-contracts * kalshi_price, 2)
                    bet["bet_amount"] = round(contracts * kalshi_price, 2)
                elif status == "open":
                    bet["result"] = "open"
                else:
                    bet["result"] = "pending"
            except Exception:
                pass
        with open(whale_bets_file, "w") as f:
            json.dump(whale_bets, f, indent=2, default=str)
    except Exception as e:
        P(f"  WARNING: Could not check whale outcomes: {e}")

report = build_report(bot_bets)
crypto_report = build_report(crypto_bets)
whale_report = build_report(whale_bets)

# Step 4: Build dashboard
P("  [4/4] Building Kalshi dashboard...")

template_file = "kalshi_dashboard_template.html"
if not os.path.exists(template_file):
    P(f"  No {template_file} found.")
    sys.exit(1)

with open(template_file) as f:
    html = f.read()

refresh_time = datetime.now(timezone.utc).isoformat()

html = html.replace("KALSHI_REPORT_PLACEHOLDER", json.dumps(report, default=str))
html = html.replace("KALSHI_STATUS_PLACEHOLDER", json.dumps(bot_status, default=str))
html = html.replace("CRYPTO_REPORT_PLACEHOLDER", json.dumps(crypto_report, default=str))
html = html.replace("CRYPTO_STATUS_PLACEHOLDER", json.dumps(crypto_status, default=str))
html = html.replace("WHALE_REPORT_PLACEHOLDER", json.dumps(whale_report, default=str))
html = html.replace("WHALE_STATUS_PLACEHOLDER", json.dumps(whale_status, default=str))
html = html.replace("DATA_REFRESHED_PLACEHOLDER", refresh_time)

with open("kalshi_dashboard.html", "w") as f:
    f.write(html)

s_resolved = [b for b in bot_bets if b.get("result") in ("win", "loss")]
c_resolved = [b for b in crypto_bets if b.get("result") in ("win", "loss")]
w_resolved = [b for b in whale_bets if b.get("result") in ("win", "loss")]

P()
P(f"  Kalshi Dashboard refreshed!")
P(f"  Sports: {len(bot_bets)} bets, {len(s_resolved)} resolved, P&L: ${sum(b.get('pnl',0) for b in s_resolved):+.2f}")
P(f"  Crypto: {len(crypto_bets)} bets, {len(c_resolved)} resolved, P&L: ${sum(b.get('pnl',0) for b in c_resolved):+.2f}")
P(f"  Whale:  {len(whale_bets)} bets, {len(w_resolved)} resolved, P&L: ${sum(b.get('pnl',0) for b in w_resolved):+.2f}")
P(f"  -> Open kalshi_dashboard.html to view")
