"""
Dashboard Server + Crypto Bot
==============================
FastAPI server that serves the Kalshi dashboard with live data.
Crypto 15m bot runs as a background thread.

Railway start command: uvicorn dashboard_server:app --host 0.0.0.0 --port $PORT
"""

import os
import sys
import json
import time
import threading
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI()

# ── Import shared Kalshi auth from crypto bot ──────────────────────────
# We reuse the auth functions from crypto_15m_bot
from crypto_15m_bot import (
    auth_get, public_get, get_balance, get_existing_positions,
    run as run_bot, P,
)

# ── Config ─────────────────────────────────────────────────────────────
CRYPTO_SERIES = {
    "KXBTC15M": "BTC",
    "KXETH15M": "ETH",
    "KXSOL15M": "SOL",
    "KXXRP15M": "XRP",
    "KXDOGE15M": "DOGE",
    "KXHYPE15M": "HYPE",
    "KXBNB15M": "BNB",
}

# ── Cache ──────────────────────────────────────────────────────────────
_cache = {"data": None, "ts": 0}
CACHE_TTL = 60  # seconds


# ── Data fetching (from refresh_kalshi_dashboard.py logic) ─────────────
def fetch_dashboard_data():
    """Fetch all dashboard data fresh from Kalshi API."""
    now = time.time()
    if _cache["data"] and (now - _cache["ts"]) < CACHE_TTL:
        return _cache["data"]

    P("  [API] Fetching dashboard data...")

    # Balance
    balance_info = get_balance() or {}

    # Sports bets from kalshi_bets.json (if exists on disk)
    bot_bets = []
    if os.path.exists("kalshi_bets.json"):
        with open("kalshi_bets.json") as f:
            bot_bets = json.load(f)

    # Check sports outcomes
    for bet in bot_bets:
        ticker = bet.get("ticker", "")
        if not ticker:
            continue
        if bet.get("result") in ("win", "loss"):
            continue
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
            elif status == "open":
                bet["result"] = "open"
            else:
                bet["result"] = "pending"
        except Exception:
            pass
        time.sleep(0.15)

    # Crypto bets from Kalshi fills API
    crypto_bets = []
    try:
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

        # Group by ticker
        crypto_fills_by_ticker = {}
        for fill in all_fills:
            ticker = fill.get("ticker", "")
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
            if entry["side"] == "yes":
                price = float(fill.get("yes_price_dollars", fill.get("yes_price_fixed", 0)))
            else:
                price = float(fill.get("no_price_dollars", fill.get("no_price_fixed", 0)))
            entry["total_count"] += count
            entry["total_cost_dollars"] += count * price
            fill_time = fill.get("created_time", "")
            if fill_time and (not entry["timestamp"] or fill_time < entry["timestamp"]):
                entry["timestamp"] = fill_time

        # Build bets and check outcomes
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

    except Exception as e:
        P(f"  WARNING: Could not fetch crypto data: {e}")

    # Build reports
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

    result = {
        "sports_report": build_report(bot_bets),
        "sports_status": {},
        "crypto_report": build_report(crypto_bets),
        "crypto_status": {},
        "refreshed_at": datetime.now(timezone.utc).isoformat(),
    }

    _cache["data"] = result
    _cache["ts"] = time.time()
    P(f"  [API] Data fetched: {len(bot_bets)} sports, {len(crypto_bets)} crypto")
    return result


# ── Routes ─────────────────────────────────────────────────────────────
TEMPLATE_PATH = Path(__file__).parent / "kalshi_dashboard_template.html"


@app.get("/", response_class=HTMLResponse)
def serve_dashboard():
    return TEMPLATE_PATH.read_text()


@app.get("/api/data")
def get_data():
    try:
        data = fetch_dashboard_data()
        return JSONResponse(data)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


# ── Bot background thread ──────────────────────────────────────────────
def bot_thread():
    while True:
        try:
            P("  [BOT] Starting crypto 15m bot...")
            run_bot(live=True)
        except Exception as e:
            P(f"  [BOT] Crashed: {e}")
            time.sleep(30)


@app.on_event("startup")
def start_bot():
    t = threading.Thread(target=bot_thread, daemon=True)
    t.start()
    P("  [SERVER] Bot thread started")
