"""
Dashboard Server + Crypto Bot
==============================
FastAPI server that serves the Kalshi dashboard with live data.
Crypto 15m bot runs as a background thread.
Data is fetched in the background every 2 minutes — API returns instantly.

Railway start command: uvicorn dashboard_server:app --host 0.0.0.0 --port $PORT
"""

import os
import json
import time
import threading
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

app = FastAPI()

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

REFRESH_INTERVAL = 60  # Refresh data every 60 seconds

# ── Shared data store ──────────────────────────────────────────────────
_data = {"result": None, "refreshing": False, "last_refresh": 0}
_lock = threading.Lock()


# ── Data fetching ──────────────────────────────────────────────────────
def _fetch_data():
    """Fetch all dashboard data from Kalshi API. Called by background thread."""
    P("  [DATA] Refreshing dashboard data...")
    start = time.time()

    balance_info = get_balance() or {}

    # Sports bets from kalshi_bets.json (if exists on disk)
    bot_bets = []
    if os.path.exists("kalshi_bets.json"):
        with open("kalshi_bets.json") as f:
            bot_bets = json.load(f)

    # Check sports outcomes (only unresolved ones)
    for bet in bot_bets:
        ticker = bet.get("ticker", "")
        if not ticker or bet.get("result") in ("win", "loss"):
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
                bet["pnl"] = round(contracts * (1.0 - price), 2) if won else round(-contracts * price, 2)
            elif status == "open":
                bet["result"] = "open"
            else:
                bet["result"] = "pending"
        except Exception:
            pass

    # Crypto bets from Kalshi fills API
    crypto_bets = []
    try:
        # Fetch all fills
        all_fills = []
        cursor = None
        while True:
            params = {"limit": 200}
            if cursor:
                params["cursor"] = cursor
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
                    "total_count": 0,
                    "total_cost_dollars": 0,
                    "timestamp": fill.get("created_time", ""),
                }
            entry = crypto_fills_by_ticker[ticker]
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

        # Batch check market outcomes — fetch all unique tickers at once
        # Use concurrent lookups to speed things up
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
        P(f"  [DATA] WARNING: Could not fetch crypto data: {e}")

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

    elapsed = time.time() - start
    P(f"  [DATA] Done in {elapsed:.1f}s: {len(bot_bets)} sports, {len(crypto_bets)} crypto")
    return result


def data_refresh_loop():
    """Background thread that refreshes data every REFRESH_INTERVAL seconds."""
    while True:
        try:
            with _lock:
                _data["refreshing"] = True
            result = _fetch_data()
            with _lock:
                _data["result"] = result
                _data["last_refresh"] = time.time()
                _data["refreshing"] = False
        except Exception as e:
            P(f"  [DATA] Refresh error: {e}")
            with _lock:
                _data["refreshing"] = False
        time.sleep(REFRESH_INTERVAL)


# ── Routes ─────────────────────────────────────────────────────────────
TEMPLATE_PATH = Path(__file__).parent / "kalshi_dashboard_template.html"


@app.get("/", response_class=HTMLResponse)
def serve_dashboard():
    return TEMPLATE_PATH.read_text()


@app.get("/api/data")
def get_data():
    with _lock:
        data = _data["result"]
    if data:
        return JSONResponse(data)
    return JSONResponse({"error": "Data still loading, try again in a few seconds"}, status_code=503)


# ── Background threads ─────────────────────────────────────────────────
def bot_thread():
    while True:
        try:
            P("  [BOT] Starting crypto 15m bot...")
            run_bot(live=True)
        except Exception as e:
            P(f"  [BOT] Crashed: {e}")
            time.sleep(30)


@app.on_event("startup")
def start_threads():
    # Start data refresh thread
    t1 = threading.Thread(target=data_refresh_loop, daemon=True)
    t1.start()
    P("  [SERVER] Data refresh thread started")

    # Start bot thread
    t2 = threading.Thread(target=bot_thread, daemon=True)
    t2.start()
    P("  [SERVER] Bot thread started")
