"""
Dashboard Server + Crypto Bot
==============================
FastAPI server that serves the Kalshi dashboard with live data.
Crypto 15m bot runs as a background thread.
Data is fetched in the background every 60 seconds — API returns instantly.

P&L uses actual Kalshi fee data from fills API (fee_cost, is_taker),
not formula estimates. Matches kalshi-dash methodology.

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
from crypto_score_bot import run as run_score_bot
# History data is committed as kalshi_history.json — no live fetch on Railway
HISTORY_FILE = "kalshi_history.json"

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

SPORTS_PREFIXES = [
    "KXNBA", "KXMLB", "KXNHL", "KXNCAA", "KXEPL", "KXUCL",
    "KXCS2", "KXLOL", "KXEUR", "KXDOTA", "KXCBB", "KXWCBB",
    "KXIPL",
]

# Exclude specific outlier tickers from dashboard
EXCLUDED_TICKERS = set(os.environ.get("EXCLUDED_TICKERS", "KXNCAAWBGAME-26MAR23UVAIOWA-UVA").split(","))

ACCOUNT_NAME = os.environ.get("ACCOUNT_NAME", "Default")
REFRESH_INTERVAL = int(os.environ.get("REFRESH_INTERVAL", "60"))

# ── Shared data store ──────────────────────────────────────────────────
_data = {"result": None, "refreshing": False, "last_refresh": 0}
_lock = threading.Lock()

_history = {"records": None}
_history_lock = threading.Lock()


# ── Helpers ────────────────────────────────────────────────────────────
def _get_crypto_name(ticker):
    for prefix, name in CRYPTO_SERIES.items():
        if ticker.startswith(prefix):
            return name
    return None


def _is_sports(ticker):
    return any(ticker.startswith(p) for p in SPORTS_PREFIXES)


# ── Data fetching ──────────────────────────────────────────────────────
def _fetch_data():
    """Fetch all dashboard data from Kalshi fills API. Called by background thread."""
    P("  [DATA] Refreshing dashboard data...")
    start = time.time()

    balance_info = get_balance() or {}

    # ── Fetch ALL fills from Kalshi API ────────────────────────────────
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

    P(f"  [DATA] Fetched {len(all_fills)} fills")

    # ── Group fills by ticker ──────────────────────────────────────────
    fills_by_ticker = {}
    for fill in all_fills:
        ticker = fill.get("ticker", "")
        if ticker not in fills_by_ticker:
            fills_by_ticker[ticker] = []
        fills_by_ticker[ticker].append(fill)

    # ── Build bets from fills (both crypto and sports) ─────────────────
    def build_bets_from_fills(ticker_filter):
        """Build bet list from fills for tickers matching filter function.
        Uses actual fee_cost and is_taker from API (like kalshi-dash)."""
        bets = []
        for ticker, fills in fills_by_ticker.items():
            if ticker in EXCLUDED_TICKERS:
                continue
            category = ticker_filter(ticker)
            if not category:
                continue

            # Aggregate fills for this ticker
            side = fills[0].get("side", "")
            total_count = 0
            total_cost = 0
            total_fee = 0
            maker_count = 0
            taker_count = 0
            earliest_time = None

            for fill in fills:
                count = int(float(fill.get("count_fp", 0)))
                fee = float(fill.get("fee_cost", 0))
                is_taker = fill.get("is_taker", False)

                if side == "yes":
                    price = float(fill.get("yes_price_dollars", 0))
                else:
                    price = float(fill.get("no_price_dollars", 0))

                total_count += count
                total_cost += count * price
                total_fee += fee

                if is_taker:
                    taker_count += count
                else:
                    maker_count += count

                fill_time = fill.get("created_time", "")
                if fill_time and (not earliest_time or fill_time < earliest_time):
                    earliest_time = fill_time

            avg_price = total_cost / total_count if total_count else 0

            bet = {
                "ticker": ticker,
                "side": side,
                "price": round(avg_price, 4),
                "bet_amount": round(total_cost, 2),
                "contracts": total_count,
                "timestamp": earliest_time or "",
                "fee": round(total_fee, 4),
                "maker_count": maker_count,
                "taker_count": taker_count,
                "result": "open",
            }

            # Add category-specific fields
            if isinstance(category, str) and category in CRYPTO_SERIES.values():
                bet["crypto"] = category
            else:
                # Sports — extract league from ticker
                for prefix in SPORTS_PREFIXES:
                    if ticker.startswith(prefix):
                        bet["league"] = prefix.replace("KX", "")
                        break

            # Check market outcome
            try:
                mkt = public_get(f"/markets/{ticker}")
                market = mkt.get("market", {})
                status = market.get("status", "")
                result_val = market.get("result", "")
                if status in ("settled", "finalized") and result_val:
                    won = (result_val == "yes" and side == "yes") or \
                          (result_val == "no" and side == "no")
                    bet["result"] = "win" if won else "loss"
                    bet["market_result"] = result_val
                    # P&L = settlement - cost - fees (like kalshi-dash)
                    # Settlement at $1 or $0 has no additional fee
                    if won:
                        bet["pnl"] = round(total_count * (1.0 - avg_price) - total_fee, 2)
                    else:
                        bet["pnl"] = round(-total_count * avg_price - total_fee, 2)
                    # ROI like kalshi-dash: net_profit / entry_cost
                    if total_cost > 0:
                        bet["roi"] = round(bet["pnl"] / total_cost * 100, 1)
                elif status == "open":
                    bet["result"] = "open"
                else:
                    bet["result"] = "pending"
            except Exception:
                pass

            bets.append(bet)

        # Sort by timestamp
        bets.sort(key=lambda b: b.get("timestamp", ""))
        return bets

    # Build crypto bets
    crypto_bets = build_bets_from_fills(
        lambda t: _get_crypto_name(t)
    )

    # Build sports bets from fills
    sports_bets = build_bets_from_fills(
        lambda t: "sports" if _is_sports(t) else None
    )

    P(f"  [DATA] {len(crypto_bets)} crypto, {len(sports_bets)} sports bets from fills")

    def build_report(bets):
        resolved = [b for b in bets if b.get("result") in ("win", "loss")]
        open_bets = [b for b in bets if b.get("result") == "open"]
        pending = [b for b in bets if b.get("result") not in ("win", "loss", "open")]
        wins = [b for b in resolved if b["result"] == "win"]
        losses = [b for b in resolved if b["result"] == "loss"]
        total_pnl = sum(b.get("pnl", 0) for b in resolved)
        total_fees = sum(b.get("fee", 0) for b in resolved)
        total_wagered = sum(b.get("bet_amount", 0) for b in bets)
        open_cost = sum(b.get("bet_amount", 0) for b in open_bets)
        # Maker/taker stats
        total_maker = sum(b.get("maker_count", 0) for b in bets)
        total_taker = sum(b.get("taker_count", 0) for b in bets)
        # Best/worst trade (like kalshi-dash)
        best_trade = max(resolved, key=lambda b: b.get("pnl", 0)) if resolved else None
        worst_trade = min(resolved, key=lambda b: b.get("pnl", 0)) if resolved else None
        best_roi = max(resolved, key=lambda b: b.get("roi", 0)) if resolved else None
        # PNL per dollar risked (like kalshi-dash)
        resolved_wagered = sum(b.get("bet_amount", 0) for b in resolved)
        pnl_per_dollar = round(total_pnl / resolved_wagered, 4) if resolved_wagered > 0 else 0
        return {
            "total_bets": len(bets),
            "resolved": len(resolved),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": round(len(wins) / len(resolved) * 100, 1) if resolved else 0,
            "total_pnl": round(total_pnl, 2),
            "total_fees": round(total_fees, 2),
            "total_wagered": round(total_wagered, 2),
            "open_count": len(open_bets),
            "open_cost": round(open_cost, 2),
            "pending_count": len(pending),
            "balance": balance_info.get("balance", 0),
            "portfolio_value": balance_info.get("portfolio_value", 0),
            "maker_fills": total_maker,
            "taker_fills": total_taker,
            "pnl_per_dollar": pnl_per_dollar,
            "best_trade": {"ticker": best_trade["ticker"], "pnl": best_trade["pnl"]} if best_trade else None,
            "worst_trade": {"ticker": worst_trade["ticker"], "pnl": worst_trade["pnl"]} if worst_trade else None,
            "best_roi_trade": {"ticker": best_roi["ticker"], "roi": best_roi.get("roi", 0)} if best_roi else None,
            "bets": bets,
        }

    result = {
        "account_name": ACCOUNT_NAME,
        "sports_report": build_report(sports_bets),
        "sports_status": {},
        "crypto_report": build_report(crypto_bets),
        "crypto_status": {},
        "refreshed_at": datetime.now(timezone.utc).isoformat(),
    }

    elapsed = time.time() - start
    P(f"  [DATA] Done in {elapsed:.1f}s: {len(sports_bets)} sports, {len(crypto_bets)} crypto")
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


@app.get("/api/history")
def get_history():
    """Serve historical sports game records from committed JSON file."""
    with _history_lock:
        records = _history["records"]

    if records is None:
        # Load from committed file on first request
        if os.path.exists(HISTORY_FILE):
            try:
                with open(HISTORY_FILE) as f:
                    records = json.load(f)
                with _history_lock:
                    _history["records"] = records
            except Exception:
                records = None

    if records is None:
        return JSONResponse(
            {"error": "No history data. Run fetch_kalshi_history.py locally and commit kalshi_history.json."},
            status_code=404,
        )

    return JSONResponse({
        "count": len(records),
        "records": records,
    })


def _load_history_cache():
    """Load committed history JSON into memory on boot."""
    if os.path.exists(HISTORY_FILE):
        try:
            with open(HISTORY_FILE) as f:
                cached = json.load(f)
            with _history_lock:
                _history["records"] = cached
            P(f"  [HISTORY] Loaded {len(cached)} cached records from disk")
        except Exception as e:
            P(f"  [HISTORY] Cache load failed: {e}")


# ── Score bot data ─────────────────────────────────────────────────────
SCORE_BETS_FILE = "crypto_score_bets.json"
SCORE_STATUS_FILE = "crypto_score_status.json"


@app.get("/api/score-data")
def get_score_data():
    """Serve score bot bets + status for dashboard."""
    bets = []
    status = {}
    if os.path.exists(SCORE_BETS_FILE):
        try:
            with open(SCORE_BETS_FILE) as f:
                bets = json.load(f)
        except Exception:
            pass
    if os.path.exists(SCORE_STATUS_FILE):
        try:
            with open(SCORE_STATUS_FILE) as f:
                status = json.load(f)
        except Exception:
            pass

    # Build report from bets
    trades = [b for b in bets if b.get("action") == "trade"]
    skips = [b for b in bets if b.get("action") == "skip"]

    # Check outcomes for trades
    for bet in trades:
        if bet.get("result") == "open":
            ticker = bet.get("ticker", "")
            try:
                mkt = public_get(f"/markets/{ticker}")
                market = mkt.get("market", {})
                mkt_status = market.get("status", "")
                result_val = market.get("result", "")
                if mkt_status in ("settled", "finalized") and result_val:
                    side = bet.get("side", "")
                    won = (result_val == "yes" and side == "yes") or \
                          (result_val == "no" and side == "no")
                    bet["result"] = "win" if won else "loss"
                    bet["market_result"] = result_val
                    price = bet.get("price", 0)
                    contracts = bet.get("contracts", 1)
                    if won:
                        bet["pnl"] = round(contracts * (1.0 - price), 2)
                    else:
                        bet["pnl"] = round(-contracts * price, 2)
            except Exception:
                pass

    # Save updated results back
    if os.path.exists(SCORE_BETS_FILE):
        try:
            with open(SCORE_BETS_FILE, "w") as f:
                json.dump(bets, f, indent=2, default=str)
        except Exception:
            pass

    resolved = [b for b in trades if b.get("result") in ("win", "loss")]
    wins = [b for b in resolved if b["result"] == "win"]
    losses = [b for b in resolved if b["result"] == "loss"]
    total_pnl = sum(b.get("pnl", 0) for b in resolved)

    # Score distribution
    score_dist = {}
    for b in trades:
        sc = b.get("score", 0)
        score_dist[str(sc)] = score_dist.get(str(sc), 0) + 1

    # Per-crypto breakdown
    by_crypto = {}
    for b in trades:
        c = b.get("crypto", "?")
        if c not in by_crypto:
            by_crypto[c] = {"trades": 0, "wins": 0, "losses": 0, "pnl": 0, "skips": 0}
        by_crypto[c]["trades"] += 1
        if b.get("result") == "win":
            by_crypto[c]["wins"] += 1
            by_crypto[c]["pnl"] += b.get("pnl", 0)
        elif b.get("result") == "loss":
            by_crypto[c]["losses"] += 1
            by_crypto[c]["pnl"] += b.get("pnl", 0)
    for b in skips:
        c = b.get("crypto", "?")
        if c not in by_crypto:
            by_crypto[c] = {"trades": 0, "wins": 0, "losses": 0, "pnl": 0, "skips": 0}
        by_crypto[c]["skips"] += 1

    return JSONResponse({
        "total_trades": len(trades),
        "total_skips": len(skips),
        "resolved": len(resolved),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(len(wins) / len(resolved) * 100, 1) if resolved else 0,
        "total_pnl": round(total_pnl, 2),
        "score_distribution": score_dist,
        "by_crypto": by_crypto,
        "indicators": status.get("indicators", {}),
        "last_indicator_update": status.get("last_update", ""),
        "recent_bets": bets[-50:],  # Last 50 decisions
    })


# ── Background threads ─────────────────────────────────────────────────
def bot_thread():
    while True:
        try:
            P("  [BOT] Starting crypto 15m bot...")
            run_bot(live=True)
        except Exception as e:
            P(f"  [BOT] Crashed: {e}")
            time.sleep(30)


def score_bot_thread():
    while True:
        try:
            P("  [SCORE-BOT] Starting crypto score bot...")
            run_score_bot(live=True)
        except Exception as e:
            P(f"  [SCORE-BOT] Crashed: {e}")
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

    # Start score bot thread
    t3 = threading.Thread(target=score_bot_thread, daemon=True)
    t3.start()
    P("  [SERVER] Score bot thread started")

    # Load cached history (no auto-fetch — use /api/history/refresh manually)
    _load_history_cache()
    P("  [SERVER] History cache loaded (manual refresh only via /api/history/refresh)")
