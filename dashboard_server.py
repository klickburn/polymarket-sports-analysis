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
from datetime import datetime, timezone, timedelta
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.middleware.gzip import GZipMiddleware

app = FastAPI()
app.add_middleware(GZipMiddleware, minimum_size=1000)

from crypto_15m_bot import (
    auth_get, public_get, get_balance, get_existing_positions,
    run as run_bot, P,
)
from crypto_score_bot import (run as run_score_bot, SCALE_UP_COUNT,
                              SPLIT_DIP_ENABLED, SPLIT_DIP_PRICE, SPLIT_DIP_COUNT,
                              SPLIT_DIP_TIERS,
                              TRAIL_STOP_ENABLED, TRAIL_STOP_ARM, TRAIL_STOP_GIVEBACK,
                              TRAIL_STREAK_HOLD_K, TRAIL_STOP_DYNAMIC,
                              TRAIL_LOSS_FLOOR, TRAIL_FLOOR_REACT_K, TRAIL_HARD_FLOOR,
                              PROTECT_MODE, COUNT_SOFT_C, COUNT_REACT_K,
                              PRESS_STREAK_N, PRESS_MULT,
                              VOL_GATE, SIG_WEIGHTS, WR_CAP_N, WR_CAP,
                              WR_BOOST_N, WR_BOOST_LO, WR_BOOST_HI, WR_BOOST_MULT,
                              _trailing_wr)
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
REFRESH_INTERVAL = int(os.environ.get("REFRESH_INTERVAL", "300"))  # 5 min

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


# ── Cached data files ─────────────────────────────────────────────────
CACHE_DIR = os.environ.get("SCORE_DATA_DIR", "/data")
if not os.path.isdir(CACHE_DIR):
    CACHE_DIR = "."
FILLS_CACHE = os.path.join(CACHE_DIR, "kalshi_fills_cache.json")
RESULTS_CACHE = os.path.join(CACHE_DIR, "kalshi_results_cache.json")


def _load_cache(path):
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def _save_cache(path, data):
    try:
        with open(path, "w") as f:
            json.dump(data, f, default=str)
    except Exception as e:
        P(f"  [CACHE] Save error: {e}")


_first_load = True

# ── Data fetching (incremental) ──────────────────────────────────────
def _fetch_data():
    """Fetch dashboard data incrementally — only new fills + re-check open markets."""
    global _first_load
    P("  [DATA] Refreshing dashboard data...")
    start = time.time()

    balance_info = get_balance() or {}

    # ── Load cached fills and results ─────────────────────────────────
    fills_cache = _load_cache(FILLS_CACHE)  # {ticker: [fills]}
    results_cache = _load_cache(RESULTS_CACHE)  # {ticker: {result, market_result}}

    # Find latest fill timestamp to fetch only new ones
    latest_ts = ""
    for fills in fills_cache.values():
        for f in fills:
            t = f.get("created_time", "")
            if t > latest_ts:
                latest_ts = t

    # ── Fetch only NEW fills from Kalshi API ──────────────────────────
    new_fills = []
    cursor = None
    while True:
        params = {"limit": 200}
        if cursor:
            params["cursor"] = cursor
        if latest_ts:
            params["min_ts"] = int(datetime.fromisoformat(latest_ts.replace("Z", "+00:00")).timestamp()) + 1
        data = auth_get("/portfolio/fills", params=params)
        fills = data.get("fills", [])
        new_fills.extend(fills)
        cursor = data.get("cursor")
        if not cursor or not fills:
            break

    # Merge new fills into cache
    new_count = 0
    for fill in new_fills:
        ticker = fill.get("ticker", "")
        if ticker not in fills_cache:
            fills_cache[ticker] = []
        # Deduplicate by trade_id
        existing_ids = {f.get("trade_id") for f in fills_cache[ticker]}
        if fill.get("trade_id") not in existing_ids:
            fills_cache[ticker].append(fill)
            new_count += 1

    if new_count > 0:
        _save_cache(FILLS_CACHE, fills_cache)
        P(f"  [DATA] {new_count} new fills (total {sum(len(v) for v in fills_cache.values())} cached)")
    else:
        P(f"  [DATA] No new fills ({sum(len(v) for v in fills_cache.values())} cached)")

    # ── Build bets from cached fills ──────────────────────────────────
    def build_bets_from_fills(ticker_filter):
        bets = []
        for ticker, fills in fills_cache.items():
            if ticker in EXCLUDED_TICKERS:
                continue
            category = ticker_filter(ticker)
            if not category:
                continue

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

            if isinstance(category, str) and category in CRYPTO_SERIES.values():
                bet["crypto"] = category
            else:
                for prefix in SPORTS_PREFIXES:
                    if ticker.startswith(prefix):
                        bet["league"] = prefix.replace("KX", "")
                        break

            # Use cached result if already resolved — skip API call
            if ticker in results_cache:
                cached = results_cache[ticker]
                bet["result"] = cached["result"]
                bet["market_result"] = cached.get("market_result", "")
                won = bet["result"] == "win"
                if won:
                    bet["pnl"] = round(total_count * (1.0 - avg_price) - total_fee, 2)
                else:
                    bet["pnl"] = round(-total_count * avg_price - total_fee, 2)
                if total_cost > 0:
                    bet["roi"] = round(bet["pnl"] / total_cost * 100, 1)
            else:
                if _first_load:
                    bet["result"] = "pending"
                else:
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
                            if won:
                                bet["pnl"] = round(total_count * (1.0 - avg_price) - total_fee, 2)
                            else:
                                bet["pnl"] = round(-total_count * avg_price - total_fee, 2)
                            if total_cost > 0:
                                bet["roi"] = round(bet["pnl"] / total_cost * 100, 1)
                            results_cache[ticker] = {
                                "result": bet["result"],
                                "market_result": result_val,
                            }
                        elif status == "open":
                            bet["result"] = "open"
                        else:
                            bet["result"] = "pending"
                        time.sleep(0.2)
                    except Exception:
                        pass

            bets.append(bet)

        bets.sort(key=lambda b: b.get("timestamp", ""))
        return bets

    crypto_bets = build_bets_from_fills(lambda t: _get_crypto_name(t))
    sports_bets = build_bets_from_fills(lambda t: "sports" if _is_sports(t) else None)

    # Save results cache after resolving
    _save_cache(RESULTS_CACHE, results_cache)

    P(f"  [DATA] {len(crypto_bets)} crypto, {len(sports_bets)} sports bets")

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
        total_maker = sum(b.get("maker_count", 0) for b in bets)
        total_taker = sum(b.get("taker_count", 0) for b in bets)
        best_trade = max(resolved, key=lambda b: b.get("pnl", 0)) if resolved else None
        worst_trade = min(resolved, key=lambda b: b.get("pnl", 0)) if resolved else None
        best_roi = max(resolved, key=lambda b: b.get("roi", 0)) if resolved else None
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

    _first_load = False
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
SCORE_DATA_DIR = os.environ.get("SCORE_DATA_DIR", "/data")
if not os.path.isdir(SCORE_DATA_DIR):
    SCORE_DATA_DIR = "."
SCORE_BETS_FILE = os.path.join(SCORE_DATA_DIR, "crypto_score_bets.json")
SCORE_STATUS_FILE = os.path.join(SCORE_DATA_DIR, "crypto_score_status.json")


_score_cache = {"result": None, "last_resolve": 0}
_score_lock = threading.Lock()

import math

def kalshi_fee(contracts, price):
    """Estimate Kalshi taker fee: ceil(0.07 * C * P * (1-P)) rounded up to cent.
    Used for live P&L; the audit overwrites with exact fee_cost from fills."""
    try:
        p = float(price)
        c = int(contracts)
        if c <= 0 or p <= 0 or p >= 1:
            return 0.0
        return math.ceil(0.07 * c * p * (1 - p) * 100) / 100.0
    except Exception:
        return 0.0

def _bet_fee(bet, contracts, price):
    """Real fee if reconciled from fills, else estimate."""
    f = bet.get("fee")
    return f if f is not None else kalshi_fee(contracts, price)


def _resolve_score_bets():
    """Background: resolve open score bot bets via Kalshi API."""
    bets = []
    if os.path.exists(SCORE_BETS_FILE):
        try:
            with open(SCORE_BETS_FILE) as f:
                bets = json.load(f)
        except json.JSONDecodeError:
            # Try to repair corrupted JSON
            try:
                with open(SCORE_BETS_FILE) as f:
                    raw = f.read()
                last_bracket = raw.rfind(']')
                if last_bracket > 0:
                    bets = json.loads(raw[:last_bracket + 1])
                    with open(SCORE_BETS_FILE, "w") as f:
                        json.dump(bets, f, indent=2, default=str)
                    P(f"  [SCORE-DATA] Repaired corrupted bets file: {len(bets)} bets")
            except Exception:
                return
        except Exception:
            return

    before_filter = len(bets)
    bets = [b for b in bets if b.get("ticker")]
    if len(bets) < before_filter:
        P(f"  [SCORE-DATA] Removed {before_filter - len(bets)} entries with no ticker")

    # Drop bets older than the last reset (resurrection guard)
    try:
        with open(os.path.join(SCORE_DATA_DIR, ".reset_ts")) as f:
            cutoff = f.read().strip()
        if cutoff:
            n = len(bets)
            bets = [b for b in bets if b.get("timestamp", "") >= cutoff]
            if len(bets) < n:
                P(f"  [SCORE-DATA] Dropped {n - len(bets)} bets older than reset at {cutoff}")
    except Exception:
        pass

    changed = len(bets) < before_filter

    # Normalize fill_price from cents to dollars if needed, recalculate P&L
    for bet in bets:
        fp = bet.get("fill_price")
        if fp is not None and fp > 1:
            bet["fill_price"] = fp / 100
            changed = True
        # Fix NO fill_prices that were stored as YES price (V2 bug)
        if bet.get("side") == "no" and fp is not None and bet.get("price"):
            recorded_no = bet["price"]
            if fp < 0.50 and recorded_no >= 0.50:
                bet["fill_price"] = round(1.0 - fp, 4)
                bet["fill_price_checked"] = False
                changed = True
        if bet.get("audited"):
            continue  # P&L already reconciled from actual fills — don't recompute
        if bet.get("action") == "trade" and bet.get("result") in ("win", "loss"):
            actual_price = bet.get("fill_price", bet.get("price", 0))
            contracts = bet.get("filled_count", bet.get("contracts", 1))
            fee = _bet_fee(bet, contracts, actual_price)
            if bet["result"] == "win":
                correct_pnl = round(contracts * (1.0 - actual_price) - fee, 2)
            else:
                correct_pnl = round(-contracts * actual_price - fee, 2)
            if abs((bet.get("pnl", 0)) - correct_pnl) > 0.001:
                bet["pnl"] = correct_pnl
                changed = True

    # A 15-minute market settles within minutes, so age alone says nothing about
    # whether an outcome is available — Kalshi will still answer for a ticker from
    # weeks ago. This used to blind-mark every bet older than 2h as "expired",
    # discarding a real win/loss without ever asking. It was invisible while the
    # score-bot thread clobbered these writes; now that both sides merge, it would
    # have destroyed ~4,000 outcomes on the first pass. Expiry is now decided ONLY
    # by the market lookup below.
    #
    # Bounded and oldest-first for the same reason the bot's resolver is: a pass
    # over a large backlog makes one or two API calls per bet, and an unbounded
    # pass never finishes.
    RESOLVE_CAP = int(os.environ.get("SERVER_RESOLVE_MAX", "150"))
    _open = [b for b in bets if b.get("result") == "open"]
    _open.sort(key=lambda b: b.get("window_end") or b.get("timestamp") or "")
    _attempt = set(id(b) for b in _open[:RESOLVE_CAP])
    if len(_open) > RESOLVE_CAP:
        P(f"  [SCORE-DATA] {len(_open)} open — resolving oldest {RESOLVE_CAP} this pass")
    for bet in bets:
        if bet.get("result") == "open":
            if id(bet) not in _attempt:
                continue
            # For traded bets, verify the order was actually filled before resolving
            if bet.get("action") == "trade" and bet.get("order_id"):
                verified = bet.get("filled_count", 0) > 0  # fill already confirmed at placement
                try:
                    order_resp = auth_get(f"/portfolio/orders/{bet['order_id']}")
                    order_data = order_resp.get("order", {})
                    order_status = order_data.get("status", "")
                    remaining = int(order_data.get("remaining_count", 0))
                    filled_count = int(order_data.get("count", 0)) - remaining
                    actual_price = order_data.get("avg_price", 0)

                    if order_status in ("canceled", "expired") and filled_count == 0:
                        # Order was never filled — mark as unfilled
                        bet["result"] = "unfilled"
                        bet["status"] = order_status
                        changed = True
                        time.sleep(0.3)
                        continue
                    elif order_status == "resting":
                        # Still sitting in orderbook — skip for now
                        time.sleep(0.3)
                        continue
                    elif filled_count > 0:
                        verified = True
                        if actual_price > 0:
                            avg_p = actual_price / 100 if actual_price > 1 else actual_price
                            if bet.get("side") == "no":
                                avg_p = 1.0 - avg_p
                            bet["fill_price"] = avg_p
                        bet["filled_count"] = min(filled_count, bet.get("contracts", filled_count))
                    time.sleep(0.3)
                except Exception:
                    pass
                if not verified:
                    # No evidence this order ever filled — never book settlement
                    # P&L on it; retry verification next cycle
                    continue

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
                    # Use actual Kalshi fill price for traded bets, bot price for skips
                    price = bet.get("fill_price", bet.get("price", 0)) if bet.get("action") == "trade" else bet.get("price", 0)
                    contracts = bet.get("filled_count", bet.get("contracts", 1))
                    fee = _bet_fee(bet, contracts, price) if bet.get("action") == "trade" else 0.0
                    if won:
                        bet["pnl"] = round(contracts * (1.0 - price) - fee, 2)
                    else:
                        bet["pnl"] = round(-contracts * price - fee, 2)
                    if bet.get("action") == "skip":
                        bet["hypothetical_pnl"] = bet["pnl"]
                        bet["would_have_won"] = won
                        del bet["pnl"]
                    changed = True
                time.sleep(0.3)
            except Exception:
                pass

    if changed:
        try:
            # This pass makes an API call per open bet, so `bets` can be minutes
            # stale by now — and the score-bot thread writes the same file every
            # time it places a trade. Re-read and re-attach anything that landed
            # underneath us, or this write silently deletes trades the bot just
            # made. (The bot performs the mirror-image merge in save_bets.)
            fresh, _ = [], None
            try:
                with open(SCORE_BETS_FILE) as f:
                    fresh = json.load(f)
            except Exception:
                fresh = []
            if fresh:
                have = {(b.get("timestamp"), b.get("crypto"), b.get("side")) for b in bets}
                added = [b for b in fresh
                         if (b.get("timestamp"), b.get("crypto"), b.get("side")) not in have]
                if added:
                    bets = bets + added
                    bets.sort(key=lambda b: b.get("timestamp", ""))
                    P(f"  [SCORE-DATA] Re-attached {len(added)} bet(s) written while resolving")
            # Atomic write to prevent corruption
            tmp = SCORE_BETS_FILE + ".tmp"
            with open(tmp, "w") as f:
                json.dump(bets, f, indent=2, default=str)
            if os.path.exists(SCORE_BETS_FILE):
                try:
                    os.replace(SCORE_BETS_FILE, SCORE_BETS_FILE + ".bak")
                except Exception:
                    pass
            os.replace(tmp, SCORE_BETS_FILE)
        except Exception:
            pass

    return bets


def _build_scaling_performance(resolved, status=None):
    """Analyze how dynamic contract scaling is performing (BTC/ETH)."""
    if not resolved:
        return {}

    # Only BTC/ETH, only after scaling was deployed; exclude split-dip adds
    scaling_deployed = "2026-06-17T19:40"
    filtered = [b for b in resolved
                if b.get("crypto") in ("BTC", "ETH")
                and b.get("timestamp", "") >= scaling_deployed
                and not b.get("dip_add")]

    trades_at_1 = [b for b in filtered if b.get("contracts", 1) == 1]
    trades_at_scaled = [b for b in filtered if b.get("contracts", 1) > 1]

    def calc_stats(trades):
        if not trades:
            return {"trades": 0, "wins": 0, "losses": 0, "win_rate": 0, "pnl": 0}
        wins = sum(1 for b in trades if b["result"] == "win")
        pnl = sum(b.get("pnl", 0) for b in trades)
        return {
            "trades": len(trades),
            "wins": wins,
            "losses": len(trades) - wins,
            "win_rate": round(wins / len(trades) * 100, 1),
            "pnl": round(pnl, 2),
        }

    # Group A: signals 4,5 | Group B: signals 1,2,3 | sig 0,6,7 never scale
    group_a_sigs = {4, 5}
    group_b_sigs = {1, 2, 3}
    by_group = {
        "Group A (sig 4,5)": {"at_1": [], "at_scaled": []},
        "Group B (sig 1,2,3)": {"at_1": [], "at_scaled": []},
    }
    for b in filtered:
        sig = b.get("score", 0) + 3
        if sig in group_a_sigs:
            key = "Group A (sig 4,5)"
        elif sig in group_b_sigs:
            key = "Group B (sig 1,2,3)"
        else:
            continue
        if b.get("contracts", 1) > 1:
            by_group[key]["at_scaled"].append(b)
        else:
            by_group[key]["at_1"].append(b)

    breakdown = {}
    for key, data in sorted(by_group.items()):
        breakdown[key] = {
            "at_1": calc_stats(data["at_1"]),
            "at_scaled": calc_stats(data["at_scaled"]),
        }

    # Read actual scale state from bot's status file
    scale_configs = {
        "A": {"signals": {4, 5}, "up_window": 4, "up_thresh": 3, "down_window": 16, "down_thresh": 3},
        "B": {"signals": {1, 2, 3}, "up_window": 6, "up_thresh": 4, "down_window": 16, "down_thresh": 4},
    }
    bot_scale_state = (status or {}).get("scale_state", {})
    bot_scale_up_at = (status or {}).get("scale_up_at", {})
    if not bot_scale_state:
        try:
            if os.path.exists(SCORE_STATUS_FILE):
                with open(SCORE_STATUS_FILE) as f:
                    saved = json.load(f)
                    bot_scale_state = saved.get("scale_state", {})
                    bot_scale_up_at = saved.get("scale_up_at", {})
        except Exception:
            pass

    all_resolved = [b for b in resolved if b.get("crypto") in ("BTC", "ETH")
                    and not b.get("dip_add")]
    scaling_status = {}
    for gname, cfg in scale_configs.items():
        g_trades = [b for b in all_resolved if (b.get("score", 0) + 3) in cfg["signals"]]
        current = int(bot_scale_state.get(gname, 1))

        if current == 1:
            window = cfg["up_window"]
            last_n = g_trades[-window:] if len(g_trades) >= window else g_trades
            wins = sum(1 for x in last_n if x["result"] == "win")
            needed = cfg["up_thresh"] - wins
            scaling_status[gname] = {
                "current": 1,
                "direction": "up",
                "wins_in_window": wins,
                "window": window,
                "threshold": cfg["up_thresh"],
                "needed": max(0, needed),
                "recent": [t["result"][0].upper() for t in last_n[-8:]],
            }
        else:
            # Only count trades since scale-up (matches bot logic)
            up_idx = int(bot_scale_up_at.get(gname, 0))
            since_up = g_trades[up_idx:]
            window = cfg["down_window"]
            check_trades = since_up[-window:] if len(since_up) >= window else since_up
            losses = sum(1 for x in check_trades if x["result"] == "loss")
            needed = cfg["down_thresh"] - losses
            scaling_status[gname] = {
                "current": current,
                "direction": "down",
                "losses_in_window": losses,
                "trades_since_up": len(since_up),
                "window": window,
                "threshold": cfg["down_thresh"],
                "needed": max(0, needed),
                "recent": [t["result"][0].upper() for t in since_up[-8:]],
            }

    # Cool-off lid state (mirrors bot logic: trailing WR over last N resolved)
    cool_off = None
    try:
        from crypto_score_bot import COOL_OFF_WR, COOL_OFF_WINDOW, COOL_OFF_BYPASS_STREAK
        if COOL_OFF_WR > 0:
            last = all_resolved[-COOL_OFF_WINDOW:]
            wins = sum(1 for b in last if b.get("result") == "win")
            wr = wins / len(last) if last else 0
            streak = 0
            for b in reversed(all_resolved):
                if b.get("result") == "win":
                    streak += 1
                else:
                    break
            bypassing = (COOL_OFF_BYPASS_STREAK > 0 and streak >= COOL_OFF_BYPASS_STREAK)
            cool_off = {
                "active": len(last) >= COOL_OFF_WINDOW and wr >= COOL_OFF_WR and not bypassing,
                "wins": wins,
                "window": COOL_OFF_WINDOW,
                "threshold": COOL_OFF_WR,
                "win_streak": streak,
                "bypass_streak": COOL_OFF_BYPASS_STREAK,
                "bypassing": bool(len(last) >= COOL_OFF_WINDOW and wr >= COOL_OFF_WR and bypassing),
            }
            # Lid effectiveness: trades placed while the lid was on
            lid = [b for b in resolved if b.get("cool_off")]
            if lid:
                lid_wins = sum(1 for b in lid if b.get("result") == "win")
                lid_pnl = sum(b.get("pnl", 0) for b in lid)
                # What those trades would have made at the blocked scale;
                # negative saved = the lid cost money (hot run continued)
                saved = -sum(b.get("pnl", 0) * (b.get("blocked_scale", SCALE_UP_COUNT) - 1)
                             for b in lid)
                cool_off["lid_trades"] = len(lid)
                cool_off["lid_win_rate"] = round(lid_wins / len(lid) * 100, 1)
                cool_off["lid_pnl"] = round(lid_pnl, 2)
                cool_off["saved"] = round(saved, 2)
    except Exception:
        pass

    # WR-ladder boost effectiveness: P&L of the 2x-boosted trades vs the rest.
    # Boosted trades are tagged decision.wr_boosted at sizing time. Break-even
    # win rate for a binary bought at price p is p itself, so edge = WR - price.
    boost = None
    try:
        core_res = [b for b in resolved
                    if b.get("action") == "trade" and b.get("result") in ("win", "loss")
                    and b.get("crypto") in ("BTC", "ETH") and not b.get("dip_add")]
        if core_res:
            def _bstat(ts):
                if not ts:
                    return None
                w = sum(1 for b in ts if b["result"] == "win")
                prices = [b.get("fill_price") or b.get("price") or 0 for b in ts]
                ap = sum(prices) / len(prices) if prices else 0
                # Under PHANTOM_MODE the unboosted buckets are recorded but never
                # ordered, so their P&L is imaginary. Counts/win-rate/edge keep
                # every trade (that IS the history the strategy sized from), but
                # `pnl` must be real money only — these are the exact cards you
                # would read to judge whether phantom mode was working.
                real = [b for b in ts if not b.get("phantom")]
                ph = [b for b in ts if b.get("phantom")]
                pnl = sum(b.get("pnl", 0) for b in real)
                return {"count": len(ts), "wins": w,
                        "win_rate": round(w / len(ts) * 100, 1),
                        "avg_price": round(ap, 3),
                        "break_even": round(ap * 100, 1),
                        "edge": round(w / len(ts) * 100 - ap * 100, 1),
                        "pnl": round(pnl, 2),
                        "pnl_per_trade": round(pnl / len(real), 4) if real else 0,
                        "phantom_count": len(ph),
                        "phantom_pnl": round(sum(b.get("pnl", 0) for b in ph), 2),
                        "contracts": sum(b.get("filled_count", b.get("contracts", 1)) for b in real)}
            def _boosted(b):
                d = b.get("decision") or {}
                return bool(d.get("wr_boosted")) or d.get("clip_reason") == "wr_boost"
            bo = [b for b in core_res if _boosted(b)]
            sc = [b for b in core_res if not _boosted(b) and (b.get("contracts") or 1) > 1]
            on = [b for b in core_res if not _boosted(b) and (b.get("contracts") or 1) <= 1]
            boost = {"boosted": _bstat(bo), "scaled_unboosted": _bstat(sc), "at_1x": _bstat(on),
                     "mult": WR_BOOST_MULT, "band_lo": WR_BOOST_LO, "band_hi": WR_BOOST_HI,
                     "n": WR_BOOST_N}
    except Exception:
        boost = None

    # Dip-add effectiveness, broken down by price tier (10c / 20c / 5c ...)
    dip = None
    dip_trades = [b for b in resolved if b.get("dip_add") and b.get("result") in ("win", "loss")]
    if dip_trades:
        def _stat(ts, tier_price=None):
            if not ts:
                return None
            w = sum(1 for b in ts if b["result"] == "win")
            contracts = sum(b.get("filled_count", b.get("contracts", 1)) for b in ts)
            s = {"count": len(ts), "wins": w,
                 "win_rate": round(w / len(ts) * 100, 1),
                 "pnl": round(sum(b.get("pnl", 0) for b in ts), 2),
                 "contracts": contracts}
            if tier_price is not None:
                # break-even win rate = the buy price (buy at p, win pays 1)
                s["break_even"] = round(tier_price * 100, 1)
                s["edge"] = round(s["win_rate"] - tier_price * 100, 1)  # above BE = profitable
            return s
        dip = _stat(dip_trades)
        # Per-tier breakdown (untagged legacy dips -> "10c")
        # Both books currently rest at 10c, so a tier-only key would pool a
        # 25-contract split book with a 1-contract core-dip experiment and report
        # one meaningless win rate. Tag the tier with its type whenever more than
        # one type is present.
        _types = {(b.get("dip_type") or "split") for b in dip_trades}
        tiers = {}
        for b in dip_trades:
            _t = (b.get("dip_type") or "split")
            # Core = all four cells now, but still excluding fills whose window
            # turned into a split, matching _core below exactly. When the two
            # views disagreed before it was because the tier row kept
            # split_window fills that the cards dropped.
            if _t == "core" and b.get("split_window"):
                continue
            t = b.get("dip_tier") or f"{int(round(b.get('price', 0.10)*100))}c"
            if len(_types) > 1:
                t = f"{t} {_t}"
            tiers.setdefault(t, []).append(b)
        def _tier_price(t):
            try:
                return int(t.split()[0].replace("c", "")) / 100.0
            except Exception:
                return 0.10
        dip["tiers"] = {t: _stat(ts, _tier_price(t)) for t, ts in sorted(tiers.items(),
                        key=lambda kv: _tier_price(kv[0]))}

        # Losses since the last win, per book. Counted two ways: by fill and by
        # window. Both legs of a SPLIT window settle together, so a 2-leg losing
        # window adds 2 to the fill count but only 1 to the window count, and the
        # window count is the one whose streaks behave like a sequence. Core dips
        # are single-leg today, so their two counts track each other.
        def _streak(ts):
            o = sorted(ts, key=lambda b: b.get("timestamp", ""))
            if not o:
                return None
            sf = 0
            for b in reversed(o):
                if b["result"] == "win":
                    break
                sf += 1
            byw, seen = [], set()
            for b in o:
                k = b.get("window_end") or b.get("timestamp")
                if k not in seen:
                    seen.add(k)
                    byw.append([k, False])
                if b["result"] == "win":
                    byw[-1][1] = True
            sw = 0
            for _, won in reversed(byw):
                if won:
                    break
                sw += 1
            runs, cur = [], 0
            for _, won in byw:
                if won:
                    runs.append(cur)
                    cur = 0
                else:
                    cur += 1
            lw = next((b.get("timestamp") for b in reversed(o) if b["result"] == "win"), None)
            hrs = None
            if lw:
                try:
                    t = datetime.fromisoformat(lw)
                    if t.tzinfo is None:
                        t = t.replace(tzinfo=timezone.utc)
                    hrs = round((datetime.now(timezone.utc) - t).total_seconds() / 3600, 1)
                except Exception:
                    pass
            return {"fills": sf, "windows": sw, "last_win": lw, "hours_since_win": hrs,
                    "max_run": max(runs + [sw]) if (runs or sw) else 0,
                    "median_run": (sorted(runs)[len(runs) // 2] if runs else 0),
                    "runs": len(runs)}

        # The daily table below is SPLIT-only: its @100c column reprices against
        # the split book's sizing history (100 -> 25), which is meaningless for a
        # 1-contract core-dip experiment.
        ordered = sorted((b for b in dip_trades if (b.get("dip_type") or "split") == "split"),
                         key=lambda b: b.get("timestamp", ""))
        # Per-day wins. The dip book's P&L is carried by a handful of days
        # (5 of 33 days = 82% of it historically), so a daily column shows the
        # concentration that a headline win rate hides.
        _byday = {}
        for b in ordered:
            k = b.get("timestamp", "")[:10]
            e = _byday.setdefault(k, {"day": k, "fills": 0, "wins": 0, "pnl": 0.0,
                                      "pnl_at_100": 0.0})
            e["fills"] += 1
            e["wins"] += 1 if b["result"] == "win" else 0
            e["pnl"] += b.get("pnl", 0)
            # Same day repriced at a flat 100 contracts. Live size has changed
            # over time (100 -> 25), so the as-traded column is not comparable
            # across days; this one is.
            p = b.get("fill_price") or b.get("price") or 0
            if 0 < p < 1:
                f100 = _bet_fee(b, 100, p)
                e["pnl_at_100"] += (100 * (1 - p) - f100) if b["result"] == "win" \
                    else (-100 * p - f100)
        dip["by_day"] = [
            {**e, "pnl": round(e["pnl"], 2), "pnl_at_100": round(e["pnl_at_100"], 2),
             "win_rate": round(e["wins"] / e["fills"] * 100, 1) if e["fills"] else 0}
            for e in sorted(_byday.values(), key=lambda x: x["day"], reverse=True)
        ]

        # The core book is all four cells (BTC yes/no, ETH yes/no) flat at 1
        # contract. split_window fills are still excluded: those were topped up
        # by the split tier, so their size and P&L belong to that book.
        _core = [b for b in dip_trades
                 if b.get("dip_type") == "core"
                 and not b.get("split_window")]
        dip["streak"] = _streak([b for b in dip_trades
                                  if (b.get("dip_type") or "split") == "split"])
        dip["streak_core"] = _streak(_core)

        _cbd = {}
        for b in sorted(_core, key=lambda x: x.get("timestamp", "")):
            k = b.get("timestamp", "")[:10]
            e = _cbd.setdefault(k, {"day": k, "fills": 0, "wins": 0, "pnl": 0.0,
                                    "contracts": 0, "pnl_at_100": 0.0})
            e["fills"] += 1
            e["wins"] += 1 if b["result"] == "win" else 0
            e["pnl"] += b.get("pnl", 0)
            e["contracts"] += b.get("filled_count", b.get("contracts", 0)) or 0
            # Same flat-100c repricing the split table uses, so the two books can
            # be read against each other despite different live sizes.
            p = b.get("fill_price") or b.get("price") or 0
            if 0 < p < 1:
                f100 = _bet_fee(b, 100, p)
                e["pnl_at_100"] += (100 * (1 - p) - f100) if b["result"] == "win" \
                    else (-100 * p - f100)
        # Split book by crypto x side. Sizing has moved (100 -> 25 -> 26), so the
        # as-traded P&L is not comparable across cells that traded in different
        # periods; the @100c column reprices every fill flat.
        _grid = {}
        for b in ordered:
            k = f"{b.get('crypto')} {b.get('side')}"
            g = _grid.setdefault(k, {"count": 0, "wins": 0, "pnl": 0.0,
                                     "pnl_at_100": 0.0, "contracts": 0})
            g["count"] += 1
            g["wins"] += 1 if b["result"] == "win" else 0
            g["pnl"] += b.get("pnl", 0)
            g["contracts"] += b.get("filled_count", b.get("contracts", 0)) or 0
            p = b.get("fill_price") or b.get("price") or 0
            if 0 < p < 1:
                f100 = _bet_fee(b, 100, p)
                g["pnl_at_100"] += (100 * (1 - p) - f100) if b["result"] == "win" \
                    else (-100 * p - f100)
        dip["grid_split"] = {k: {**v, "pnl": round(v["pnl"], 2),
                                 "pnl_at_100": round(v["pnl_at_100"], 2),
                                 "win_rate": round(v["wins"] / v["count"] * 100, 1)}
                             for k, v in sorted(_grid.items())}

        # Core book by crypto x side -- the whole point of running all four flat.
        # Every cell is 1 contract, so as-traded P&L is already comparable; the
        # @100c column is kept so core cells can be read against the split grid.
        _cg = {}
        for b in _core:
            k = f"{b.get('crypto')} {b.get('side')}"
            g = _cg.setdefault(k, {"count": 0, "wins": 0, "pnl": 0.0,
                                   "pnl_at_100": 0.0, "contracts": 0})
            g["count"] += 1
            g["wins"] += 1 if b["result"] == "win" else 0
            g["pnl"] += b.get("pnl", 0)
            g["contracts"] += b.get("filled_count", b.get("contracts", 0)) or 0
            p = b.get("fill_price") or b.get("price") or 0
            if 0 < p < 1:
                f100 = _bet_fee(b, 100, p)
                g["pnl_at_100"] += (100 * (1 - p) - f100) if b["result"] == "win" \
                    else (-100 * p - f100)
        dip["grid_core"] = {k: {**v, "pnl": round(v["pnl"], 2),
                                "pnl_at_100": round(v["pnl_at_100"], 2),
                                "win_rate": round(v["wins"] / v["count"] * 100, 1)}
                            for k, v in sorted(_cg.items())}

        dip["by_day_core"] = [
            {**e, "pnl": round(e["pnl"], 2), "pnl_at_100": round(e["pnl_at_100"], 2),
             "win_rate": round(e["wins"] / e["fills"] * 100, 1) if e["fills"] else 0}
            for e in sorted(_cbd.values(), key=lambda x: x["day"], reverse=True)
        ]

    # Protection status (persisted by the bot); only show for today
    trail = None
    if PROTECT_MODE == "count":
        raw = (status or {}).get("trail") or {}
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        r = raw if raw.get("day") == today else {}
        trail = {"mode": "count", "soft_c": COUNT_SOFT_C, "react_k": COUNT_REACT_K,
                 "press_streak_n": PRESS_STREAK_N, "press_mult": PRESS_MULT,
                 "net_count": r.get("net_count", 0), "stopped": bool(r.get("stopped")),
                 "streak": r.get("streak", 0), "resolved": r.get("resolved", 0),
                 "vol_gate": VOL_GATE, "sig_weights": SIG_WEIGHTS,
                 "wr_cap": WR_CAP, "wr_cap_n": WR_CAP_N,
                 "wr_boost_mult": WR_BOOST_MULT, "wr_boost_n": WR_BOOST_N,
                 "wr_boost_lo": WR_BOOST_LO, "wr_boost_hi": WR_BOOST_HI,
                 "wr_cap_val": _trailing_wr(resolved, WR_CAP_N) if WR_CAP > 0 else None,
                 "wr_boost_val": _trailing_wr(resolved, WR_BOOST_N) if WR_BOOST_MULT > 1 else None}
    elif TRAIL_STOP_ENABLED:
        raw = (status or {}).get("trail") or {}
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if raw.get("day") == today:
            trail = {"arm": TRAIL_STOP_ARM, "giveback": TRAIL_STOP_GIVEBACK,
                     "peak": raw.get("peak", 0), "day_pnl": raw.get("day_pnl", 0),
                     "armed": bool(raw.get("armed")), "stopped": bool(raw.get("stopped")),
                     "streak_hold_k": TRAIL_STREAK_HOLD_K, "streak": raw.get("streak", 0),
                     "ever_stopped": bool(raw.get("ever_stopped")), "dynamic": TRAIL_STOP_DYNAMIC,
                     "loss_floor": TRAIL_LOSS_FLOOR, "floor_react_k": TRAIL_FLOOR_REACT_K,
                     "floored": bool(raw.get("floored")),
                     "hard_floor": TRAIL_HARD_FLOOR, "hard_stopped": bool(raw.get("hard_stopped"))}
        else:
            trail = {"arm": TRAIL_STOP_ARM, "giveback": TRAIL_STOP_GIVEBACK,
                     "peak": 0, "day_pnl": 0, "armed": False, "stopped": False,
                     "streak_hold_k": TRAIL_STREAK_HOLD_K, "streak": 0,
                     "ever_stopped": False, "dynamic": TRAIL_STOP_DYNAMIC,
                     "loss_floor": TRAIL_LOSS_FLOOR, "floor_react_k": TRAIL_FLOOR_REACT_K,
                     "floored": False,
                     "hard_floor": TRAIL_HARD_FLOOR, "hard_stopped": False}

    return {
        "overall_at_1": calc_stats(trades_at_1),
        "overall_at_scaled": calc_stats(trades_at_scaled),
        "total_trades": len(filtered),
        "pct_at_scaled": round(len(trades_at_scaled) / len(filtered) * 100, 1) if filtered else 0,
        "scale_up_count": SCALE_UP_COUNT,
        "breakdown": breakdown,
        "scaling_status": scaling_status,
        "cool_off": cool_off,
        "dip_add": dip,
        "boost_stats": boost,
        "trail_stop": trail,
    }


_SLIM_KEEP = {"crypto", "side", "price", "fill_price", "score", "action", "result",
              "pnl", "contracts", "filled_count", "timestamp", "strategy_version",
              "bet_amount", "would_have_won", "hypothetical_pnl", "market_result",
              "cool_off", "blocked_scale", "fee", "audited", "dip_add", "dip_type", "dip_tier",
              "trail_stopped", "base_contracts"}
_DETAIL_KEYS = {"reasons", "score_breakdown", "indicators", "entry_minute", "window_end",
                "order_id", "event_ticker", "ticker"}

def _slim_bets(bets, recent=500):
    """Strip heavy fields and filter out noise to reduce payload size."""
    filtered = [b for b in bets if b.get("result") not in ("expired", "unfilled", "dip_expired", "dip_pending")]
    if len(filtered) <= recent:
        return filtered
    slim = []
    for b in filtered[:-recent]:
        slim.append({k: v for k, v in b.items() if k not in _DETAIL_KEYS})
    slim.extend(filtered[-recent:])
    return slim


def _build_trail_compare(bets, max_points=500):
    """Two cumulative CORE P&L curves over the dashboard's trades: with and
    without the intraday trailing stop. 'without' reconstructs the free-scaling
    P&L from each trade's base_contracts (the scale absent the stop); 'with'
    replays the *enabled* stop policy per UTC day — the plain one-way latch, or
    the streak-hold toggle when TRAIL_STREAK_HOLD_K > 0 (lift while on a K-win
    streak, re-fire when it breaks). This mirrors the bot's _trail_replay. Split
    dips are excluded so they can't move either line."""
    K = TRAIL_STREAK_HOLD_K
    FLOOR = TRAIL_LOSS_FLOOR
    RK = TRAIL_FLOOR_REACT_K
    HARD = TRAIL_HARD_FLOOR
    core = [b for b in bets
            if b.get("action") == "trade" and b.get("result") in ("win", "loss")
            and b.get("crypto") in ("BTC", "ETH") and not b.get("dip_add")
            and b.get("timestamp")]
    core.sort(key=lambda b: b.get("timestamp", ""))
    # Count-based protection: the live trades already reflect the count floor
    # (contracts = 1 when floored, else the pressed scale). So 'with' = the actual
    # realized P&L, and 'without' = free-scaling from base_contracts. No re-sim.
    if PROTECT_MODE == "count":
        labels, with_series, without_series = [], [], []
        cum_with = cum_without = 0.0
        for b in core:
            c_rec = b.get("filled_count") or b.get("contracts") or 1
            pnl = b.get("pnl", 0)
            per_contract = pnl / c_rec if c_rec else pnl
            base = b.get("base_contracts") or c_rec
            cum_with += pnl
            cum_without += per_contract * base
            labels.append(b.get("timestamp"))
            with_series.append(round(cum_with, 2))
            without_series.append(round(cum_without, 2))
        n = len(labels)
        if n > max_points:
            step = n / max_points
            idx = sorted(set([int(i * step) for i in range(max_points)] + [n - 1]))
            labels = [labels[i] for i in idx]
            with_series = [with_series[i] for i in idx]
            without_series = [without_series[i] for i in idx]
        return {
            "labels": labels, "with_stop": with_series, "without_stop": without_series,
            "with_total": round(cum_with, 2), "without_total": round(cum_without, 2),
            "delta": round(cum_with - cum_without, 2), "enabled": True, "mode": "count",
            "soft_c": COUNT_SOFT_C, "react_k": COUNT_REACT_K,
            "press_streak_n": PRESS_STREAK_N, "press_mult": PRESS_MULT,
        }
    labels, with_series, without_series = [], [], []
    cum_with = cum_without = 0.0
    cur_day = None
    day_start = 0.0   # with-stop cumulative at the start of the current UTC day
    day_peak = 0.0    # highest with-stop cumulative reached during the current day
    day_stopped = False
    day_floored = False
    day_streak = 0
    for b in core:
        day = b.get("timestamp", "")[:10]
        if day != cur_day:
            cur_day = day
            day_start = cum_with   # the trailing stop resets each UTC day
            day_peak = cum_with
            day_stopped = False
            day_floored = False
            day_hard = False
            day_streak = 0
        # lift the trailing stop / reactivate the soft floor first (streak signals);
        # the hard backstop never reactivates.
        if K > 0 and day_stopped and day_streak >= K:
            day_stopped = False
        if day_floored and RK > 0 and day_streak >= RK:
            day_floored = False
        c_rec = b.get("filled_count") or b.get("contracts") or 1
        pnl = b.get("pnl", 0)
        per_contract = pnl / c_rec if c_rec else pnl
        base = b.get("base_contracts") or c_rec  # intended scale absent the stop
        clipped = day_stopped or day_floored or day_hard
        # without the stop: always free-scaling
        cum_without += per_contract * base
        # with the stop: 1x while clipped
        cum_with += per_contract * (1 if clipped else base)
        if cum_with > day_peak:
            day_peak = cum_with
        day_streak = day_streak + 1 if b.get("result") == "win" else 0
        # Trailing stop: arm on the DAY's own peak gain, fire on giveback,
        # unless a K-win streak is holding it off.
        if (not day_stopped and (K <= 0 or day_streak < K)
                and (day_peak - day_start) >= TRAIL_STOP_ARM
                and (day_peak - cum_with) >= TRAIL_STOP_GIVEBACK):
            day_stopped = True
        # Loss floor: fire once the day is down >= FLOOR from its start.
        if FLOOR > 0 and (cum_with - day_start) <= -FLOOR:
            day_floored = True
        # Hard backstop: latches with no reactivation.
        if HARD > 0 and (cum_with - day_start) <= -HARD:
            day_hard = True
        labels.append(b.get("timestamp"))
        with_series.append(round(cum_with, 2))
        without_series.append(round(cum_without, 2))
    # Downsample for payload size, always keeping the last point
    n = len(labels)
    if n > max_points:
        step = n / max_points
        idx = sorted(set([int(i * step) for i in range(max_points)] + [n - 1]))
        labels = [labels[i] for i in idx]
        with_series = [with_series[i] for i in idx]
        without_series = [without_series[i] for i in idx]
    return {
        "labels": labels,
        "with_stop": with_series,
        "without_stop": without_series,
        "with_total": round(cum_with, 2),
        "without_total": round(cum_without, 2),
        "delta": round(cum_with - cum_without, 2),
        "arm": TRAIL_STOP_ARM,
        "giveback": TRAIL_STOP_GIVEBACK,
        "enabled": TRAIL_STOP_ENABLED,
        "streak_hold_k": K,
        "dynamic": TRAIL_STOP_DYNAMIC,
        "loss_floor": FLOOR,
        "floor_react_k": RK,
        "hard_floor": HARD,
    }


def _build_score_report(bets, status, balance_info=None):
    """Build the score data report from bets — no API calls, instant."""
    # Core strategy excludes dip-add trades (a separate experiment that
    # otherwise pollutes win rate / counts); dips shown in their own tracker
    trades = [b for b in bets if b.get("action") == "trade"
              and b.get("result") != "unfilled" and not b.get("dip_add")]
    skips = [b for b in bets if b.get("action") == "skip"]

    # Dips-inclusive set for the scaling-performance dip tracker only
    resolved_all = [b for b in bets if b.get("action") == "trade"
                    and b.get("result") in ("win", "loss")]
    resolved = [b for b in trades if b.get("result") in ("win", "loss")]
    wins = [b for b in resolved if b["result"] == "win"]
    losses = [b for b in resolved if b["result"] == "loss"]
    # Win rate / counts stay core-only; total P&L includes split-dip P&L so the
    # headline reflects the actual account movement (dips are real money too).
    #
    # PHANTOM trades were recorded but never ordered — they exist only to keep
    # the sizing brain's history intact. Their P&L is imaginary, so the headline
    # must EXCLUDE it or the dashboard reports money that was never made. Win
    # rate and counts still include them, because that is the history the
    # strategy actually reasoned from.
    core_pnl = sum(b.get("pnl", 0) for b in resolved if not b.get("phantom"))
    dip_pnl = sum(b.get("pnl", 0) for b in resolved_all
                  if b.get("dip_add") and not b.get("phantom"))
    total_pnl = core_pnl + dip_pnl
    phantom_res = [b for b in resolved_all if b.get("phantom")]
    phantom_pnl = sum(b.get("pnl", 0) for b in phantom_res)
    phantom_count = len(phantom_res)

    resolved_skips = [b for b in skips if b.get("result") in ("win", "loss")]
    skip_would_won = [b for b in resolved_skips if b.get("would_have_won")]
    skip_would_lost = [b for b in resolved_skips if not b.get("would_have_won")]
    skip_hypothetical_pnl = sum(b.get("hypothetical_pnl", 0) for b in resolved_skips)
    # Losses saved = sum of what we would have lost on skips that were losers (positive number)
    skip_losses_saved = sum(abs(b.get("hypothetical_pnl", 0)) for b in skip_would_lost)
    # Missed gains = sum of what we would have gained on skips that were winners
    skip_missed_gains = sum(b.get("hypothetical_pnl", 0) for b in skip_would_won)

    score_dist = {}
    for b in trades:
        sc = b.get("score", 0)
        score_dist[str(sc)] = score_dist.get(str(sc), 0) + 1

    by_crypto = {}
    for b in trades:
        c = b.get("crypto", "?")
        if c not in by_crypto:
            by_crypto[c] = {"trades": 0, "wins": 0, "losses": 0, "pnl": 0, "skips": 0, "skip_would_won": 0, "skip_would_lost": 0, "skip_hypothetical_pnl": 0}
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
            by_crypto[c] = {"trades": 0, "wins": 0, "losses": 0, "pnl": 0, "skips": 0, "skip_would_won": 0, "skip_would_lost": 0, "skip_hypothetical_pnl": 0}
        by_crypto[c]["skips"] += 1
        if b.get("would_have_won") is not None:
            if b["would_have_won"]:
                by_crypto[c]["skip_would_won"] += 1
            else:
                by_crypto[c]["skip_would_lost"] += 1
            by_crypto[c]["skip_hypothetical_pnl"] += b.get("hypothetical_pnl", 0)

    # ── Monthly P&L ────────────────────────────────────────────────────
    # Split by book, because the three do not move together: core is a sizing
    # strategy, split dips are a handful of big days, core dips are a flat
    # 1-contract probe. A single monthly number hides which one changed.
    # Phantom P&L is carried in its own column and never added to the total --
    # same rule as the headline, since that money was never ordered.
    _months = {}
    def _m(ts):
        return (ts or "")[:7]
    for b in resolved_all:
        k = _m(b.get("timestamp"))
        if not k:
            continue
        e = _months.setdefault(k, {"month": k, "core_pnl": 0.0, "split_dip_pnl": 0.0,
                                   "core_dip_pnl": 0.0, "phantom_pnl": 0.0,
                                   "core_fills": 0, "core_wins": 0, "dip_fills": 0,
                                   "dip_wins": 0, "days": set()})
        e["days"].add((b.get("timestamp") or "")[:10])
        won = b.get("result") == "win"
        pnl = b.get("pnl", 0)
        if b.get("phantom"):
            e["phantom_pnl"] += pnl
        if b.get("dip_add"):
            e["dip_fills"] += 1
            e["dip_wins"] += 1 if won else 0
            if not b.get("phantom"):
                if b.get("dip_type") == "core":
                    e["core_dip_pnl"] += pnl
                else:
                    e["split_dip_pnl"] += pnl
        else:
            e["core_fills"] += 1
            e["core_wins"] += 1 if won else 0
            if not b.get("phantom"):
                e["core_pnl"] += pnl
    monthly = []
    for e in sorted(_months.values(), key=lambda x: x["month"], reverse=True):
        tot = e["core_pnl"] + e["split_dip_pnl"] + e["core_dip_pnl"]
        monthly.append({
            "month": e["month"], "days": len(e["days"]),
            "core_pnl": round(e["core_pnl"], 2),
            "split_dip_pnl": round(e["split_dip_pnl"], 2),
            "core_dip_pnl": round(e["core_dip_pnl"], 2),
            "phantom_pnl": round(e["phantom_pnl"], 2),
            "total_pnl": round(tot, 2),
            "per_day": round(tot / len(e["days"]), 2) if e["days"] else 0,
            "core_fills": e["core_fills"], "core_wins": e["core_wins"],
            "core_wr": round(e["core_wins"] / e["core_fills"] * 100, 1) if e["core_fills"] else 0,
            "dip_fills": e["dip_fills"], "dip_wins": e["dip_wins"],
            "dip_wr": round(e["dip_wins"] / e["dip_fills"] * 100, 1) if e["dip_fills"] else 0,
        })

    # ── Scaling performance breakdown ──────────────────────────────────
    scaling_perf = _build_scaling_performance(resolved_all, status)
    trail_compare = _build_trail_compare(bets)

    return {
        "monthly": monthly,
        "total_trades": len(trades),
        "total_skips": len(skips),
        "resolved": len(resolved),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(len(wins) / len(resolved) * 100, 1) if resolved else 0,
        "total_pnl": round(total_pnl, 2),
        "core_pnl": round(core_pnl, 2),
        "dip_pnl": round(dip_pnl, 2),
        # Imaginary P&L from recorded-but-not-ordered trades. Never folded into
        # total_pnl; surfaced so the cost of phantom mode stays visible.
        "phantom_pnl": round(phantom_pnl, 2),
        "phantom_count": phantom_count,
        "skip_resolved": len(resolved_skips),
        "skip_would_won": len(skip_would_won),
        "skip_would_lost": len(skip_would_lost),
        "skip_hypothetical_pnl": round(skip_hypothetical_pnl, 2),
        "skip_losses_saved": round(skip_losses_saved, 2),
        "skip_missed_gains": round(skip_missed_gains, 2),
        "score_distribution": score_dist,
        "by_crypto": by_crypto,
        "scaling_performance": scaling_perf,
        "trail_compare": trail_compare,
        "indicators": status.get("indicators", {}),
        "last_indicator_update": status.get("last_update", ""),
        "recent_bets": _slim_bets(bets),
        "strategy_version": os.environ.get("SCORE_VERSION", "v4"),
        "balance": (balance_info or {}).get("balance", 0),
        "portfolio_value": (balance_info or {}).get("portfolio_value", 0),
        "reset_baseline": _load_reset_baseline(),
    }


def _load_reset_baseline():
    try:
        with open(os.path.join(SCORE_DATA_DIR, ".reset_baseline")) as f:
            return json.load(f)
    except Exception:
        return None


@app.get("/api/audit-pnl")
def audit_pnl(limit: int = 150, repair: bool = False):
    """Authoritative fill check via the orders API status field.
    Only orders that are canceled/expired with zero fills are genuine
    phantoms; with repair=true those are marked unfilled (P&L zeroed).
    Fees are handled separately by the live resolve loop's estimate."""
    try:
        with open(SCORE_BETS_FILE) as f:
            bets = json.load(f)
        resolved = [b for b in bets if b.get("action") == "trade" and b.get("order_id")
                    and b.get("result") in ("win", "loss")]
        sample = resolved[-min(limit, 400):]
        if not sample:
            return JSONResponse({"checked": 0, "note": "no resolved trades with order_id"})

        from collections import Counter
        status_counts = Counter()
        phantoms, not_found = [], []
        checked = 0
        for b in sample:
            try:
                od = auth_get(f"/portfolio/orders/{b['order_id']}").get("order", {})
            except Exception as e:
                if "404" in str(e):
                    status_counts["not_found"] += 1
                    not_found.append(b)
                else:
                    status_counts["error"] += 1
                time.sleep(0.05)
                continue
            checked += 1
            status = od.get("status", "unknown")
            status_counts[status] += 1
            # Genuine phantom: order resolved without executing
            remaining = od.get("remaining_count")
            count = od.get("count")
            never_filled = status in ("canceled", "expired")
            if count is not None and remaining is not None:
                never_filled = never_filled and (int(count) - int(remaining) == 0)
            if never_filled:
                phantoms.append(b)
                if repair:
                    b["result"] = "unfilled"
                    b["pnl"] = 0
                    b["filled_count"] = 0
                    b["audited"] = True
            time.sleep(0.05)

        if repair and phantoms:
            with open(SCORE_BETS_FILE, "w") as f:
                json.dump(bets, f)
            with _score_lock:
                _score_cache["result"] = None

        phantom_pnl = round(sum(b.get("pnl", 0) for b in phantoms), 2)
        return JSONResponse({
            "checked": checked,
            "status_distribution": dict(status_counts),
            "genuine_phantoms": len(phantoms),
            "phantom_pnl_removed": phantom_pnl if repair else None,
            "phantom_pnl_if_repaired": phantom_pnl,
            "not_found_orders": len(not_found),
            "phantom_samples": [{"ts": b.get("timestamp", "")[:19], "crypto": b.get("crypto"),
                                 "pnl": b.get("pnl"), "c": b.get("contracts")} for b in phantoms[:15]],
            "repaired": bool(repair and phantoms),
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/check-dips")
def check_dips(repair: bool = False):
    """Verify dip trades against Kalshi's actual fills feed. Dips whose order_id
    never appears in the feed never filled (phantom); repair marks them unfilled."""
    try:
        def _epoch(ts):
            try:
                return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
            except Exception:
                return None
        with open(SCORE_BETS_FILE) as f:
            bets = json.load(f)
        dips = [b for b in bets if b.get("dip_add") and b.get("order_id")
                and b.get("result") in ("win", "loss", "open")]
        if not dips:
            return JSONResponse({"dips_checked": 0, "note": "no resolved dip trades"})
        oldest = min((_epoch(b.get("timestamp")) or 1e18) for b in dips)

        # Page the fills feed back to the oldest dip
        filled_orders = set()
        cursor = None
        for _ in range(60):
            params = {"limit": 200}
            if cursor:
                params["cursor"] = cursor
            resp = auth_get("/portfolio/fills", params=params)
            fl = resp.get("fills", [])
            oldest_f = None
            for f in fl:
                filled_orders.add(f.get("order_id"))
                e = _epoch(f.get("created_time"))
                if e and (oldest_f is None or e < oldest_f):
                    oldest_f = e
            cursor = resp.get("cursor")
            if not fl or not cursor or (oldest_f and oldest_f <= oldest):
                break
            time.sleep(0.2)

        phantom = [b for b in dips if b["order_id"] not in filled_orders]
        real = [b for b in dips if b["order_id"] in filled_orders]
        phantom_pnl = round(sum(b.get("pnl", 0) for b in phantom), 2)
        if repair:
            for b in phantom:
                b["result"] = "dip_expired"
                b["pnl"] = 0
                b["filled_count"] = 0
            if phantom:
                with open(SCORE_BETS_FILE, "w") as f:
                    json.dump(bets, f)
                with _score_lock:
                    _score_cache["result"] = None
        return JSONResponse({
            "dips_checked": len(dips),
            "real_fills": len(real),
            "phantom_dips": len(phantom),
            "phantom_pnl_removed": phantom_pnl,
            "real_dip_pnl": round(sum(b.get("pnl", 0) for b in real), 2),
            "repaired": bool(repair and phantom),
            "phantom_sample": [{"ts": b.get("timestamp", "")[:16], "crypto": b.get("crypto"),
                                "type": b.get("dip_type"), "pnl_was": b.get("pnl")} for b in phantom[:15]],
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/reconcile")
def reconcile():
    """Compare every Kalshi fill since reset against our recorded order_ids to
    find uncaptured trades (e.g. take-profit sells) that cause account drift."""
    try:
        def _epoch(ts):
            try:
                return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
            except Exception:
                return None
        cutoff = None
        try:
            with open(os.path.join(SCORE_DATA_DIR, ".reset_ts")) as f:
                cutoff = _epoch(f.read().strip())
        except Exception:
            pass

        from collections import defaultdict
        with open(SCORE_BETS_FILE) as f:
            bets = json.load(f)
        # Every order_id we know about: main orders, take-profits, dip orders
        known = set()
        for b in bets:
            for k in ("order_id", "tp_order_id"):
                if b.get(k):
                    known.add(b[k])

        # Page fills feed back to reset
        fills = []
        cursor = None
        for _ in range(60):
            params = {"limit": 200}
            if cursor:
                params["cursor"] = cursor
            resp = auth_get("/portfolio/fills", params=params)
            fl = resp.get("fills", [])
            fills.extend(fl)
            oldest = min((_epoch(f.get("created_time")) or 1e18) for f in fl) if fl else None
            cursor = resp.get("cursor")
            if not fl or not cursor or (cutoff and oldest and oldest <= cutoff):
                break
            time.sleep(0.2)

        # Keep fills after reset
        if cutoff:
            fills = [f for f in fills if (_epoch(f.get("created_time")) or 0) >= cutoff]

        by_order = defaultdict(lambda: {"buy_qty": 0.0, "buy_cost": 0.0,
                                        "sell_qty": 0.0, "sell_proceeds": 0.0, "fees": 0.0, "action": ""})
        for f in fills:
            oid = f.get("order_id")
            q = float(f.get("count_fp") or f.get("count") or 0)
            yp = float(f.get("yes_price_dollars") or 0)
            price = yp  # value fills in YES terms for a consistent cash measure
            o = by_order[oid]
            o["fees"] += float(f.get("fee_cost") or 0)
            if f.get("action") == "sell":
                o["sell_qty"] += q; o["sell_proceeds"] += q * price; o["action"] = "sell"
            else:
                o["buy_qty"] += q; o["buy_cost"] += q * price
                if not o["action"]:
                    o["action"] = "buy"

        uncaptured = []
        unc_cash = 0.0
        for oid, o in by_order.items():
            if oid in known:
                continue
            # cash flow of this uncaptured order: proceeds - cost - fees
            cash = o["sell_proceeds"] - o["buy_cost"] - o["fees"]
            unc_cash += cash
            uncaptured.append({"order_id": oid[:12], "action": o["action"],
                               "buy_qty": round(o["buy_qty"]), "sell_qty": round(o["sell_qty"]),
                               "cash": round(cash, 2)})
        uncaptured.sort(key=lambda x: x["cash"])

        return JSONResponse({
            "fills_since_reset": len(fills),
            "distinct_orders": len(by_order),
            "known_orders_matched": sum(1 for oid in by_order if oid in known),
            "uncaptured_orders": len(uncaptured),
            "uncaptured_net_cash": round(unc_cash, 2),
            "sample": uncaptured[:25],
        })
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/set-reset-baseline")
def set_reset_baseline(value: float):
    """Manually set the account-value baseline (for resets before this feature)."""
    try:
        with open(os.path.join(SCORE_DATA_DIR, ".reset_baseline"), "w") as f:
            json.dump({"ts": datetime.now(timezone.utc).isoformat(),
                       "account_value": round(value, 2), "manual": True}, f)
        return JSONResponse({"status": "ok", "account_value": round(value, 2)})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


def score_resolve_loop():
    """Background thread: resolve open bets every 60s, cache the report."""
    while True:
        try:
            bets = _resolve_score_bets() or []
            status = {}
            if os.path.exists(SCORE_STATUS_FILE):
                try:
                    with open(SCORE_STATUS_FILE) as f:
                        status = json.load(f)
                except Exception:
                    pass
            balance_info = {}
            try:
                balance_info = get_balance() or {}
            except Exception:
                pass
            report = _build_score_report(bets, status, balance_info)
            with _score_lock:
                _score_cache["result"] = report
                _score_cache["last_resolve"] = time.time()
        except Exception as e:
            P(f"  [SCORE-DATA] Resolve error: {e}")
        time.sleep(60)


@app.get("/api/score-data")
def get_score_data():
    """Serve cached score bot report — instant response."""
    with _score_lock:
        result = _score_cache["result"]
    if result:
        return JSONResponse(result)
    # First request before cache is ready — build from disk (no API calls)
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
    return JSONResponse(_build_score_report(bets, status))


@app.get("/api/window-sides")
def api_window_sides(limit: int = 8):
    """Per-window BTC/ETH sides — a few hundred bytes, for the Polymarket bot.

    Deliberately tiny and read-only. /api/score-data is ~5MB, so polling it to
    learn two sides would move half a gigabyte a day and risk stalling the
    caller on a slow response. This reads the bets already on disk and returns
    only what a mirror needs.

    Touches no trading code and writes nothing. If it breaks, or this whole
    service is down, the Polymarket bot falls back to its own split detection --
    the dependency is one-directional and non-essential by design.
    """
    try:
        with open(SCORE_BETS_FILE) as f:
            bets = json.load(f)
    except Exception as e:
        return JSONResponse({"error": str(e), "windows": []}, status_code=200)
    byw = {}
    for b in bets:
        if b.get("action") != "trade" or b.get("dip_add"):
            continue
        if b.get("crypto") not in ("BTC", "ETH"):
            continue
        we = b.get("window_end")
        if not we:
            continue
        byw.setdefault(we, {})[b["crypto"]] = (b.get("side") or "").lower()
    out = []
    for we in sorted(byw, reverse=True)[:max(1, min(limit, 50))]:
        sides = byw[we]
        out.append({
            "window_end": we,
            "sides": sides,
            "split": len(sides) > 1 and len(set(sides.values())) > 1,
        })
    return JSONResponse({"windows": out,
                         "generated": datetime.now(timezone.utc).isoformat()})


@app.get("/api/score-debug")
def score_debug():
    """Debug: show raw status file contents and resolved scale_state."""
    raw = {}
    try:
        if os.path.exists(SCORE_STATUS_FILE):
            with open(SCORE_STATUS_FILE) as f:
                raw = json.load(f)
    except Exception as e:
        raw = {"error": str(e)}
    reset_ts_path = os.path.join(SCORE_DATA_DIR, ".reset_ts")
    reset_ts = None
    try:
        with open(reset_ts_path) as f:
            reset_ts = f.read().strip()
    except Exception:
        pass
    return JSONResponse({
        "status_file_path": SCORE_STATUS_FILE,
        "file_exists": os.path.exists(SCORE_STATUS_FILE),
        "raw_status_keys": list(raw.keys()) if isinstance(raw, dict) else "not_dict",
        "scale_state": raw.get("scale_state") if isinstance(raw, dict) else None,
        "scale_up_at": raw.get("scale_up_at") if isinstance(raw, dict) else None,
        "score_data_dir": SCORE_DATA_DIR,
        "reset_ts": reset_ts,
        "split_dip_enabled": SPLIT_DIP_ENABLED,
        "split_dip_price": SPLIT_DIP_PRICE,
        "split_dip_count": SPLIT_DIP_COUNT,
        "split_dip_tiers": [f"{c}@{p*100:.0f}c" for p, c in SPLIT_DIP_TIERS],
        "trail_stop_enabled": TRAIL_STOP_ENABLED,
        "trail_stop_arm": TRAIL_STOP_ARM,
        "trail_stop_giveback": TRAIL_STOP_GIVEBACK,
        "trail_streak_hold_k": TRAIL_STREAK_HOLD_K,
        "trail_stop_dynamic": TRAIL_STOP_DYNAMIC,
        "trail_loss_floor": TRAIL_LOSS_FLOOR,
        "trail_floor_react_k": TRAIL_FLOOR_REACT_K,
        "trail_hard_floor": TRAIL_HARD_FLOOR,
    })


_backfill_running = False
_backfill_status = {"checked": 0, "updated": 0, "errors": 0, "total": 0, "done": False}

@app.post("/api/score-backfill-prices")
def backfill_fill_prices(force: bool = False):
    """Trigger background backfill of fill_price for all trades."""
    global _backfill_running
    if _backfill_running:
        return JSONResponse({"status": "running", **_backfill_status})

    def _run_backfill():
        global _backfill_running, _backfill_status
        try:
            with open(SCORE_BETS_FILE) as f:
                bets = json.load(f)
            if force:
                for b in bets:
                    b.pop("fill_price_backfilled", None)
            pending = [b for b in bets if b.get("action") == "trade" and b.get("order_id")
                       and b.get("result") in ("win", "loss") and not b.get("fill_price_backfilled")]
            _backfill_status = {"checked": 0, "updated": 0, "errors": 0, "total": len(pending), "done": False}
            for bet in pending:
                _backfill_status["checked"] += 1
                try:
                    order_resp = auth_get(f"/portfolio/orders/{bet['order_id']}")
                    order_data = order_resp.get("order", {})
                    remaining = int(order_data.get("remaining_count", 0))
                    api_filled = int(order_data.get("count", 0)) - remaining
                    avg_p = order_data.get("avg_price", 0)
                    if avg_p and avg_p > 1:
                        avg_p = avg_p / 100
                    if avg_p and bet.get("side") == "no":
                        avg_p = 1.0 - avg_p
                    if avg_p:
                        bet["fill_price"] = avg_p
                        _backfill_status["updated"] += 1
                    if api_filled > 0:
                        bet["filled_count"] = min(api_filled, bet.get("contracts", api_filled))
                    elif bet.get("filled_count", 1) == 1 and bet.get("contracts", 1) > 1:
                        bet["filled_count"] = bet["contracts"]
                        _backfill_status["updated"] += 1
                    bet["fill_price_backfilled"] = True
                    time.sleep(0.2)
                except Exception:
                    _backfill_status["errors"] += 1
                if _backfill_status["checked"] % 100 == 0:
                    with open(SCORE_BETS_FILE, "w") as f:
                        json.dump(bets, f)
            with open(SCORE_BETS_FILE, "w") as f:
                json.dump(bets, f)
            _backfill_status["done"] = True
        finally:
            _backfill_running = False

    _backfill_running = True
    import threading
    threading.Thread(target=_run_backfill, daemon=True).start()
    return JSONResponse({"status": "started", **_backfill_status})

@app.get("/api/score-backfill-status")
def backfill_status():
    return JSONResponse({"running": _backfill_running, **_backfill_status})


@app.post("/api/fix-filled-counts")
def fix_filled_counts():
    """Fix trades where filled_count=1 but contracts>1 (no API calls needed)."""
    try:
        with open(SCORE_BETS_FILE) as f:
            bets = json.load(f)
        fixed = 0
        for b in bets:
            if b.get("action") != "trade" or b.get("result") not in ("win", "loss"):
                continue
            c = b.get("contracts", 1)
            fc = b.get("filled_count", 1)
            if fc == 1 and c > 1:
                b["filled_count"] = c
            elif fc > c:
                # Can't fill more than ordered
                b["filled_count"] = c
            else:
                continue
            fp = b.get("fill_price", b.get("price", 0))
            fc = b["filled_count"]
            fee = _bet_fee(b, fc, fp)
            if b["result"] == "win":
                b["pnl"] = round(fc * (1.0 - fp) - fee, 2)
            else:
                b["pnl"] = round(-fc * fp - fee, 2)
            fixed += 1
        with open(SCORE_BETS_FILE, "w") as f:
            json.dump(bets, f)
        return JSONResponse({"fixed": fixed, "total_trades": len([b for b in bets if b.get("action") == "trade"])})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/score-reset")
def reset_score_data():
    """Clear all score bot trade history and signal bot to reset."""
    try:
        with open(SCORE_BETS_FILE, "w") as f:
            json.dump([], f)
        with open(SCORE_STATUS_FILE, "w") as f:
            json.dump({}, f)
        # Write reset flag so the bot clears its in-memory bets on next load
        reset_flag = os.path.join(SCORE_DATA_DIR, ".reset_flag")
        with open(reset_flag, "w") as f:
            f.write("reset")
        # Persist reset timestamp: bets older than this are dropped on every
        # load, so stale in-memory copies or GitHub backups can't resurrect them
        reset_ts = os.path.join(SCORE_DATA_DIR, ".reset_ts")
        with open(reset_ts, "w") as f:
            f.write(datetime.now(timezone.utc).isoformat())
        # Snapshot account value so the dashboard can reconcile P&L vs balance
        try:
            bal = get_balance() or {}
            with open(os.path.join(SCORE_DATA_DIR, ".reset_baseline"), "w") as f:
                json.dump({
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "account_value": round(bal.get("balance", 0) + bal.get("portfolio_value", 0), 2),
                }, f)
        except Exception:
            pass
        with _score_lock:
            _score_cache["result"] = None
        # Also clear the git repo copy so it doesn't restore on next deploy
        try:
            from crypto_score_bot import git_backup_bets
            git_backup_bets([], force=True)
        except Exception:
            pass
        P("  [SCORE] Trade history reset (flag written, GitHub cleared)")
        return JSONResponse({"status": "ok", "message": "Trade history cleared"})
    except Exception as e:
        return JSONResponse({"status": "error", "message": str(e)}, status_code=500)


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
            is_live = os.environ.get("SCORE_LIVE", "true").lower() == "true"
            P(f"  [SCORE-BOT] Starting crypto score bot... (live={is_live})")
            run_score_bot(live=is_live)
        except Exception as e:
            P(f"  [SCORE-BOT] Crashed: {e}")
            time.sleep(30)


@app.on_event("startup")
def start_threads():
    # Start data refresh thread
    t1 = threading.Thread(target=data_refresh_loop, daemon=True)
    t1.start()
    P("  [SERVER] Data refresh thread started")

    # Crypto 15m bot disabled — score bot only
    # t2 = threading.Thread(target=bot_thread, daemon=True)
    # t2.start()
    # P("  [SERVER] Bot thread started")

    # Start score bot thread
    t3 = threading.Thread(target=score_bot_thread, daemon=True)
    t3.start()
    P("  [SERVER] Score bot thread started")

    # Start score data resolve thread (background outcome checking)
    t4 = threading.Thread(target=score_resolve_loop, daemon=True)
    t4.start()
    P("  [SERVER] Score resolve thread started")

    # Load cached history (no auto-fetch — use /api/history/refresh manually)
    _load_history_cache()
    P("  [SERVER] History cache loaded (manual refresh only via /api/history/refresh)")
