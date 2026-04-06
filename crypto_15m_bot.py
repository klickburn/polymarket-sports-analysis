"""
Kalshi 15-Minute Crypto Bot
============================
Trades 4 cryptos (ETH, SOL, XRP, DOGE) on Kalshi's 15-min prediction markets.

Strategy: "Late Entry Dominant Side"
  - Wait until 5 minutes into each 15-min window (10 min before strike)
  - Buy the dominant side (whichever is trading at 80c+)
  - Runs continuously, looping across windows

Usage:
    python3 crypto_15m_bot.py              # Dry run
    python3 crypto_15m_bot.py --live       # Place real bets
"""

import os
import sys
import json
import time
import uuid
import base64
import requests
from datetime import datetime, timezone, timedelta

# ── Config ──────────────────────────────────────────────────────────────
API_BASE = "https://api.elections.kalshi.com/trade-api/v2"

KALSHI_KEY_ID = os.environ.get("KALSHI_KEY_ID", "")
KALSHI_PRIVATE_KEY = os.environ.get("KALSHI_PRIVATE_KEY", "")

BET_AMOUNT = float(os.environ.get("BET_AMOUNT", "0.10"))
PRICE_BUMP_CENTS = int(os.environ.get("PRICE_BUMP_CENTS", "2"))
ACCOUNT_NAME = os.environ.get("ACCOUNT_NAME", "Default")

# ── Trading profiles ─────────────────────────────────────────────────────
# Each profile: windows, min/max price, contracts, side mode
# PROFILE_1: dominant side (default: 80-100c during 02:15-04:30,08:00-13:30)
# PROFILE_2: underdog side (default: 0-20c during 00:00-02:15,04:30-08:00,13:30-23:59)
PROFILES = []

p1_windows = os.environ.get("P1_WINDOWS", "02:15-04:30,08:00-13:30")
p1_min = float(os.environ.get("P1_MIN_PRICE", "0.80"))
p1_max = float(os.environ.get("P1_MAX_PRICE", "1.00"))
p1_contracts = int(os.environ.get("P1_CONTRACTS", "1"))
p1_mode = os.environ.get("P1_MODE", "dominant")  # dominant or underdog
if p1_windows:
    PROFILES.append({"name": "P1", "windows": p1_windows, "min": p1_min, "max": p1_max, "contracts": p1_contracts, "mode": p1_mode})

p2_windows = os.environ.get("P2_WINDOWS", "00:00-02:15,04:30-08:00,13:30-23:59")
p2_min = float(os.environ.get("P2_MIN_PRICE", "0.00"))
p2_max = float(os.environ.get("P2_MAX_PRICE", "0.20"))
p2_contracts = int(os.environ.get("P2_CONTRACTS", "1"))
p2_mode = os.environ.get("P2_MODE", "underdog")
if p2_windows:
    PROFILES.append({"name": "P2", "windows": p2_windows, "min": p2_min, "max": p2_max, "contracts": p2_contracts, "mode": p2_mode})

# Legacy env vars (ignored if profiles are set)
MIN_PRICE = float(os.environ.get("MIN_PRICE", "0.80"))
MAX_PRICE = float(os.environ.get("MAX_PRICE", "1.00"))
CONTRACT_COUNT = int(os.environ.get("CONTRACT_COUNT", "1"))

LOG_FILE = "crypto_15m_bot.log"
BETS_FILE = "crypto_15m_bets.json"
STATUS_FILE = "crypto_15m_status.json"

session = requests.Session()

# ── Crypto configs ──────────────────────────────────────────────────────
CRYPTOS = {
    "ETH":  {"series": "KXETH15M"},
    "SOL":  {"series": "KXSOL15M"},
    "XRP":  {"series": "KXXRP15M"},
    "DOGE": {"series": "KXDOGE15M"},
}

POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "5"))
ENTRY_AFTER_MINUTES = int(os.environ.get("ENTRY_AFTER_MINUTES", "10"))


# ── Trading schedule ─────────────────────────────────────────────────────
def _parse_trading_windows(windows_str):
    """Parse 'HH:MM-HH:MM,HH:MM-HH:MM' into list of (start_minutes, end_minutes)."""
    windows = []
    for w in windows_str.split(","):
        w = w.strip()
        if not w or ":" not in w:
            continue
        try:
            start_str, end_str = w.split("-")
            sh, sm = map(int, start_str.split(":"))
            eh, em = map(int, end_str.split(":"))
            windows.append((sh * 60 + sm, eh * 60 + em))
        except (ValueError, AttributeError):
            continue
    return windows


# Parse windows for each profile; drop profiles with no valid windows
for prof in PROFILES:
    prof["_parsed"] = _parse_trading_windows(prof["windows"])
PROFILES = [p for p in PROFILES if p["_parsed"]]


SKIP_DAYS = set(int(d) for d in os.environ.get("SKIP_DAYS", "4,5").split(",") if d.strip().isdigit())
# Default: 4=Friday, 5=Saturday (Python weekday: 0=Mon, 6=Sun)


def get_active_profile():
    """Return the currently active trading profile based on UTC time, or None."""
    now = datetime.now(timezone.utc)
    if now.weekday() in SKIP_DAYS:
        return None
    now_minutes = now.hour * 60 + now.minute
    for prof in PROFILES:
        for start, end in prof["_parsed"]:
            if start <= now_minutes < end:
                return prof
    return None


# ── Logging ─────────────────────────────────────────────────────────────
_log = None


def P(msg=""):
    global _log
    if _log is None:
        _log = open(LOG_FILE, "a")
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    _log.write(line + "\n")
    _log.flush()


# ── Auth (RSA-PSS) ─────────────────────────────────────────────────────
_private_key = None


def get_private_key():
    global _private_key
    if _private_key:
        return _private_key
    from cryptography.hazmat.primitives import serialization
    key_pem = KALSHI_PRIVATE_KEY.strip()
    if not key_pem:
        return None
    key_pem = key_pem.replace("\\n", "\n")
    _private_key = serialization.load_pem_private_key(key_pem.encode(), password=None)
    return _private_key


def sign_request(method, path, timestamp_ms):
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import padding
    pk = get_private_key()
    if not pk:
        return None
    message = f"{timestamp_ms}{method}{path}".encode("utf-8")
    signature = pk.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256()
    )
    return base64.b64encode(signature).decode("utf-8")


def auth_headers(method, path):
    timestamp_ms = str(int(time.time() * 1000))
    sig = sign_request(method, path, timestamp_ms)
    if not sig:
        return {}
    return {
        "KALSHI-ACCESS-KEY": KALSHI_KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": sig,
        "KALSHI-ACCESS-TIMESTAMP": timestamp_ms,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def auth_get(path, params=None):
    url = f"{API_BASE}{path}"
    full_path = f"/trade-api/v2{path}"
    headers = auth_headers("GET", full_path)
    r = session.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def auth_post(path, data=None):
    url = f"{API_BASE}{path}"
    full_path = f"/trade-api/v2{path}"
    headers = auth_headers("POST", full_path)
    r = session.post(url, headers=headers, json=data, timeout=30)
    r.raise_for_status()
    return r.json()


def public_get(path, params=None):
    url = f"{API_BASE}{path}"
    r = session.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


# ── Balance ─────────────────────────────────────────────────────────────
def get_balance():
    try:
        data = auth_get("/portfolio/balance")
        return {
            "balance": data.get("balance", 0) / 100,
            "portfolio_value": data.get("portfolio_value", 0) / 100,
        }
    except Exception as e:
        P(f"  ERROR getting balance: {e}")
        return None


def get_existing_positions():
    try:
        positions = set()
        cursor = None
        while True:
            params = {"limit": 200}
            if cursor:
                params["cursor"] = cursor
            data = auth_get("/portfolio/positions", params=params)
            for pos in data.get("market_positions", []):
                fp = float(pos.get("position_fp", 0))
                if fp != 0:
                    positions.add(pos["ticker"])
            cursor = data.get("cursor")
            if not cursor:
                break
        return positions
    except Exception as e:
        P(f"  WARNING: Could not fetch positions: {e}")
        return set()


def get_open_orders():
    """Get tickers with resting/open orders on Kalshi to prevent duplicates."""
    try:
        tickers = set()
        cursor = None
        while True:
            params = {"limit": 200, "status": "resting"}
            if cursor:
                params["cursor"] = cursor
            data = auth_get("/portfolio/orders", params=params)
            for order in data.get("orders", []):
                tickers.add(order.get("ticker", ""))
            cursor = data.get("cursor")
            if not cursor or not data.get("orders"):
                break
        return tickers
    except Exception as e:
        P(f"  WARNING: Could not fetch open orders: {e}")
        return set()


# ── Order placement ─────────────────────────────────────────────────────
def place_order(ticker, side, price_dollars, amount_dollars, count=None):
    price_cents = min(99, int(round(price_dollars * 100)) + PRICE_BUMP_CENTS)
    count = count or CONTRACT_COUNT

    order = {
        "ticker": ticker,
        "action": "buy",
        "side": side,
        "type": "limit",
        "count": count,
        "client_order_id": str(uuid.uuid4()),
    }
    if side == "yes":
        order["yes_price"] = price_cents
    else:
        order["no_price"] = price_cents

    try:
        P(f"    Placing: {count} contracts @ {price_cents}c ({side.upper()}) = ~${count * price_cents / 100:.2f}")
        result = auth_post("/portfolio/orders", data=order)
        order_data = result.get("order", {})
        status = order_data.get("status", "unknown")
        P(f"    Order status: {status}")
        if status == "canceled":
            P(f"    Order canceled (no liquidity)")
            return None
        return result
    except Exception as e:
        P(f"    ORDER FAILED: {e}")
        return None


# ── Find current 15-min window ──────────────────────────────────────────
def get_current_window():
    """Return the current 15-min window's start and end (strike) time."""
    now = datetime.now(timezone.utc)
    minute_slot = (now.minute // 15) * 15
    window_start = now.replace(minute=minute_slot, second=0, microsecond=0)
    window_end = window_start + timedelta(minutes=15)
    return window_start, window_end


def minutes_until_strike():
    """Minutes remaining in the current 15-min window."""
    now = datetime.now(timezone.utc)
    _, window_end = get_current_window()
    delta = (window_end - now).total_seconds() / 60
    return delta


# ── Find tradeable market ───────────────────────────────────────────────
def find_current_market(series_ticker):
    """Find the currently open market for a crypto series."""
    try:
        data = public_get("/events", params={
            "series_ticker": series_ticker,
            "status": "open",
            "limit": 5,
        })
        events = data.get("events", [])
        if not events:
            return None, None

        now = datetime.now(timezone.utc)
        best_event = None
        best_delta = timedelta(days=999)

        for ev in events:
            strike_str = ev.get("strike_date", "")
            if not strike_str:
                continue
            strike = datetime.fromisoformat(strike_str.replace("Z", "+00:00"))
            delta = strike - now
            if timedelta(0) < delta < timedelta(minutes=16) and delta < best_delta:
                best_delta = delta
                best_event = ev

        if not best_event:
            return None, None

        event_ticker = best_event["event_ticker"]
        resp = public_get(f"/events/{event_ticker}")
        markets = resp.get("markets", [])
        if not markets:
            return None, None

        return markets[0], best_event

    except Exception as e:
        P(f"    Error finding market for {series_ticker}: {e}")
        return None, None


def get_dominant_side(ticker):
    """Check recent trades to determine dominant side and price."""
    try:
        now = datetime.now(timezone.utc)
        trades_resp = public_get(f"/markets/trades", params={
            "ticker": ticker,
            "limit": 10,
            "min_ts": int((now - timedelta(minutes=5)).timestamp()),
        })
        trades = trades_resp.get("trades", [])

        if not trades:
            data = public_get(f"/markets/{ticker}/orderbook", params={"depth": 1})
            book = data.get("orderbook_fp", {})
            yes_bids = book.get("yes_dollars", [])
            no_bids = book.get("no_dollars", [])
            if yes_bids:
                yes_price = float(yes_bids[0][0])
                if yes_price >= 0.50:
                    return "yes", yes_price
                else:
                    return "no", 1.0 - yes_price
            if no_bids:
                no_price = float(no_bids[0][0])
                if no_price >= 0.50:
                    return "no", no_price
                else:
                    return "yes", 1.0 - no_price
            return None, None

        last_trade = trades[0]
        yes_price = float(last_trade.get("yes_price_dollars", "0.50"))

        if yes_price >= 0.50:
            return "yes", yes_price
        else:
            return "no", 1.0 - yes_price

    except Exception as e:
        P(f"    Error getting dominant side for {ticker}: {e}")
        return None, None


# ── Load/save bets ──────────────────────────────────────────────────────
def load_bets():
    if os.path.exists(BETS_FILE):
        with open(BETS_FILE) as f:
            return json.load(f)
    return []


def save_bets(bets):
    with open(BETS_FILE, "w") as f:
        json.dump(bets, f, indent=2, default=str)


# ── Main ────────────────────────────────────────────────────────────────
def run(live=False):
    P("=" * 65)
    P("  CRYPTO 15-MIN BOT — Multi-Profile")
    P(f"  Mode: {'LIVE' if live else 'DRY RUN'} | Account: {ACCOUNT_NAME}")
    for prof in PROFILES:
        P(f"  {prof['name']}: {prof['mode']} {prof['min']*100:.0f}-{prof['max']*100:.0f}c | {prof['contracts']}x | {prof['windows']}")
    P(f"  Cryptos: {', '.join(CRYPTOS.keys())}")
    P("=" * 65)

    if live:
        bal = get_balance()
        if bal:
            P(f"  Balance: ${bal['balance']:.2f} | Portfolio: ${bal['portfolio_value']:.2f}")

    bets = load_bets()
    total_new = 0
    last_window_end = None
    placed_this_window = set()
    skip_tickers = set()

    P(f"\n  Running continuously — polling every {POLL_INTERVAL}s...")

    while True:
        try:
            window_start, window_end = get_current_window()
            mins_left = minutes_until_strike()
            mins_in = 15 - mins_left

            # New window? Reset targets and check for existing orders/positions
            if window_end != last_window_end:
                last_window_end = window_end
                placed_this_window = set()
                # Check for resting orders + existing positions to prevent duplicates
                open_order_tickers = get_open_orders()
                existing_positions = get_existing_positions()
                skip_tickers = open_order_tickers | existing_positions
                if skip_tickers:
                    P(f"  Skipping {len(skip_tickers)} tickers with open orders/positions")
                P(f"\n  ── Window {window_start.strftime('%H:%M')}-{window_end.strftime('%H:%M')} UTC ──")

            # Get active profile for current time
            profile = get_active_profile()
            if not profile:
                time.sleep(POLL_INTERVAL)
                continue

            # Too early — wait until entry time into window
            if mins_in < ENTRY_AFTER_MINUTES:
                time.sleep(POLL_INTERVAL)
                continue

            # Check each crypto
            for crypto, cfg in CRYPTOS.items():
                # Track per profile+crypto to allow both profiles to bet same window
                profile_key = f"{profile['name']}:{crypto}"
                if profile_key in placed_this_window:
                    continue

                time.sleep(0.5)
                market, event = find_current_market(cfg["series"])
                if not market:
                    continue
                ticker = market["ticker"]

                # Skip if already have open order, position, or bet on this ticker
                if ticker in skip_tickers:
                    placed_this_window.add(profile_key)
                    continue
                if any(b.get("ticker") == ticker and b.get("profile") == profile["name"] for b in bets):
                    placed_this_window.add(profile_key)
                    continue

                time.sleep(0.5)
                side, price = get_dominant_side(ticker)
                if not side or not price:
                    continue

                # Flip to underdog if profile mode is underdog
                if profile["mode"] == "underdog":
                    side = "no" if side == "yes" else "yes"
                    price = 1.0 - price

                if price < profile["min"] or price > profile["max"]:
                    continue

                P(f"    [{profile['name']}] {crypto}: {side.upper()} @ {price:.4f} ({mins_in:.1f}m in, {mins_left:.1f}m left)")

                bet_record = {
                    "crypto": crypto,
                    "ticker": ticker,
                    "event_ticker": event["event_ticker"],
                    "side": side,
                    "price": price,
                    "bet_amount": BET_AMOUNT,
                    "profile": profile["name"],
                    "mode": profile["mode"],
                    "entry_window": round(mins_left, 1),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "window_end": window_end.isoformat(),
                    "result": "open",
                }

                if live:
                    result = place_order(ticker, side, price, BET_AMOUNT, count=profile["contracts"])
                    if result:
                        order = result.get("order", {})
                        bet_record["order_id"] = order.get("order_id", "")
                        bet_record["status"] = order.get("status", "")
                        bet_record["fill_price"] = order.get("avg_price", price)
                        bets.append(bet_record)
                        save_bets(bets)
                        total_new += 1
                        placed_this_window.add(profile_key)
                        P(f"    [{profile['name']}] {crypto}: BET PLACED ✓ {side.upper()} @ {price:.4f}")
                    else:
                        P(f"    [{profile['name']}] {crypto}: Order failed")
                else:
                    P(f"    [{profile['name']}] {crypto}: [DRY RUN] {side.upper()} @ {price:.4f}")
                    bet_record["status"] = "dry_run"
                    bets.append(bet_record)
                    save_bets(bets)
                    total_new += 1
                    placed_this_window.add(profile_key)

            time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            P("\n  Stopped by user")
            break
        except Exception as e:
            P(f"  ERROR: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(30)

    save_bets(bets)
    P(f"\n  Bot stopped. Total new bets this session: {total_new}")


if __name__ == "__main__":
    live = "--live" in sys.argv
    run(live=live)
