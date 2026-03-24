"""
Kalshi 15-Minute Crypto Bot
============================
Trades 5 cryptos (BTC, ETH, SOL, XRP, DOGE) on Kalshi's 15-min prediction markets.

Strategy: "Late Entry Dominant Side"
  - Enter in the last N minutes of each 15-min window
  - Buy the dominant side (whichever is trading at 96c+)
  - Earlier entry = cheaper price = more profit per win
  - Backtested at 99.5%+ win rate across 2,000+ events

Optimal entry windows (from backtest):
  BTC:  10 min — 100% WR, $1.33/day at $0.10/bet
  ETH:   3 min — 99.4% WR, $0.40/day
  SOL:   2 min — 99.4% WR, $0.36/day
  XRP:   4 min — 100% WR, $0.60/day
  DOGE:  3 min — 100% WR, $0.72/day

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

BET_AMOUNT = 0.10            # $0.10 per bet (testing)
MIN_PRICE = 0.96             # Only enter at 96c+
PRICE_BUMP_CENTS = 2         # Buy 2c above to fill at ask

LOG_FILE = "crypto_15m_bot.log"
BETS_FILE = "crypto_15m_bets.json"
STATUS_FILE = "crypto_15m_status.json"

session = requests.Session()

# ── Crypto configs ──────────────────────────────────────────────────────
CRYPTOS = {
    "BTC":  {"series": "KXBTC15M",  "entry_window": 10},
    "ETH":  {"series": "KXETH15M",  "entry_window": 3},
    "SOL":  {"series": "KXSOL15M",  "entry_window": 2},
    "XRP":  {"series": "KXXRP15M",  "entry_window": 4},
    "DOGE": {"series": "KXDOGE15M", "entry_window": 3},
}

# ── Logging ─────────────────────────────────────────────────────────────
_log = open(LOG_FILE, "a")


def P(msg=""):
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


# ── Order placement ─────────────────────────────────────────────────────
def place_order(ticker, side, price_dollars, amount_dollars):
    price_cents = min(99, int(round(price_dollars * 100)) + PRICE_BUMP_CENTS)
    count = max(1, int(amount_dollars / (price_cents / 100)))

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
    # Windows are at :00, :15, :30, :45
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

        # Find the event whose strike is soonest (the current window)
        best_event = None
        best_delta = timedelta(days=999)

        for ev in events:
            strike_str = ev.get("strike_date", "")
            if not strike_str:
                continue
            strike = datetime.fromisoformat(strike_str.replace("Z", "+00:00"))
            delta = strike - now
            # Must be in the future (not yet settled) and within ~16 min
            if timedelta(0) < delta < timedelta(minutes=16) and delta < best_delta:
                best_delta = delta
                best_event = ev

        if not best_event:
            return None, None

        # Get the market ticker
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
            # Fall back to orderbook
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

        # Use most recent trade
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


# ── Trade a single crypto ───────────────────────────────────────────────
def trade_crypto(crypto, cfg, existing, bets, live):
    """Attempt to place a bet for one crypto. Returns bet record or None."""
    series = cfg["series"]
    entry_window = cfg["entry_window"]
    window_start, window_end = get_current_window()

    P(f"\n  --- {crypto} (entry window: last {entry_window} min) ---")

    market, event = find_current_market(series)
    if not market:
        P(f"    No open market found")
        return None

    ticker = market["ticker"]
    event_ticker = event["event_ticker"]
    P(f"    Market: {ticker}")
    P(f"    Event: {event.get('title', event_ticker)}")

    if ticker in existing:
        P(f"    Already have position — skipping")
        return None

    already_bet = any(b.get("ticker") == ticker for b in bets)
    if already_bet:
        P(f"    Already bet this market — skipping")
        return None

    side, price = get_dominant_side(ticker)
    if not side or not price:
        P(f"    Could not determine dominant side")
        return None

    P(f"    Dominant: {side.upper()} @ {price:.4f}")

    if price < MIN_PRICE:
        P(f"    Price {price:.4f} < {MIN_PRICE} threshold — skipping")
        return None

    bet_record = {
        "crypto": crypto,
        "ticker": ticker,
        "event_ticker": event_ticker,
        "side": side,
        "price": price,
        "bet_amount": BET_AMOUNT,
        "entry_window": entry_window,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "window_end": window_end.isoformat(),
        "result": "open",
    }

    if live:
        result = place_order(ticker, side, price, BET_AMOUNT)
        if result:
            order = result.get("order", {})
            bet_record["order_id"] = order.get("order_id", "")
            bet_record["status"] = order.get("status", "")
            bet_record["fill_price"] = order.get("avg_price", price)
            P(f"    BET PLACED: {side.upper()} @ {price:.4f}")
            return bet_record
        else:
            P(f"    Order failed — not recorded")
            return None
    else:
        P(f"    [DRY RUN] Would bet {side.upper()} @ {price:.4f}")
        bet_record["status"] = "dry_run"
        return bet_record


# ── Main ────────────────────────────────────────────────────────────────
def run(live=False):
    P("=" * 65)
    P("  CRYPTO 15-MIN BOT — Late Entry Dominant Side")
    P(f"  Mode: {'LIVE' if live else 'DRY RUN'} | Bet: ${BET_AMOUNT:.2f}/trade")
    P(f"  Cryptos: {', '.join(CRYPTOS.keys())}")
    P("=" * 65)

    window_start, window_end = get_current_window()
    P(f"  Current window: {window_start.strftime('%H:%M')} - {window_end.strftime('%H:%M')} UTC")
    P(f"  Minutes until strike: {minutes_until_strike():.1f}")

    # Check balance
    bal = get_balance()
    if bal:
        P(f"  Balance: ${bal['balance']:.2f} | Portfolio: ${bal['portfolio_value']:.2f}")
    else:
        P("  WARNING: Could not fetch balance")

    # Get existing positions to avoid duplicates
    existing = get_existing_positions()
    P(f"  Existing positions: {len(existing)}")

    bets = load_bets()
    new_bets = 0

    # Sort cryptos by entry window: largest first (BTC 10min first, SOL 2min last)
    sorted_cryptos = sorted(CRYPTOS.items(), key=lambda x: -x[1]["entry_window"])

    # Schedule: wait until each crypto's entry window, then trade
    # Bot starts ~10 min before strike. Each crypto enters at its optimal time.
    #   BTC  (10 min): enter immediately
    #   XRP  ( 4 min): wait until 4 min before strike
    #   ETH  ( 3 min): wait until 3 min before strike
    #   DOGE ( 3 min): wait until 3 min before strike
    #   SOL  ( 2 min): wait until 2 min before strike

    for crypto, cfg in sorted_cryptos:
        entry_window = cfg["entry_window"]

        # Wait until we're within this crypto's entry window
        mins_left = minutes_until_strike()
        if mins_left > entry_window:
            wait_secs = (mins_left - entry_window) * 60
            # Add 5 second buffer so we're clearly inside the window
            wait_secs = max(0, wait_secs + 5)
            P(f"\n  Waiting {wait_secs:.0f}s for {crypto} entry window ({entry_window} min before strike)...")
            time.sleep(wait_secs)

        # Now we're in the entry window — trade
        mins_left = minutes_until_strike()
        P(f"  {crypto}: {mins_left:.1f} min left (window: {entry_window} min)")

        if mins_left < 0.5:
            P(f"  Too close to strike (<30s) — skipping {crypto}")
            continue

        bet = trade_crypto(crypto, cfg, existing, bets, live)
        if bet:
            bets.append(bet)
            new_bets += 1

        time.sleep(0.2)

    # Save bets
    save_bets(bets)

    # Save status
    status = {
        "last_run": datetime.now(timezone.utc).isoformat(),
        "mode": "live" if live else "dry_run",
        "window": f"{window_start.strftime('%H:%M')}-{window_end.strftime('%H:%M')} UTC",
        "new_bets": new_bets,
        "total_bets": len(bets),
        "balance": bal["balance"] if bal else None,
    }
    with open(STATUS_FILE, "w") as f:
        json.dump(status, f, indent=2, default=str)

    P(f"\n  {'='*40}")
    P(f"  Done! New bets: {new_bets}")
    P(f"  Total bets on file: {len(bets)}")
    P(f"  {'='*40}")


if __name__ == "__main__":
    live = "--live" in sys.argv
    run(live=live)
