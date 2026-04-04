"""
Kalshi Trading Bot — Kalshi-Validated Multi-Sport Strategy
==========================================================
Scans 15+ sports/esports on Kalshi. Places automated bets at configurable bankroll %.

Strategies (backtested on 2000+ Kalshi games):
  S1: Coin flip favorite (40-60%)  — NBA, NFL, MLB, Soccer, Esports, Tennis
  S2: Coin flip underdog (50-60%)  — CBB, NHL
  S3: UCL underdog (15-40%)        — Champions League upsets
  S4: Bundesliga draw (15-35%)     — Only league where draws are profitable

Sizing: BANKROLL_PCT env var (default 0.3%)
Uses Kalshi API (api.elections.kalshi.com)

Usage:
    python3 kalshi_bot.py              # Scan and show qualifying bets
    python3 kalshi_bot.py --live       # Actually place bets
"""

import os
import sys
import json
import time
import uuid
import re
import base64
import requests
from datetime import datetime, timezone, timedelta

# ── Config ──────────────────────────────────────────────────────────────
API_BASE = "https://api.elections.kalshi.com/trade-api/v2"
DEMO_BASE = "https://demo-api.kalshi.co/trade-api/v2"

KALSHI_KEY_ID = os.environ.get("KALSHI_KEY_ID", "")
KALSHI_PRIVATE_KEY = os.environ.get("KALSHI_PRIVATE_KEY", "")  # PEM content

BANKROLL_PCT = float(os.environ.get("BANKROLL_PCT", "0.003"))  # 0.3% of bankroll per bet
MIN_BET = float(os.environ.get("MIN_BET", "1.00"))            # $1 minimum
MAX_BET = float(os.environ.get("MAX_BET", "500.00"))          # Safety cap

LOG_FILE = "kalshi_bot.log"
BETS_FILE = "kalshi_bets.json"
STATUS_FILE = "kalshi_bot_status.json"

session = requests.Session()

# ── Leagues and strategies ──────────────────────────────────────────────
# Series tickers for each sport
LEAGUES = {
    # US Sports (2-way markets)
    "NBA": "KXNBAGAME",
    "NFL": "KXNFLGAME",
    "MLB": "KXMLBGAME",
    "NHL": "KXNHLGAME",
    "CBB": "KXNCAAMBGAME",
    "WCBB": "KXNCAAWBGAME",
    # Soccer (3-way markets)
    "EPL": "KXEPLGAME",
    "La Liga": "KXLALIGAGAME",
    "Bundesliga": "KXBUNDESLIGAGAME",
    "Serie A": "KXSERIEAGAME",
    "Ligue 1": "KXLIGUE1GAME",
    "EFL Champ": "KXEFLCHAMPIONSHIPGAME",
    "UCL": "KXUCLGAME",
    "Liga MX": "KXLIGAMXGAME",
    # Esports (2-way markets)
    "CS2": "KXCS2GAME",
    "LoL": "KXLOLGAME",
    "Dota 2": "KXDOTA2GAME",
    # Tennis (2-way)
    "ATP": "KXATPGAME",
    "WTA": "KXWTAGAME",
    # Basketball
    "Euroleague": "KXEUROLEAGUEGAME",
    # Cricket (2-way)
    "IPL": "KXIPLGAME",
}

# Strategy: which side to bet in coin flips
COIN_FLIP_UNDERDOG = {"CBB", "WCBB", "NHL"}  # Bet underdog
# Everything else: bet favorite

# 3-way markets (soccer) — have tie option
THREE_WAY = {"EPL", "La Liga", "Bundesliga", "Serie A", "Ligue 1", "EFL Champ", "UCL", "Liga MX"}

STRATEGY_NAMES = {
    "S1": "Coin flip fav",
    "S2": "Coin flip dog",
    "S3": "UCL underdog",
    "S4": "Bundesliga draw",
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
    # Handle escaped newlines from env vars
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
    # Kalshi requires full path (including /trade-api/v2) in signature
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


# ── Balance & Positions ─────────────────────────────────────────────────
def get_balance():
    try:
        data = auth_get("/portfolio/balance")
        balance_cents = data.get("balance", 0)
        portfolio_cents = data.get("portfolio_value", 0)
        return {
            "balance": balance_cents / 100,
            "portfolio_value": portfolio_cents / 100,
        }
    except Exception as e:
        P(f"  ERROR getting balance: {e}")
        return None


def get_existing_positions():
    """Return set of tickers we already have positions in."""
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


# ── Game date filter ──────────────────────────────────────────────────
MONTHS = {'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4, 'MAY': 5, 'JUN': 6,
          'JUL': 7, 'AUG': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DEC': 12}


def get_game_date(ticker):
    """Parse game date from ticker like KXNBAGAME-26MAR25TORLAC-LAC → 2026-03-25."""
    m = re.search(r'-(\d{2})([A-Z]{3})(\d{2})', ticker)
    if m:
        yr = int('20' + m.group(1))
        mon = MONTHS.get(m.group(2), 0)
        day = int(m.group(3))
        if mon > 0:
            try:
                return datetime(yr, mon, day, tzinfo=timezone.utc).date()
            except ValueError:
                pass
    return None


def is_pregame(markets):
    """Return True if game hasn't started yet (game date is today or future).
    Skip games that are already live — we only want pre-game prices."""
    if not markets:
        return False
    ticker = markets[0].get("ticker", "")
    game_date = get_game_date(ticker)
    if not game_date:
        return True  # If we can't parse, allow it (better than skipping everything)
    now_utc = datetime.now(timezone.utc)
    today = now_utc.date()
    if game_date > today:
        return True  # Future game — safe
    if game_date < today:
        return False  # Past game — definitely live or over
    # Same day: only bet if it's before 4pm UTC (11am ET) to avoid live games
    # Most US games start in the evening ET, soccer/esports vary
    return now_utc.hour < 16


# ── Market scanning ─────────────────────────────────────────────────────
def get_open_events(series_ticker):
    """Get currently open events for a series."""
    try:
        data = public_get("/events", params={
            "series_ticker": series_ticker,
            "status": "open",
            "limit": 100,
        })
        return data.get("events", [])
    except Exception as e:
        P(f"    Error fetching events: {e}")
        return []


def get_markets_for_event(event_ticker):
    """Get markets for an event."""
    try:
        data = public_get("/markets", params={
            "event_ticker": event_ticker,
            "limit": 10,
        })
        return data.get("markets", [])
    except Exception as e:
        return []


def get_best_price(ticker):
    """Get best available price from orderbook."""
    try:
        data = public_get(f"/markets/{ticker}/orderbook", params={"depth": 1})
        book = data.get("orderbook_fp", {})
        yes_bids = book.get("yes_dollars", [])
        no_bids = book.get("no_dollars", [])
        # Best yes price = first yes bid
        yes_price = float(yes_bids[0][0]) if yes_bids else None
        no_price = float(no_bids[0][0]) if no_bids else None
        return yes_price, no_price
    except:
        return None, None


def classify_market(league, markets):
    """
    Classify a game and determine if/how to bet.
    Returns list of bet signals: [{ticker, side, price, strategy, label}, ...]
    """
    signals = []

    if league in THREE_WAY:
        # Soccer: 3 markets (home, away, tie)
        if len(markets) != 3:
            return []
        teams = []
        tie_market = None
        for m in markets:
            suffix = m["ticker"].split("-")[-1]
            sub = m.get("yes_sub_title", "").lower()
            if suffix == "TIE" or sub in ("tie", "draw"):
                tie_market = m
            else:
                teams.append(m)

        if len(teams) != 2:
            return []

        # Get current prices
        prices = []
        for t in teams:
            yp = float(t.get("yes_bid_dollars", 0) or 0)
            if yp == 0:
                yp = float(t.get("last_price_dollars", 0) or 0)
            prices.append(yp)

        if not all(p > 0 for p in prices):
            return []

        fav_idx = 0 if prices[0] >= prices[1] else 1
        fav_price = prices[fav_idx]
        dog_price = prices[1 - fav_idx]
        fav_market = teams[fav_idx]
        dog_market = teams[1 - fav_idx]

        # S4: Bundesliga draw
        if league == "Bundesliga" and tie_market:
            tie_price = float(tie_market.get("yes_bid_dollars", 0) or tie_market.get("last_price_dollars", 0) or 0)
            if 0.15 <= tie_price <= 0.35:
                signals.append({
                    "ticker": tie_market["ticker"],
                    "side": "yes",
                    "price": tie_price,
                    "strategy": "S4",
                    "label": "Draw",
                    "event": tie_market.get("event_ticker", ""),
                })

        # S3: UCL underdog
        if league == "UCL" and 0.15 <= dog_price <= 0.40:
            signals.append({
                "ticker": dog_market["ticker"],
                "side": "yes",
                "price": dog_price,
                "strategy": "S3",
                "label": dog_market.get("yes_sub_title", dog_market["ticker"].split("-")[-1]),
                "event": dog_market.get("event_ticker", ""),
            })

        # S1: Soccer coin flip favorite
        if 0.35 <= fav_price <= 0.60:
            signals.append({
                "ticker": fav_market["ticker"],
                "side": "yes",
                "price": fav_price,
                "strategy": "S1",
                "label": fav_market.get("yes_sub_title", fav_market["ticker"].split("-")[-1]),
                "event": fav_market.get("event_ticker", ""),
            })

    else:
        # 2-way market
        if len(markets) != 2:
            return []

        prices = []
        for m in markets:
            yp = float(m.get("yes_bid_dollars", 0) or 0)
            if yp == 0:
                yp = float(m.get("last_price_dollars", 0) or 0)
            prices.append(yp)

        if not all(p > 0 for p in prices):
            return []

        fav_idx = 0 if prices[0] >= prices[1] else 1
        fav_price = prices[fav_idx]
        dog_price = prices[1 - fav_idx]
        fav_market = markets[fav_idx]
        dog_market = markets[1 - fav_idx]

        is_coin_flip = 0.40 <= fav_price <= 0.60

        if not is_coin_flip:
            return []

        if league in COIN_FLIP_UNDERDOG:
            # S2: Bet underdog in CBB/NHL
            if dog_price >= 0.30:
                signals.append({
                    "ticker": dog_market["ticker"],
                    "side": "yes",
                    "price": dog_price,
                    "strategy": "S2",
                    "label": dog_market.get("yes_sub_title", dog_market["ticker"].split("-")[-1]),
                    "event": dog_market.get("event_ticker", ""),
                })
        else:
            # S1: Bet favorite
            signals.append({
                "ticker": fav_market["ticker"],
                "side": "yes",
                "price": fav_price,
                "strategy": "S1",
                "label": fav_market.get("yes_sub_title", fav_market["ticker"].split("-")[-1]),
                "event": fav_market.get("event_ticker", ""),
            })

    return signals


# ── Bet tracking ────────────────────────────────────────────────────────
def load_placed_bets():
    if os.path.exists(BETS_FILE):
        with open(BETS_FILE) as f:
            return json.load(f)
    return []


def save_bet(bet_info):
    bets = load_placed_bets()
    bets.append(bet_info)
    with open(BETS_FILE, "w") as f:
        json.dump(bets, f, indent=2, default=str)


# ── Order placement ─────────────────────────────────────────────────────
def place_order(ticker, side, price_dollars, amount_dollars):
    """Place a limit order on Kalshi. Uses GTC so orders rest until filled."""
    # Buy at the ask price (1-2 cents above bid) to fill immediately
    price_cents = min(99, int(round(price_dollars * 100)) + 2)
    count = max(1, int(amount_dollars / (price_cents / 100)))

    order = {
        "ticker": ticker,
        "action": "buy",
        "side": side,
        "type": "limit",
        "count": count,
        "yes_price": price_cents if side == "yes" else None,
        "no_price": price_cents if side == "no" else None,
        "client_order_id": str(uuid.uuid4()),
    }
    # Remove None values
    order = {k: v for k, v in order.items() if v is not None}

    try:
        P(f"    Placing order: {count} contracts @ {price_cents}c ({side.upper()}) = ~${count * price_cents / 100:.2f}")
        result = auth_post("/portfolio/orders", data=order)
        order_data = result.get("order", {})
        status = order_data.get("status", "unknown")
        P(f"    Order status: {status}")
        if status == "canceled":
            P(f"    Order was canceled (no liquidity)")
            return None
        return result
    except Exception as e:
        P(f"    ORDER FAILED: {e}")
        return None


# ── Main scan ───────────────────────────────────────────────────────────
def scan_markets(live=False):
    P("=" * 65)
    P("  KALSHI BOT — MULTI-SPORT STRATEGY")
    P(f"  Mode: {'LIVE' if live else 'DRY RUN'} | Sizing: {BANKROLL_PCT*100:.1f}% per bet")
    P(f"  Leagues: {len(LEAGUES)}")
    P("=" * 65)
    P()

    # Get balance
    balance_info = get_balance()
    if balance_info:
        bankroll = balance_info["balance"]
        P(f"  Balance: ${bankroll:.2f} | Portfolio: ${balance_info['portfolio_value']:.2f}")
    else:
        bankroll = 0
        P("  Could not fetch balance — scan-only mode")

    bet_size = max(MIN_BET, min(bankroll * BANKROLL_PCT, MAX_BET)) if bankroll > 0 else 0
    P(f"  Bet size: ${bet_size:.2f} ({BANKROLL_PCT*100:.1f}%)")
    P()

    # Get existing positions, open orders, and placed bets
    existing_positions = get_existing_positions()
    open_orders = get_open_orders()
    placed_bets = load_placed_bets()
    placed_tickers = {b["ticker"] for b in placed_bets}
    # Also track events we've already bet on to avoid betting both sides
    placed_events = {b.get("event", b.get("event_ticker", "")) for b in placed_bets if b.get("event") or b.get("event_ticker")}
    skip_tickers = existing_positions | placed_tickers | open_orders
    if open_orders:
        P(f"  Skipping {len(open_orders)} tickers with resting orders")

    qualifying = []

    for league, series in LEAGUES.items():
        P(f"  Scanning {league}...")
        events = get_open_events(series)
        P(f"    {len(events)} open events")
        time.sleep(0.2)  # Rate limit between league scans

        for event in events:
            event_ticker = event.get("event_ticker", "")
            title = event.get("title", "Unknown")
            markets = get_markets_for_event(event_ticker)

            # Skip live/in-progress games — only bet pre-game
            if not is_pregame(markets):
                P(f"    [SKIP] {title[:40]} (game already started)")
                continue

            signals = classify_market(league, markets)

            for sig in signals:
                already = sig["ticker"] in skip_tickers or sig.get("event", "") in placed_events or event_ticker in placed_events
                tag = "[DONE]" if already else "[SIGNAL]"
                P(f"    {tag} [{STRATEGY_NAMES[sig['strategy']]}] {title[:40]:<40} | {sig['label']:<20} @{sig['price']:.0%}")

                qualifying.append({
                    **sig,
                    "title": title,
                    "league": league,
                    "already": already,
                })

            time.sleep(0.05)
        time.sleep(0.1)

    P()
    P(f"  Total signals: {len(qualifying)}")
    for s in sorted(set(q["strategy"] for q in qualifying)):
        count = sum(1 for q in qualifying if q["strategy"] == s)
        P(f"    {STRATEGY_NAMES[s]}: {count}")

    new_signals = [q for q in qualifying if not q["already"]]
    P(f"  New (not yet bet): {len(new_signals)}")
    P()

    if not new_signals:
        P("  No new qualifying markets found.")
        return qualifying

    if live and bankroll > 0:
        P("  PLACING BETS:")
        P("  " + "-" * 60)
        for sig in new_signals:
            if bet_size < MIN_BET:
                P(f"    SKIP (bet too small): ${bet_size:.2f}")
                continue

            # Get fresh price from orderbook
            fresh_yes, fresh_no = get_best_price(sig["ticker"])
            if fresh_yes and sig["side"] == "yes":
                price = fresh_yes
            elif fresh_no and sig["side"] == "no":
                price = fresh_no
            else:
                price = sig["price"]

            P(f"    [{STRATEGY_NAMES[sig['strategy']]}] ${bet_size:.2f} on {sig['label']} @ {price:.0%} | {sig['league']} {sig['title'][:35]}")
            result = place_order(sig["ticker"], sig["side"], price, bet_size)

            if result:
                save_bet({
                    "ticker": sig["ticker"],
                    "event": sig.get("event", ""),
                    "title": sig["title"],
                    "label": sig["label"],
                    "league": sig["league"],
                    "strategy": sig["strategy"],
                    "strategy_name": STRATEGY_NAMES[sig["strategy"]],
                    "side": sig["side"],
                    "price": price,
                    "bet_amount": bet_size,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "order_result": result,
                })
                P(f"    BET PLACED")
                # Track event to prevent betting both sides of same game
                if sig.get("event"):
                    placed_events.add(sig["event"])
            else:
                P(f"    BET FAILED")

            time.sleep(0.3)

        # Refresh balance
        new_bal = get_balance()
        if new_bal:
            P(f"\n  Updated: ${new_bal['balance']:.2f} | Portfolio: ${new_bal['portfolio_value']:.2f}")
    elif live:
        P("  Cannot place bets — no balance available")
    else:
        P("  DRY RUN — use --live to place bets")
        P("  " + "-" * 60)
        for sig in new_signals:
            P(f"    [{STRATEGY_NAMES[sig['strategy']]}] WOULD BET ${bet_size:.2f} on {sig['label']:<20} @{sig['price']:.0%} | {sig['league']} {sig['title'][:35]}")

    return qualifying


# ── Main ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    live = "--live" in sys.argv

    if not KALSHI_KEY_ID or not KALSHI_PRIVATE_KEY:
        P("  WARNING: No Kalshi API credentials set")
        P("  Set KALSHI_KEY_ID and KALSHI_PRIVATE_KEY environment variables")
        P("  Running in scan-only mode (public data only)")
        P()

    scan_markets(live=live)

    # Save bot status
    status = {
        "last_run": datetime.now(timezone.utc).isoformat(),
        "mode": "LIVE" if live else "DRY RUN",
        "bets_placed": len(load_placed_bets()),
    }
    with open(STATUS_FILE, "w") as f:
        json.dump(status, f, indent=2)

    _log.close()
