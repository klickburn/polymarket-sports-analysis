"""
Whale Follow Bot — Mirror Polymarket Whale on Kalshi
=====================================================
Monitors a Polymarket whale's trades on 15-minute crypto markets every 5 seconds.
When the whale buys, mirrors the trade on Kalshi. When the whale sells, exits.

Usage:
    python3 whale_follow_bot.py              # Dry run (detect + log, no Kalshi orders)
    python3 whale_follow_bot.py --live       # Live trading
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
KALSHI_API = "https://api.elections.kalshi.com/trade-api/v2"
GAMMA_API = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"

KALSHI_KEY_ID = os.environ.get("KALSHI_KEY_ID", "")
KALSHI_PRIVATE_KEY = os.environ.get("KALSHI_PRIVATE_KEY", "")

WHALE_WALLETS = {
    "0xc1737e2db2d19e0b73a958ecd5d0f279d0e726ee": "Fickle-Spark",
    "0x63ce342161250d705dc0b16df89036c8e5f9ba9a": "Blushing-Fine",
    "0x576b0696fd5a9225d66fd9500fd98f5be10b0cab": "Moral-Roof",
}
# Normalize to lowercase
WHALE_WALLETS = {k.lower(): v for k, v in WHALE_WALLETS.items()}

WHALE_SCALE = 0.0025         # 0.25% of whale's trade size (proportional sizing)
PRICE_BUMP_CENTS = 2         # Buy 2c above to fill at ask
POLL_INTERVAL = 2            # Seconds between Polymarket polls
EVENT_REFRESH = 900          # Refresh active events every 15 min

GITHUB_TOKEN = os.environ.get("GH_TOKEN", "")
GITHUB_REPO = "klickburn/polymarket-sports-analysis"

LOG_FILE = "whale_follow_bot.log"
TRADES_FILE = "whale_trades.json"
STATUS_FILE = "whale_status.json"
SEEN_FILE = "whale_seen_trades.json"

# Polymarket crypto → Kalshi series
CRYPTO_MAP = {
    "btc": {"name": "BTC", "series": "KXBTC15M"},
    "eth": {"name": "ETH", "series": "KXETH15M"},
    "sol": {"name": "SOL", "series": "KXSOL15M"},
    "xrp": {"name": "XRP", "series": "KXXRP15M"},
}

session = requests.Session()
poly_session = requests.Session()
poly_session.headers.update({"Accept": "application/json"})

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


# ── Kalshi Auth (RSA-PSS) ──────────────────────────────────────────────
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
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
        hashes.SHA256(),
    )
    return base64.b64encode(signature).decode("utf-8")


def kalshi_headers(method, path):
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


def kalshi_get(path, params=None):
    url = f"{KALSHI_API}{path}"
    full_path = f"/trade-api/v2{path}"
    headers = kalshi_headers("GET", full_path)
    r = session.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def kalshi_post(path, data=None):
    url = f"{KALSHI_API}{path}"
    full_path = f"/trade-api/v2{path}"
    headers = kalshi_headers("POST", full_path)
    r = session.post(url, headers=headers, json=data, timeout=30)
    r.raise_for_status()
    return r.json()


def kalshi_public(path, params=None):
    url = f"{KALSHI_API}{path}"
    r = session.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


# ── Kalshi Balance & Positions ──────────────────────────────────────────
def get_balance():
    try:
        data = kalshi_get("/portfolio/balance")
        return {
            "balance": data.get("balance", 0) / 100,
            "portfolio_value": data.get("portfolio_value", 0) / 100,
        }
    except Exception as e:
        P(f"  ERROR getting balance: {e}")
        return None


def get_positions_detail():
    """Return dict of {ticker: {side, count}} for all positions."""
    positions = {}
    try:
        cursor = None
        while True:
            params = {"limit": 200}
            if cursor:
                params["cursor"] = cursor
            data = kalshi_get("/portfolio/positions", params=params)
            for pos in data.get("market_positions", []):
                fp = float(pos.get("position_fp", 0))
                if fp != 0:
                    # Determine side from position fields
                    yes_count = int(float(pos.get("yes_number_fp", 0)))
                    no_count = int(float(pos.get("no_number_fp", 0)))
                    if yes_count > 0:
                        positions[pos["ticker"]] = {"side": "yes", "count": yes_count}
                    elif no_count > 0:
                        positions[pos["ticker"]] = {"side": "no", "count": no_count}
            cursor = data.get("cursor")
            if not cursor:
                break
    except Exception as e:
        P(f"  WARNING: Could not fetch positions: {e}")
    return positions


# ── Kalshi Order Placement ──────────────────────────────────────────────
def place_buy(ticker, side, price_dollars, amount_dollars):
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
        P(f"    BUY: {count}x @ {price_cents}c ({side.upper()}) = ~${count * price_cents / 100:.2f}")
        result = kalshi_post("/portfolio/orders", data=order)
        order_data = result.get("order", {})
        status = order_data.get("status", "unknown")
        P(f"    Order status: {status}")
        return result if status != "canceled" else None
    except Exception as e:
        P(f"    BUY FAILED: {e}")
        return None


def place_sell(ticker, side, count):
    """Sell our position — price aggressively to fill."""
    try:
        # Get current price to set sell price
        book = kalshi_public(f"/markets/{ticker}/orderbook", params={"depth": 1})
        orderbook = book.get("orderbook_fp", {})

        # Sell 2c below current price to fill
        if side == "yes":
            yes_bids = orderbook.get("yes_dollars", [])
            price_cents = max(1, int(float(yes_bids[0][0]) * 100) - PRICE_BUMP_CENTS) if yes_bids else 1
        else:
            no_bids = orderbook.get("no_dollars", [])
            price_cents = max(1, int(float(no_bids[0][0]) * 100) - PRICE_BUMP_CENTS) if no_bids else 1

        order = {
            "ticker": ticker,
            "action": "sell",
            "side": side,
            "type": "limit",
            "count": count,
            "client_order_id": str(uuid.uuid4()),
        }
        if side == "yes":
            order["yes_price"] = price_cents
        else:
            order["no_price"] = price_cents

        P(f"    SELL: {count}x @ {price_cents}c ({side.upper()})")
        result = kalshi_post("/portfolio/orders", data=order)
        order_data = result.get("order", {})
        status = order_data.get("status", "unknown")
        P(f"    Sell status: {status}")
        return result
    except Exception as e:
        P(f"    SELL FAILED: {e}")
        return None


# ── Kalshi Market Finder ────────────────────────────────────────────────
# Cache: {series_ticker: {strike_iso: market_dict, ...}}
_kalshi_market_cache = {}
_kalshi_cache_time = 0
KALSHI_CACHE_TTL = 120  # Refresh Kalshi markets every 2 minutes


def refresh_kalshi_markets():
    """Fetch all open Kalshi 15m crypto markets and cache by series + strike time."""
    global _kalshi_market_cache, _kalshi_cache_time
    now = time.time()
    if now - _kalshi_cache_time < KALSHI_CACHE_TTL and _kalshi_market_cache:
        return
    _kalshi_market_cache = {}
    for crypto_key, cfg in CRYPTO_MAP.items():
        series = cfg["series"]
        try:
            data = kalshi_public("/events", params={
                "series_ticker": series,
                "status": "open",
                "limit": 5,
            })
            for ev in data.get("events", []):
                strike_str = ev.get("strike_date", "")
                if not strike_str:
                    continue
                event_ticker = ev["event_ticker"]
                time.sleep(0.2)
                resp = kalshi_public(f"/events/{event_ticker}")
                markets = resp.get("markets", [])
                if markets:
                    if series not in _kalshi_market_cache:
                        _kalshi_market_cache[series] = {}
                    # Normalize strike time for matching
                    strike_norm = strike_str.replace("+00:00", "Z")
                    if not strike_norm.endswith("Z"):
                        strike_norm += "Z"
                    _kalshi_market_cache[series][strike_norm] = markets[0]
        except Exception as e:
            P(f"    Error fetching Kalshi markets for {series}: {e}")
        time.sleep(0.3)
    _kalshi_cache_time = now
    total = sum(len(v) for v in _kalshi_market_cache.values())
    P(f"  Kalshi market cache: {total} markets across {len(_kalshi_market_cache)} series")


def find_kalshi_market_for_window(series_ticker, poly_end_date):
    """Find the Kalshi market that matches a specific Polymarket window end time."""
    refresh_kalshi_markets()
    series_markets = _kalshi_market_cache.get(series_ticker, {})
    # Normalize poly end date
    end_norm = poly_end_date.replace("+00:00", "Z")
    if not end_norm.endswith("Z"):
        end_norm += "Z"
    return series_markets.get(end_norm)


def find_kalshi_market(series_ticker):
    """Find the nearest open Kalshi market for a crypto series (fallback)."""
    refresh_kalshi_markets()
    series_markets = _kalshi_market_cache.get(series_ticker, {})
    now = datetime.now(timezone.utc)
    best = None
    best_delta = timedelta(days=999)
    for strike_str, market in series_markets.items():
        try:
            strike = datetime.fromisoformat(strike_str.replace("Z", "+00:00"))
            delta = strike - now
            if timedelta(0) < delta < timedelta(minutes=16) and delta < best_delta:
                best_delta = delta
                best = market
        except Exception:
            pass
    return best


def get_kalshi_price(ticker, side):
    """Get current price for a side from orderbook."""
    try:
        book = kalshi_public(f"/markets/{ticker}/orderbook", params={"depth": 1})
        orderbook = book.get("orderbook_fp", {})
        if side == "yes":
            asks = orderbook.get("yes_dollars", [])
            return float(asks[0][0]) if asks else None
        else:
            asks = orderbook.get("no_dollars", [])
            return float(asks[0][0]) if asks else None
    except:
        return None


# ── Polymarket Event Discovery ──────────────────────────────────────────
def fetch_active_poly_events():
    """Fetch currently active (open) 15-minute crypto events from Polymarket."""
    events = []
    try:
        for closed_flag in ["false", "true"]:
            params = {
                "tag_slug": "15m",
                "closed": closed_flag,
                "order": "endDate",
                "ascending": "false",
                "limit": 50,
            }
            r = poly_session.get(f"{GAMMA_API}/events", params=params, timeout=20)
            if r.status_code != 200:
                continue
            for ev in r.json():
                slug = (ev.get("slug") or "").lower()
                if "15m" not in slug:
                    continue
                crypto = None
                for sym, cfg in CRYPTO_MAP.items():
                    if sym in slug:
                        crypto = cfg["name"]
                        break
                if crypto:
                    ev["_crypto"] = crypto
                    ev["_series"] = CRYPTO_MAP.get(crypto.lower(), {}).get("series", "")
                    ev["_end_date"] = ev.get("endDate", "")
                    events.append(ev)
    except Exception as e:
        P(f"  Error fetching Poly events: {e}")
    return events


def fetch_event_trades(event_id):
    """Fetch trades for a Polymarket event, return trades from any tracked whale."""
    whale_trades = []
    offset = 0
    while True:
        try:
            r = poly_session.get(f"{DATA_API}/trades", params={
                "eventId": event_id, "limit": 1000, "offset": offset,
            }, timeout=20)
            if r.status_code != 200:
                break
            trades = r.json()
            if not isinstance(trades, list) or not trades:
                break
            for t in trades:
                wallet = t.get("proxyWallet", "").lower()
                if wallet in WHALE_WALLETS:
                    t["_whale_name"] = WHALE_WALLETS[wallet]
                    whale_trades.append(t)
            if len(trades) < 1000:
                break
            offset += 1000
            if offset >= 5000:
                break
        except Exception:
            break
    return whale_trades


# ── Persistence ─────────────────────────────────────────────────────────
def load_json(path, default=None):
    if default is None:
        default = []
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except:
            pass
    return default


def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2, default=str)


def push_to_github(filename, data):
    """Push a JSON file to the GitHub repo so the dashboard can read it."""
    if not GITHUB_TOKEN:
        return
    try:
        api_url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{filename}"
        headers = {
            "Authorization": f"token {GITHUB_TOKEN}",
            "Accept": "application/vnd.github.v3+json",
        }
        # Get current file SHA (needed for updates)
        r = requests.get(api_url, headers=headers, timeout=15)
        sha = r.json().get("sha", "") if r.status_code == 200 else ""

        content = base64.b64encode(json.dumps(data, indent=2, default=str).encode()).decode()
        payload = {
            "message": f"whale-bot: update {filename}",
            "content": content,
            "branch": "main",
        }
        if sha:
            payload["sha"] = sha

        r = requests.put(api_url, headers=headers, json=payload, timeout=15)
        if r.status_code in (200, 201):
            P(f"  Pushed {filename} to GitHub")
        else:
            P(f"  GitHub push failed: {r.status_code}")
    except Exception as e:
        P(f"  GitHub push error: {e}")


# ── Main Bot ────────────────────────────────────────────────────────────
def run(live=False):
    P("=" * 65)
    P("  WHALE FOLLOW BOT — Mirror Polymarket Whale on Kalshi")
    P(f"  Mode: {'LIVE' if live else 'DRY RUN'} | Scale: {WHALE_SCALE*100:.2f}% of whale volume")
    P(f"  Following {len(WHALE_WALLETS)} whales:")
    for wallet, name in WHALE_WALLETS.items():
        P(f"    {name:20s} {wallet[:10]}...{wallet[-4:]}")
    P(f"  Cryptos: {', '.join(c['name'] for c in CRYPTO_MAP.values())}")
    P(f"  Poll: every {POLL_INTERVAL}s")
    P("=" * 65)

    # Check Kalshi balance
    if live:
        bal = get_balance()
        if bal:
            P(f"  Balance: ${bal['balance']:.2f} | Portfolio: ${bal['portfolio_value']:.2f}")
        time.sleep(0.5)

    # Load state
    trades_log = load_json(TRADES_FILE, [])
    seen_ids = set(load_json(SEEN_FILE, []))
    P(f"  Previously seen trades: {len(seen_ids)}")
    P(f"  Previously logged trades: {len(trades_log)}")

    # Track our Kalshi positions for exit signals — keyed by (whale, ticker)
    our_positions = {}  # {(whale_name, kalshi_ticker): {side, count, poly_event_id}}

    # Fetch initial events
    active_events = fetch_active_poly_events()
    P(f"  Active Polymarket events: {len(active_events)}")
    last_event_refresh = time.time()

    # ── WARMUP: mark all existing trades as seen (don't act on history) ──
    warmup = len(seen_ids) == 0
    if warmup:
        P("  WARMUP: Scanning existing trades to avoid replaying history...")
        for ev in active_events:
            event_id = ev.get("id")
            if not event_id:
                continue
            trades = fetch_event_trades(event_id)
            for t in trades:
                trade_id = t.get("transactionHash", f"{t.get('timestamp')}_{t.get('size')}_{t.get('price')}")
                seen_ids.add(trade_id)
        save_json(SEEN_FILE, list(seen_ids))
        P(f"  WARMUP complete: marked {len(seen_ids)} existing trades as seen")

    total_mirrored = 0
    total_exits = 0
    start_time = time.time()
    last_heartbeat = time.time()
    poll_count = 0
    HEARTBEAT_INTERVAL = 60  # Log heartbeat every 60 seconds

    P(f"\n  Polling started — watching for whale trades...")

    while True:
        try:
            # Refresh events periodically
            if time.time() - last_event_refresh > EVENT_REFRESH:
                active_events = fetch_active_poly_events()
                P(f"  Refreshed events: {len(active_events)} active")
                last_event_refresh = time.time()

            poll_count += 1

            # Heartbeat log
            if time.time() - last_heartbeat >= HEARTBEAT_INTERVAL:
                uptime_min = int((time.time() - start_time) / 60)
                P(f"  ♥ Heartbeat: {poll_count} polls, {uptime_min}m uptime, {len(active_events)} events, "
                  f"{total_mirrored} mirrored, {len(seen_ids)} seen")
                last_heartbeat = time.time()

            new_whale_trades = []

            # Only poll events with windows ending in the next 15 minutes
            now_utc = datetime.now(timezone.utc)
            cutoff = now_utc + timedelta(minutes=15)
            relevant_events = []
            for ev in active_events:
                end_str = ev.get("_end_date", "")
                if end_str:
                    try:
                        end_dt = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
                        if now_utc <= end_dt <= cutoff:
                            relevant_events.append(ev)
                    except Exception:
                        relevant_events.append(ev)  # include if can't parse
                else:
                    relevant_events.append(ev)

            for ev in relevant_events:
                event_id = ev.get("id")
                if not event_id:
                    continue

                trades = fetch_event_trades(event_id)
                for t in trades:
                    # Create unique ID from transaction hash or timestamp+size
                    trade_id = t.get("transactionHash", f"{t.get('timestamp')}_{t.get('size')}_{t.get('price')}")
                    if trade_id in seen_ids:
                        continue

                    seen_ids.add(trade_id)
                    t["_crypto"] = ev.get("_crypto", "?")
                    t["_series"] = ev.get("_series", "")
                    t["_event_id"] = event_id
                    t["_end_date"] = ev.get("_end_date", "")
                    new_whale_trades.append(t)

            # Aggregate whale trades by (whale, event) to determine NET direction
            # Each whale gets their own aggregation — we follow each independently
            event_agg = {}  # {(whale_name, event_id): {Up_vol, Down_vol, ...}}
            for t in new_whale_trades:
                whale_name = t.get("_whale_name", "?")
                eid = t["_event_id"]
                key = (whale_name, eid)
                if key not in event_agg:
                    event_agg[key] = {"Up": 0, "Down": 0, "crypto": t["_crypto"],
                                      "series": t["_series"], "whale": whale_name,
                                      "buys": [], "sells": []}
                action = t.get("side", "")
                outcome = t.get("outcome", "")
                size = float(t.get("size", 0))
                price_t = float(t.get("price", 0))
                dollar_vol = size * price_t
                if action == "BUY" and outcome in ("Up", "Down"):
                    event_agg[key][outcome] += dollar_vol
                    event_agg[key]["buys"].append(t)
                elif action == "SELL":
                    event_agg[key]["sells"].append(t)

            # Build one trade signal per (whale, event) based on net direction
            aggregated_trades = []
            for key, agg in event_agg.items():
                # Handle sells
                for t in agg["sells"]:
                    aggregated_trades.append(t)
                # Determine net buy direction
                if agg["Up"] > 0 or agg["Down"] > 0:
                    if agg["Up"] > agg["Down"]:
                        net_side = "Up"
                        net_vol = agg["Up"]
                    else:
                        net_side = "Down"
                        net_vol = agg["Down"]
                    # Use the largest trade on the winning side as representative
                    best = max([t for t in agg["buys"] if t.get("outcome") == net_side],
                               key=lambda x: float(x.get("size", 0)), default=None)
                    if best:
                        best["_net_side"] = net_side
                        best["_net_vol"] = net_vol
                        aggregated_trades.append(best)

            # Process aggregated trades
            for t in aggregated_trades:
                crypto = t["_crypto"]
                series = t["_series"]
                poly_end_date = t.get("_end_date", "")
                whale_name = t.get("_whale_name", "?")
                action = t.get("side", "")  # BUY or SELL
                outcome = t.get("outcome", "")  # Up or Down
                price = float(t.get("price", 0))
                size = float(t.get("size", 0))
                ts = t.get("timestamp", 0)

                # Use net direction if available (aggregated), else raw outcome
                net_side = t.get("_net_side", outcome)
                net_vol = t.get("_net_vol", size)

                # Map Polymarket outcome to Kalshi side
                if net_side == "Up":
                    kalshi_side = "yes"
                elif net_side == "Down":
                    kalshi_side = "no"
                else:
                    P(f"  ?? Unknown outcome: {net_side} for {crypto}")
                    continue

                # Proportional sizing: our bet = WHALE_SCALE * whale's net volume
                bet_amount = max(0.01, round(WHALE_SCALE * net_vol, 2))
                P(f"\n  ┌─ [{whale_name}] {action} {crypto} {net_side}")
                P(f"  │  Whale volume:  ${net_vol:>10.2f}  (price: {price:.2f}, size: {size:.1f} shares)")
                P(f"  │  Our bet:       ${bet_amount:>10.2f}  ({WHALE_SCALE*100:.2f}% of whale)")
                P(f"  │  Kalshi side:   {kalshi_side.upper()}")

                trade_record = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "whale_name": whale_name,
                    "whale_timestamp": ts,
                    "crypto": crypto,
                    "whale_action": action,
                    "whale_outcome": outcome,
                    "whale_price": price,
                    "whale_size": size,
                    "kalshi_side": kalshi_side,
                    "kalshi_action": None,
                    "kalshi_ticker": None,
                    "kalshi_price": None,
                    "kalshi_status": None,
                    "result": "pending",
                }

                if action == "BUY":
                    # Mirror: buy on Kalshi — match by window time
                    if not series:
                        P(f"    No Kalshi series for {crypto}")
                        trade_record["kalshi_status"] = "no_series"
                    else:
                        # Match Polymarket window to Kalshi market by exact end/strike time
                        # NO fallback — if we can't match the exact window, skip it
                        market = None
                        if poly_end_date:
                            market = find_kalshi_market_for_window(series, poly_end_date)
                        if not market:
                            P(f"  └─ No matching Kalshi market for {series} (window: {poly_end_date[:19]})")
                            trade_record["kalshi_status"] = "no_market"
                        else:
                            ticker = market["ticker"]
                            trade_record["kalshi_ticker"] = ticker
                            trade_record["kalshi_action"] = "buy"

                            time.sleep(0.3)  # Rate limit
                            kalshi_price = get_kalshi_price(ticker, kalshi_side)
                            if kalshi_price and kalshi_price >= 0.02:
                                trade_record["kalshi_price"] = kalshi_price
                                contracts = max(1, int(bet_amount / (kalshi_price + 0.02)))
                                cost_est = round(contracts * (kalshi_price + 0.02), 2)
                                P(f"  │  Kalshi mkt:   {ticker}")
                                P(f"  │  Kalshi price:  {kalshi_price:.2f} → {contracts} contracts ≈ ${cost_est:.2f}")

                                pos_key = (whale_name, ticker)
                                if live:
                                    time.sleep(0.3)  # Rate limit
                                    result = place_buy(ticker, kalshi_side, kalshi_price, bet_amount)
                                    if result:
                                        trade_record["kalshi_status"] = "filled"
                                        trade_record["bet_amount"] = bet_amount
                                        our_positions[pos_key] = {
                                            "side": kalshi_side,
                                            "count": our_positions.get(pos_key, {}).get("count", 0) + contracts,
                                            "poly_event_id": t["_event_id"],
                                        }
                                        total_mirrored += 1
                                        P(f"  └─ PLACED ✓")
                                    else:
                                        trade_record["kalshi_status"] = "failed"
                                        P(f"  └─ FAILED ✗")
                                else:
                                    trade_record["kalshi_status"] = "dry_run"
                                    trade_record["bet_amount"] = bet_amount
                                    our_positions[pos_key] = {
                                        "side": kalshi_side,
                                        "count": our_positions.get(pos_key, {}).get("count", 0) + contracts,
                                        "poly_event_id": t["_event_id"],
                                    }
                                    total_mirrored += 1
                                    P(f"  └─ [DRY RUN] Would place order")
                            else:
                                P(f"  └─ No valid price for {ticker} (got: {kalshi_price})")
                                trade_record["kalshi_status"] = "no_price"

                elif action == "SELL":
                    # Exit: sell our Kalshi position
                    trade_record["kalshi_action"] = "sell"
                    P(f"\n  ┌─ [{whale_name}] EXIT {crypto} {outcome}")
                    P(f"  │  Whale sold:   {size:.1f} shares @ {price:.2f}")

                    # Find our position on the matching Kalshi market (exact window only)
                    if series:
                        market = None
                        if poly_end_date:
                            market = find_kalshi_market_for_window(series, poly_end_date)
                        if market:
                            ticker = market["ticker"]
                            trade_record["kalshi_ticker"] = ticker

                            pos_key = (whale_name, ticker)
                            if pos_key in our_positions:
                                pos = our_positions[pos_key]
                                P(f"  │  Our position: {pos['count']}x {pos['side'].upper()} on {ticker} (via {whale_name})")

                                if live:
                                    result = place_sell(ticker, pos["side"], pos["count"])
                                    if result:
                                        trade_record["kalshi_status"] = "sold"
                                        del our_positions[pos_key]
                                        total_exits += 1
                                        P(f"  └─ EXITED ✓")
                                    else:
                                        trade_record["kalshi_status"] = "sell_failed"
                                        P(f"  └─ EXIT FAILED ✗")
                                else:
                                    trade_record["kalshi_status"] = "dry_run"
                                    del our_positions[pos_key]
                                    total_exits += 1
                                    P(f"  └─ [DRY RUN] Would exit position")
                            else:
                                P(f"  └─ No position for {whale_name} on {ticker}")
                                trade_record["kalshi_status"] = "no_position"
                        else:
                            P(f"  └─ No open Kalshi market for {series}")
                    else:
                        P(f"  └─ No Kalshi series for {crypto}")

                trades_log.append(trade_record)

            # Save state after processing
            if new_whale_trades:
                save_json(TRADES_FILE, trades_log)
                save_json(SEEN_FILE, list(seen_ids))

                # Update status
                uptime = int(time.time() - start_time)
                status_data = {
                    "last_poll": datetime.now(timezone.utc).isoformat(),
                    "uptime_seconds": uptime,
                    "active_events": len(active_events),
                    "total_mirrored": total_mirrored,
                    "total_exits": total_exits,
                    "trades_logged": len(trades_log),
                    "whale_wallets": WHALE_WALLETS,
                    "our_positions": {f"{k[0]}:{k[1]}": v for k, v in our_positions.items()},
                }
                save_json(STATUS_FILE, status_data)

                # Push to GitHub so dashboard can read the data
                push_to_github("whale_trades.json", trades_log)
                push_to_github("whale_status.json", status_data)

            time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            P("\n  Stopped by user")
            break
        except Exception as e:
            P(f"  ERROR in poll loop: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(10)  # Back off on error

    # Final save
    save_json(TRADES_FILE, trades_log)
    save_json(SEEN_FILE, list(seen_ids))
    P(f"\n  Bot stopped. Mirrored: {total_mirrored}, Exits: {total_exits}")


if __name__ == "__main__":
    live = "--live" in sys.argv
    run(live=live)
