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

WHALE_WALLET = os.environ.get(
    "WHALE_WALLET", "0xc1737e2db2d19e0b73a958ecd5d0f279d0e726ee"
).lower()

BET_AMOUNT = 0.50            # $0.50 per mirrored trade
PRICE_BUMP_CENTS = 2         # Buy 2c above to fill at ask
POLL_INTERVAL = 5            # Seconds between Polymarket polls
EVENT_REFRESH = 900          # Refresh active events every 15 min

LOG_FILE = "whale_follow_bot.log"
TRADES_FILE = "whale_trades.json"
STATUS_FILE = "whale_status.json"
SEEN_FILE = "whale_seen_trades.json"

# Polymarket crypto → Kalshi series
CRYPTO_MAP = {
    "btc": {"name": "BTC", "series": "KXBTC15M"},
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
def find_kalshi_market(series_ticker):
    """Find the currently open Kalshi market for a crypto series."""
    try:
        data = kalshi_public("/events", params={
            "series_ticker": series_ticker,
            "status": "open",
            "limit": 5,
        })
        events = data.get("events", [])
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
            return None

        event_ticker = best_event["event_ticker"]
        resp = kalshi_public(f"/events/{event_ticker}")
        markets = resp.get("markets", [])
        return markets[0] if markets else None
    except Exception as e:
        P(f"    Error finding Kalshi market for {series_ticker}: {e}")
        return None


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
                    events.append(ev)
    except Exception as e:
        P(f"  Error fetching Poly events: {e}")
    return events


def fetch_event_trades(event_id):
    """Fetch trades for a Polymarket event, return whale trades only."""
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
                if t.get("proxyWallet", "").lower() == WHALE_WALLET:
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


# ── Main Bot ────────────────────────────────────────────────────────────
def run(live=False):
    P("=" * 65)
    P("  WHALE FOLLOW BOT — Mirror Polymarket Whale on Kalshi")
    P(f"  Mode: {'LIVE' if live else 'DRY RUN'} | Bet: ${BET_AMOUNT:.2f}/trade")
    P(f"  Whale: {WHALE_WALLET[:10]}...{WHALE_WALLET[-4:]}")
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

    # Track our Kalshi positions for exit signals
    our_positions = {}  # {kalshi_ticker: {side, count, poly_event_id}}

    # Track which Kalshi tickers we've already bet on (one bet per market)
    bet_tickers = set()

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

    P(f"\n  Polling started — watching for whale trades...")

    while True:
        try:
            # Refresh events periodically
            if time.time() - last_event_refresh > EVENT_REFRESH:
                active_events = fetch_active_poly_events()
                P(f"  Refreshed events: {len(active_events)} active")
                last_event_refresh = time.time()
                bet_tickers.clear()  # New windows = new bets allowed

            new_whale_trades = []

            # Poll each active event for whale trades
            for ev in active_events:
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
                    new_whale_trades.append(t)

            # Aggregate whale trades by event to determine NET direction
            # The whale often buys both sides — we follow the heavier side
            event_agg = {}  # {event_id: {Up_vol, Down_vol, crypto, series, trades}}
            for t in new_whale_trades:
                eid = t["_event_id"]
                if eid not in event_agg:
                    event_agg[eid] = {"Up": 0, "Down": 0, "crypto": t["_crypto"],
                                      "series": t["_series"], "buys": [], "sells": []}
                action = t.get("side", "")
                outcome = t.get("outcome", "")
                size = float(t.get("size", 0))
                if action == "BUY" and outcome in ("Up", "Down"):
                    event_agg[eid][outcome] += size
                    event_agg[eid]["buys"].append(t)
                elif action == "SELL":
                    event_agg[eid]["sells"].append(t)

            # Build one trade signal per event based on net direction
            aggregated_trades = []
            for eid, agg in event_agg.items():
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

                P(f"\n  WHALE SIGNAL: {action} {crypto} {net_side} — net ${net_vol:.2f} volume")

                trade_record = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
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
                    # Mirror: buy on Kalshi
                    if not series:
                        P(f"    No Kalshi series for {crypto}")
                        trade_record["kalshi_status"] = "no_series"
                    else:
                        time.sleep(0.5)  # Rate limit
                        market = find_kalshi_market(series)
                        if not market:
                            P(f"    No open Kalshi market for {series}")
                            trade_record["kalshi_status"] = "no_market"
                        else:
                            ticker = market["ticker"]
                            trade_record["kalshi_ticker"] = ticker
                            trade_record["kalshi_action"] = "buy"

                            # Only one bet per Kalshi market per window
                            if ticker in bet_tickers:
                                P(f"    Already bet on {ticker} — skipping")
                                trade_record["kalshi_status"] = "duplicate"
                            else:
                                time.sleep(0.3)  # Rate limit
                                kalshi_price = get_kalshi_price(ticker, kalshi_side)
                                if kalshi_price:
                                    trade_record["kalshi_price"] = kalshi_price
                                    P(f"    Kalshi: {ticker} {kalshi_side.upper()} @ {kalshi_price:.2f}")

                                    if live:
                                        time.sleep(0.3)  # Rate limit
                                        result = place_buy(ticker, kalshi_side, kalshi_price, BET_AMOUNT)
                                        if result:
                                            trade_record["kalshi_status"] = "filled"
                                            bet_tickers.add(ticker)
                                            our_positions[ticker] = {
                                                "side": kalshi_side,
                                                "count": max(1, int(BET_AMOUNT / (kalshi_price + 0.02))),
                                                "poly_event_id": t["_event_id"],
                                            }
                                            total_mirrored += 1
                                        else:
                                            trade_record["kalshi_status"] = "failed"
                                    else:
                                        trade_record["kalshi_status"] = "dry_run"
                                        P(f"    [DRY RUN] Would buy {kalshi_side} on {ticker}")
                                else:
                                    P(f"    No price available for {ticker}")
                                    trade_record["kalshi_status"] = "no_price"

                elif action == "SELL":
                    # Exit: sell our Kalshi position
                    trade_record["kalshi_action"] = "sell"

                    # Find our position on the matching Kalshi market
                    if series:
                        time.sleep(0.5)  # Rate limit
                        market = find_kalshi_market(series)
                        if market:
                            ticker = market["ticker"]
                            trade_record["kalshi_ticker"] = ticker

                            if ticker in our_positions:
                                pos = our_positions[ticker]
                                P(f"    EXIT: Selling {pos['count']}x {pos['side']} on {ticker}")

                                if live:
                                    result = place_sell(ticker, pos["side"], pos["count"])
                                    if result:
                                        trade_record["kalshi_status"] = "sold"
                                        del our_positions[ticker]
                                        total_exits += 1
                                    else:
                                        trade_record["kalshi_status"] = "sell_failed"
                                else:
                                    trade_record["kalshi_status"] = "dry_run"
                                    P(f"    [DRY RUN] Would sell {pos['side']} on {ticker}")
                            else:
                                P(f"    No Kalshi position to exit for {ticker}")
                                trade_record["kalshi_status"] = "no_position"

                trades_log.append(trade_record)

            # Save state after processing
            if new_whale_trades:
                save_json(TRADES_FILE, trades_log)
                save_json(SEEN_FILE, list(seen_ids))

                # Update status
                uptime = int(time.time() - start_time)
                save_json(STATUS_FILE, {
                    "last_poll": datetime.now(timezone.utc).isoformat(),
                    "uptime_seconds": uptime,
                    "active_events": len(active_events),
                    "total_mirrored": total_mirrored,
                    "total_exits": total_exits,
                    "trades_logged": len(trades_log),
                    "whale_wallet": WHALE_WALLET,
                    "our_positions": {k: v for k, v in our_positions.items()},
                })

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
