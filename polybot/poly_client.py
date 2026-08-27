"""Polymarket CLOB adapter for the 15-minute BTC/ETH up-down markets.

Standalone: shares no file with the Kalshi bot.

Two APIs are involved and they do different jobs:
  * Gamma (gamma-api.polymarket.com) — public. Discovers the per-window events.
    Slugs are deterministic: `btc-updown-15m-<unix_ts>` where <unix_ts> is the
    window's CLOSE time, so a window can be addressed without searching.
  * CLOB (clob.polymarket.com) — order book reads (public) and order placement
    (signed). Signing is EIP-712 over Polygon and is handled by py-clob-client;
    we never hand-roll it.

Structural note, because it drives the whole bot: a Polymarket window is ONE
market with two outcome tokens, "Up" and "Down". Kalshi lists a strike market
per crypto with yes/no. The mapping used here is Up<->yes, Down<->no, which
keeps the strategy code readable, but the instruments are not equivalent: Kalshi
prices against a fixed strike, Polymarket against the window's own opening
price. Measured over 360 windows, the Polymarket favourite peaked at a median
0.51 and touched the deployed 0.60-0.85 entry band in 1 case out of 360.
"""
import json
import os
import time
from datetime import datetime, timezone

import requests

GAMMA = "https://gamma-api.polymarket.com"
CLOB = "https://clob.polymarket.com"
POLYGON_CHAIN_ID = 137

WINDOW_SECONDS = int(os.environ.get("POLY_WINDOW_SECONDS", "900"))   # 15m
CRYPTOS = [c.strip().upper() for c in
           os.environ.get("POLY_CRYPTOS", "BTC,ETH").split(",") if c.strip()]

_SLUG = {"BTC": "btc", "ETH": "eth", "SOL": "sol", "XRP": "xrp"}

_session = requests.Session()
_session.headers["User-Agent"] = "polybot/1.0"


def P(msg=""):
    print(f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


# ── Window helpers ──────────────────────────────────────────────────────
def window_close(ts=None):
    """Close timestamp of the window currently in progress."""
    now = int(ts or time.time())
    return now - (now % WINDOW_SECONDS) + WINDOW_SECONDS


def window_slug(crypto, close_ts):
    d = "15m" if WINDOW_SECONDS == 900 else f"{WINDOW_SECONDS // 60}m"
    return f"{_SLUG.get(crypto.upper(), crypto.lower())}-updown-{d}-{int(close_ts)}"


# ── Discovery (public) ──────────────────────────────────────────────────
def get_window_market(crypto, close_ts):
    """The market for one crypto's window, or None.

    Returns tokens keyed by our yes/no vocabulary so strategy code never has to
    know about "Up"/"Down".
    """
    slug = window_slug(crypto, close_ts)
    try:
        r = _session.get(f"{GAMMA}/events", params={"slug": slug}, timeout=20)
        if r.status_code != 200:
            return None
        events = r.json() or []
        if not events:
            return None
        ev = events[0]
        markets = ev.get("markets") or []
        if not markets:
            return None
        m = markets[0]
        outcomes = json.loads(m.get("outcomes") or "[]")
        tokens = json.loads(m.get("clobTokenIds") or "[]")
        if len(outcomes) < 2 or len(tokens) < 2:
            return None
        by_side = {}
        for label, tok in zip(outcomes, tokens):
            key = "yes" if label.strip().lower() == "up" else "no"
            by_side[key] = tok
        return {
            "crypto": crypto.upper(),
            "slug": slug,
            "event_slug": ev.get("slug"),
            "question": m.get("question") or ev.get("title"),
            "close_ts": int(close_ts),
            "window_end": datetime.fromtimestamp(int(close_ts), timezone.utc).isoformat(),
            "closed": bool(ev.get("closed")),
            "tokens": by_side,
            "min_size": float(m.get("orderMinSize") or 5),
            "tick": float(m.get("orderPriceMinTickSize") or 0.01),
            "outcome_prices": m.get("outcomePrices"),
        }
    except Exception as e:
        P(f"  [DISCOVER] {slug}: {e}")
        return None


def get_window_markets(close_ts):
    """{crypto: market} for every configured crypto in this window."""
    out = {}
    for c in CRYPTOS:
        m = get_window_market(c, close_ts)
        if m and not m["closed"]:
            out[c] = m
    return out


# ── Order book (public) ─────────────────────────────────────────────────
def get_book(token_id):
    try:
        r = _session.get(f"{CLOB}/book", params={"token_id": token_id}, timeout=15)
        if r.status_code != 200:
            return None
        j = r.json() or {}
        return {"bids": j.get("bids") or [], "asks": j.get("asks") or []}
    except Exception:
        return None


def best_prices(token_id):
    """(best_bid, best_ask) as floats. Polymarket returns books ascending, so
    the touch is the LAST element on each side."""
    b = get_book(token_id)
    if not b:
        return None, None
    bid = float(b["bids"][-1]["price"]) if b["bids"] else None
    ask = float(b["asks"][-1]["price"]) if b["asks"] else None
    return bid, ask


def mid_price(token_id):
    bid, ask = best_prices(token_id)
    if bid is None and ask is None:
        return None
    if bid is None:
        return ask
    if ask is None:
        return bid
    return (bid + ask) / 2.0


def depth_at_or_below(token_id, price):
    """Total resting BID size at or below `price` — how much competition a dip
    order queues behind at that level."""
    b = get_book(token_id)
    if not b:
        return 0.0
    return sum(float(x["size"]) for x in b["bids"] if float(x["price"]) <= price)


# ── Authenticated client ────────────────────────────────────────────────
_client = None
_client_err = None


def _build_client():
    """Level-2 py-clob-client, or None if credentials are absent/incomplete.

    Absence is not an error: the bot runs read-only and records phantom trades
    without credentials, which is how the collector phase works.
    """
    global _client, _client_err
    if _client is not None or _client_err is not None:
        return _client
    key = os.environ.get("POLY_PRIVATE_KEY", "").strip()
    api_key = os.environ.get("POLY_API_KEY", "").strip()
    secret = os.environ.get("POLY_API_SECRET", "").strip()
    passphrase = os.environ.get("POLY_API_PASSPHRASE", "").strip()
    funder = os.environ.get("POLY_FUNDER", "").strip() or None
    if not key:
        _client_err = "POLY_PRIVATE_KEY not set — read-only mode"
        P(f"  [CLIENT] {_client_err}")
        return None
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds

        sig_type = int(os.environ.get("POLY_SIGNATURE_TYPE", "0"))
        kwargs = {"host": CLOB, "chain_id": POLYGON_CHAIN_ID, "key": key}
        if funder:
            kwargs["funder"] = funder
            kwargs["signature_type"] = sig_type
        c = ClobClient(**kwargs)
        if api_key and secret and passphrase:
            c.set_api_creds(ApiCreds(api_key=api_key, api_secret=secret,
                                     api_passphrase=passphrase))
        else:
            # L1 key alone is enough to derive L2 creds
            c.set_api_creds(c.create_or_derive_api_creds())
        _client = c
        P(f"  [CLIENT] CLOB ready for {c.get_address()}")
        return _client
    except Exception as e:
        _client_err = str(e)
        P(f"  [CLIENT] init failed: {e}")
        return None


def client():
    return _build_client()


def is_live():
    return _build_client() is not None


# ── Orders ──────────────────────────────────────────────────────────────
def place_limit(token_id, side, price, size, gtc=True):
    """Rest (or cross) a limit order. Returns an order id, or None.

    GTC is what the dip strategy needs: the order must SIT at 10c waiting for a
    collapse. FOK/FAK would defeat the entire premise.
    """
    c = client()
    if c is None:
        return None
    try:
        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY, SELL

        args = OrderArgs(
            token_id=str(token_id),
            price=round(float(price), 2),
            size=float(size),
            side=BUY if str(side).upper() == "BUY" else SELL,
        )
        signed = c.create_order(args)
        resp = c.post_order(signed, OrderType.GTC if gtc else OrderType.FAK)
        oid = (resp or {}).get("orderID") or (resp or {}).get("order_id")
        if not oid and (resp or {}).get("success") is False:
            P(f"    [ORDER] rejected: {str(resp)[:200]}")
            return None
        return oid
    except Exception as e:
        P(f"    [ORDER] failed: {e}")
        return None


def cancel_order(order_id):
    c = client()
    if c is None:
        return False
    try:
        r = c.cancel(str(order_id))
        return bool(r)
    except Exception as e:
        P(f"    [CANCEL] failed for {order_id}: {e}")
        return False


def get_open_orders():
    c = client()
    if c is None:
        return []
    try:
        return c.get_orders() or []
    except Exception as e:
        P(f"  [ORDERS] fetch failed: {e}")
        return []


def get_balance():
    """USDC collateral available, or None when read-only."""
    c = client()
    if c is None:
        return None
    try:
        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
        b = c.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
        raw = (b or {}).get("balance")
        return float(raw) / 1e6 if raw is not None else None   # USDC has 6 dp
    except Exception as e:
        P(f"  [BALANCE] fetch failed: {e}")
        return None


def settled_outcome(crypto, close_ts):
    """('yes'|'no') once the window resolves, else None."""
    m = get_window_market(crypto, close_ts)
    if not m:
        return None
    try:
        prices = json.loads(m.get("outcome_prices") or "[]")
    except Exception:
        return None
    if len(prices) < 2:
        return None
    up, down = float(prices[0]), float(prices[1])
    if up >= 0.99:
        return "yes"
    if down >= 0.99:
        return "no"
    return None
