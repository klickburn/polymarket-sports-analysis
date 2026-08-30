"""
Multi-Whale Dry Runner
======================
Runs whale_follow_bot logic for multiple whales simultaneously in dry-run mode.
Compares signals side by side.

Usage: python3 whale_dry_run.py
"""

import json
import os
import sys
import time
import requests
from datetime import datetime, timezone
from collections import defaultdict

GAMMA_API = "https://gamma-api.polymarket.com"
DATA_API = "https://data-api.polymarket.com"

WHALES = {
    "Fickle-Spark": "0xc1737e2db2d19e0b73a958ecd5d0f279d0e726ee",
    "Blushing-Fine": "0x63ce342161250d705dc0b16df89036c8e5f9ba9a",
    "Moral-Roof": "0x576b0696fd5a9225d66fd9500fd98f5be10b0cab",
}

WHALE_SCALE = 0.01  # 1% of whale volume

CRYPTO_MAP = {
    "btc": {"name": "BTC", "series": "KXBTC15M"},
    "eth": {"name": "ETH", "series": "KXETH15M"},
    "sol": {"name": "SOL", "series": "KXSOL15M"},
    "xrp": {"name": "XRP", "series": "KXXRP15M"},
}

POLL_INTERVAL = 5
EVENT_REFRESH = 900
LOG_FILE = "whale_dry_run.log"

poly_session = requests.Session()
poly_session.headers.update({"Accept": "application/json"})

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


def fetch_active_events():
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
        P(f"  Error fetching events: {e}")
    return events


def fetch_whale_trades(event_id, wallet):
    trades = []
    offset = 0
    while True:
        try:
            r = poly_session.get(f"{DATA_API}/trades", params={
                "eventId": event_id, "limit": 1000, "offset": offset,
            }, timeout=20)
            if r.status_code != 200:
                break
            data = r.json()
            if not isinstance(data, list) or not data:
                break
            for t in data:
                if t.get("proxyWallet", "").lower() == wallet:
                    trades.append(t)
            if len(data) < 1000:
                break
            offset += 1000
            if offset >= 5000:
                break
        except Exception:
            break
    return trades


def run():
    P("=" * 75)
    P("  MULTI-WHALE DRY RUNNER — Comparing 3 whales side by side")
    P(f"  Scale: {WHALE_SCALE*100:.1f}% of whale volume")
    for name, wallet in WHALES.items():
        P(f"  {name:20s} {wallet[:10]}...{wallet[-4:]}")
    P(f"  Poll: every {POLL_INTERVAL}s")
    P("=" * 75)

    # Per-whale state
    seen = {name: set() for name in WHALES}
    stats = {name: {"mirrored": 0, "exits": 0, "total_vol": 0, "our_total": 0}
             for name in WHALES}

    # Warmup
    active_events = fetch_active_events()
    P(f"  Active events: {len(active_events)}")

    P("  WARMUP: Marking existing trades as seen...")
    for ev in active_events:
        event_id = ev.get("id")
        if not event_id:
            continue
        for name, wallet in WHALES.items():
            trades = fetch_whale_trades(event_id, wallet)
            for t in trades:
                tid = t.get("transactionHash", f"{t.get('timestamp')}_{t.get('size')}_{t.get('price')}")
                seen[name].add(tid)
    for name in WHALES:
        P(f"  {name}: {len(seen[name])} existing trades marked")

    last_event_refresh = time.time()
    last_heartbeat = time.time()
    poll_count = 0
    start_time = time.time()

    P(f"\n  Polling started...\n")

    while True:
        try:
            if time.time() - last_event_refresh > EVENT_REFRESH:
                active_events = fetch_active_events()
                P(f"  Refreshed: {len(active_events)} events")
                last_event_refresh = time.time()

            poll_count += 1

            if time.time() - last_heartbeat >= 60:
                uptime_min = int((time.time() - start_time) / 60)
                parts = []
                for name in WHALES:
                    s = stats[name]
                    parts.append(f"{name}: {s['mirrored']}m/{s['exits']}x/${s['our_total']:.2f}")
                P(f"  ♥ {poll_count} polls, {uptime_min}m | {' | '.join(parts)}")
                last_heartbeat = time.time()

            # Check each event for each whale
            for ev in active_events:
                event_id = ev.get("id")
                if not event_id:
                    continue

                # Fetch ALL trades once, filter per whale
                all_trades = []
                try:
                    r = poly_session.get(f"{DATA_API}/trades", params={
                        "eventId": event_id, "limit": 1000,
                    }, timeout=20)
                    if r.status_code == 200:
                        all_trades = r.json() if isinstance(r.json(), list) else []
                except Exception:
                    continue

                for name, wallet in WHALES.items():
                    new_trades = []
                    for t in all_trades:
                        if t.get("proxyWallet", "").lower() != wallet:
                            continue
                        tid = t.get("transactionHash", f"{t.get('timestamp')}_{t.get('size')}_{t.get('price')}")
                        if tid in seen[name]:
                            continue
                        seen[name].add(tid)
                        t["_crypto"] = ev.get("_crypto", "?")
                        t["_series"] = ev.get("_series", "")
                        t["_event_id"] = event_id
                        new_trades.append(t)

                    if not new_trades:
                        continue

                    # Aggregate by direction
                    up_vol = 0
                    down_vol = 0
                    sells = []
                    for t in new_trades:
                        action = t.get("side", "")
                        outcome = t.get("outcome", "")
                        size = float(t.get("size", 0))
                        price = float(t.get("price", 0))
                        if action == "BUY":
                            if outcome == "Up":
                                up_vol += size * price
                            elif outcome == "Down":
                                down_vol += size * price
                        elif action == "SELL":
                            sells.append(t)

                    crypto = ev.get("_crypto", "?")

                    # Log buys
                    if up_vol > 0 or down_vol > 0:
                        if up_vol > down_vol:
                            net_side, net_vol = "Up", up_vol
                            kalshi_side = "YES"
                        else:
                            net_side, net_vol = "Down", down_vol
                            kalshi_side = "NO"

                        bet_amount = max(0.01, round(WHALE_SCALE * net_vol, 2))
                        stats[name]["mirrored"] += 1
                        stats[name]["total_vol"] += net_vol
                        stats[name]["our_total"] += bet_amount

                        P(f"  ┌─ [{name}] BUY {crypto} {net_side}")
                        P(f"  │  Whale volume:  ${net_vol:>10.2f}")
                        P(f"  │  Our bet:       ${bet_amount:>10.2f}  ({WHALE_SCALE*100:.1f}%)")
                        P(f"  │  Kalshi side:   {kalshi_side}")
                        P(f"  └─ [DRY RUN]")

                    # Log sells
                    for t in sells:
                        size = float(t.get("size", 0))
                        price = float(t.get("price", 0))
                        outcome = t.get("outcome", "")
                        stats[name]["exits"] += 1
                        P(f"  ┌─ [{name}] EXIT {crypto} {outcome}")
                        P(f"  │  Sold: {size:.1f} shares @ {price:.2f}")
                        P(f"  └─ [DRY RUN]")

            time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            P("\n  Stopped by user")
            break
        except Exception as e:
            P(f"  ERROR: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(10)

    # Final summary
    P("\n" + "=" * 75)
    P("  FINAL SUMMARY")
    P("=" * 75)
    P(f"  {'Whale':20s} {'Signals':>8} {'Exits':>8} {'Whale Vol':>12} {'Our Cost':>12}")
    P(f"  {'-'*20} {'-'*8} {'-'*8} {'-'*12} {'-'*12}")
    for name in WHALES:
        s = stats[name]
        P(f"  {name:20s} {s['mirrored']:>8} {s['exits']:>8} ${s['total_vol']:>11,.2f} ${s['our_total']:>11,.2f}")
    P("=" * 75)


if __name__ == "__main__":
    run()
