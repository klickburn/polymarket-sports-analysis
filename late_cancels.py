"""Money saved by the late cancels -- read-only: public prices, no keys, no orders.

Every dip order the bot really cancelled near the end of its window (split dips
at minute 14, ETH core dips at minute 13) is checked once its market settles:

  1. would it have filled?  after the cancel, YES traded or was offered at the
     order's limit or better (public 1-minute candles)
  2. did that side win?     the market's public result

A fill on a losing side is a loss the cancel avoided (contracts x price); a fill
on a winning side is a win it gave up (contracts x (1 - price)). Saved = avoided
- given up, at the order's real size. Makers pay no fee on these orders, so fees
are left out. Method adapted from the strategy-lab session's draft (trading-6f).

Verdicts are kept on the volume: a settled market never changes, so each order
is checked once.
"""
import json
import os
import threading
import time
from datetime import datetime, timezone, timedelta

import requests

KALSHI = "https://api.elections.kalshi.com/trade-api/v2"
WINDOW = 900
PASS_EVERY = 300          # seconds between checking passes
PACE = 0.3                # seconds between public reads

_lock = threading.Lock()
_state = {"checks": None, "path": None, "report": None}


def _ts(v):
    """Epoch seconds from an ISO time; None for anything else ("gone", "", None)."""
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def orders(bets):
    """Cancelled dip orders with something left resting when the cancel landed."""
    out = []
    for b in bets:
        if not b.get("dip_add") or b.get("result") == "dip_pending":
            continue
        at = _ts(b.get("cancelled_at"))
        end = _ts(b.get("window_end"))
        side = (b.get("side") or "").lower()
        price = float(b.get("price") or 0)
        n = int(b.get("contracts") or 0) - int(b.get("filled_count") or 0)
        if at is None or not end or side not in ("yes", "no") or not 0 < price < 1 or n <= 0 or not b.get("ticker"):
            continue
        out.append({"key": "%s|%s|%s" % (b.get("order_id") or b["ticker"], price, b["cancelled_at"]),
                    "ticker": b["ticker"], "crypto": b.get("crypto"), "type": b.get("dip_type") or "split",
                    "tier": b.get("dip_tier") or "%.0fc" % (price * 100), "experiment": bool(b.get("experiment")),
                    "side": side, "price": price, "n": n, "at": at, "end": end})
    return out


def _get(path, params=None):
    time.sleep(PACE)
    for i in range(3):
        r = requests.get(KALSHI + path, params=params, timeout=20)
        if r.status_code == 429:
            time.sleep(5 * (i + 1))
            continue
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    return None


def _vals(k, *path):
    out = []
    for a, b in path:
        v = (k.get(a) or {}).get(b)
        if v not in (None, ""):
            out.append(float(v))
    return out


def check_ticker(ticker, group, get=_get):
    """{order key: verdict} for one settled market, or None if it has not settled."""
    m = (get("/markets/%s" % ticker) or {}).get("market") or {}
    res = m.get("result")
    if res not in ("yes", "no"):
        return None
    first = min(o["at"] for o in group)
    end = max(o["end"] for o in group)
    c = (get("/series/%s/markets/%s/candlesticks" % (ticker.split("-")[0], ticker),
             {"start_ts": int(first - first % 60), "end_ts": int(end), "period_interval": 1}) or {}).get("candlesticks") or []
    if not c:
        return None                    # no price data yet: try again next pass
    out = {}
    for o in group:
        start = o["at"] - o["at"] % 60          # the cancel's own minute onward
        after = [k for k in c if k.get("end_period_ts", 0) > start]
        p = o["price"]
        if o["side"] == "yes":       # a YES buy at p fills when YES trades or is offered at p or lower
            fill = any(v <= p + 1e-9 for k in after for v in _vals(k, ("price", "low_dollars"), ("yes_ask", "low_dollars")))
        else:                        # a NO buy at p is a YES sale at 1-p: fills when YES is bid or trades at 1-p or higher
            fill = any(v >= 1 - p - 1e-9 for k in after for v in _vals(k, ("price", "high_dollars"), ("yes_bid", "high_dollars")))
        out[o["key"]] = {"fill": bool(fill), "won": res == o["side"], "result": res}
    return out


def _blank():
    return {"cancelled": 0, "contracts": 0, "checked": 0, "waiting": 0, "no_fill": 0,
            "fill_lost": 0, "fill_won": 0, "avoided": 0.0, "given_up": 0.0}


def _add(g, o, v):
    g["cancelled"] += 1
    g["contracts"] += o["n"]
    if not v:
        g["waiting"] += 1
        return
    g["checked"] += 1
    if not v["fill"]:
        g["no_fill"] += 1
    elif v["won"]:
        g["fill_won"] += 1
        g["given_up"] += o["n"] * (1 - o["price"])
    else:
        g["fill_lost"] += 1
        g["avoided"] += o["n"] * o["price"]


def _done(g, **extra):
    g = dict(g, **extra)
    g["avoided"], g["given_up"] = round(g["avoided"], 2), round(g["given_up"], 2)
    g["saved"] = round(g["avoided"] - g["given_up"], 2)
    return g


def summarise(ords, checks, central_date, now=None):
    now = now or time.time()
    today = central_date(now)
    groups, today_g = {}, {}
    for o in ords:
        v = checks.get(o["key"])
        _add(groups.setdefault(o["type"], _blank()), o, v)
        if central_date(o["end"] - WINDOW) == today:
            _add(today_g.setdefault(o["type"], _blank()), o, v)
    order = lambda t: (t != "split", t)
    rows = [_done(groups[t], type=t, today=_done(today_g.get(t, _blank()))) for t in sorted(groups, key=order)]
    recent = []
    for o in sorted(ords, key=lambda o: -o["at"])[:60]:
        v = checks.get(o["key"])
        saved = None
        if v:
            saved = 0.0 if not v["fill"] else round(-o["n"] * (1 - o["price"]) if v["won"] else o["n"] * o["price"], 2)
        recent.append({"window_end": datetime.fromtimestamp(o["end"], timezone.utc).isoformat(),
                       "cancelled_at": datetime.fromtimestamp(o["at"], timezone.utc).isoformat(),
                       "crypto": o["crypto"], "type": o["type"], "tier": o["tier"], "experiment": o["experiment"],
                       "side": o["side"], "price": o["price"], "contracts": o["n"],
                       "result": v and v["result"], "would_fill": v and v["fill"], "saved": saved})
    return {"rows": rows,
            "saved": round(sum(r["saved"] for r in rows), 2),
            "saved_today": round(sum(r["today"]["saved"] for r in rows), 2),
            "since": datetime.fromtimestamp(min(o["at"] for o in ords), timezone.utc).isoformat() if ords else None,
            "recent": recent,
            "generated": datetime.now(timezone.utc).isoformat()}


def _load_checks(path):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return {}


def _save_checks(path, checks):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(checks, f)
    os.replace(tmp, path)


def run_pass(load_bets, checks_path, central_date, get=_get, budget_s=200):
    """Check every settled, unchecked cancel; rebuild the report."""
    with _lock:
        if _state["checks"] is None or _state["path"] != checks_path:
            _state["checks"], _state["path"] = _load_checks(checks_path), checks_path
        checks = dict(_state["checks"])
    ords = orders(load_bets())
    todo = {}
    for o in ords:
        if o["key"] not in checks and time.time() > o["end"] + 90:
            todo.setdefault(o["ticker"], []).append(o)
    start, new = time.time(), 0
    for ticker, group in sorted(todo.items(), key=lambda kv: min(o["end"] for o in kv[1])):
        if time.time() - start > budget_s:
            break
        try:
            got = check_ticker(ticker, group, get)
        except Exception:
            continue
        if got:
            checks.update(got)
            new += len(got)
    if new:
        _save_checks(checks_path, checks)
    rep = summarise(ords, checks, central_date)
    with _lock:
        _state["checks"], _state["report"] = checks, rep
    return rep


def loop(load_bets, checks_path, central_date, log=print):
    while True:
        try:
            run_pass(load_bets, checks_path, central_date)
        except Exception as e:
            log("  [LATE-CANCELS] pass failed: %s %s" % (type(e).__name__, str(e)[:120]))
        time.sleep(PASS_EVERY)


def report():
    with _lock:
        return _state["report"] or {"rows": [], "recent": [], "working": True}
