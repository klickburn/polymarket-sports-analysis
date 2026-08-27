"""Dashboard for the Polymarket bot. Separate service, separate data, own port.

Runs the bot in a background thread and serves its own UI, mirroring how the
Kalshi service is arranged but sharing nothing with it.

The headline panel is the SCAN RATE, not P&L. Polymarket's 15m contracts price
against each window's opening price rather than a fixed strike, so the open
question is whether the deployed 0.60-0.85 entry band is ever reached. Every
scanned window is recorded either way, and this dashboard reports
qualified/scanned with the observed price distribution, so that question is
answered by production data instead of argument.
"""
import json
import os
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

import poly_bot as B
import poly_client as PC

app = FastAPI(title="Polymarket 15m Bot")

_HTML = os.path.join(os.path.dirname(__file__), "poly_dashboard.html")
_cache = {"at": 0, "data": None}
_lock = threading.Lock()


def _pct(a, b):
    return round(a / b * 100, 1) if b else 0.0


def build_report():
    bets = B.load_bets()
    scans = [b for b in bets if b.get("kind") == "scan"]
    core = [b for b in bets if b.get("kind") == "core"]
    dips = [b for b in bets if b.get("kind") == "dip"]

    qual = [s for s in scans if s.get("qualified")]
    prices = [s["best_price"] for s in scans if s.get("best_price") is not None]
    buckets = defaultdict(int)
    for p in prices:
        buckets[f"{int(p * 20) / 20:.2f}"] += 1

    # windows, not rows: both cryptos scan the same window
    windows = {s.get("close_ts") for s in scans}
    qwindows = {s.get("close_ts") for s in qual}

    def book(rows, label):
        res = [r for r in rows if r.get("result") in ("win", "loss")]
        real = [r for r in res if not r.get("phantom")]
        wins = [r for r in res if r["result"] == "win"]
        pnl = sum(r.get("pnl", 0) for r in real)
        return {
            "label": label, "placed": len(rows), "resolved": len(res),
            "wins": len(wins), "win_rate": _pct(len(wins), len(res)),
            "pnl": round(pnl, 2),
            "contracts": round(sum(float(r.get("contracts") or 0) for r in real), 0),
        }

    split = [d for d in dips if d.get("dip_type") == "split"]
    coredip = [d for d in dips if d.get("dip_type") == "core"]

    by_day = defaultdict(lambda: {"day": "", "pnl": 0.0, "fills": 0, "wins": 0})
    for r in dips + core:
        if r.get("result") not in ("win", "loss") or r.get("phantom"):
            continue
        k = (r.get("timestamp") or "")[:10]
        e = by_day[k]
        e["day"] = k
        e["pnl"] += r.get("pnl", 0)
        e["fills"] += 1
        e["wins"] += 1 if r["result"] == "win" else 0

    recent = sorted(scans, key=lambda s: s.get("timestamp") or "", reverse=True)[:40]

    return {
        "mode": "LIVE" if PC.is_live() else "READ-ONLY",
        "balance": PC.get_balance(),
        "config": {
            "band": [B.MIN_PRICE, B.MAX_PRICE],
            "phantom": B.PHANTOM_MODE,
            "split_dip": [B.SPLIT_DIP_ENABLED, B.SPLIT_DIP_COUNT],
            "core_dip": [B.CORE_DIP_ENABLED, B.CORE_DIP_COUNT],
            "cryptos": PC.CRYPTOS,
        },
        "scan": {
            "rows": len(scans),
            "windows": len(windows),
            "qualified_rows": len(qual),
            "qualified_windows": len(qwindows),
            "qualify_rate": _pct(len(qwindows), len(windows)),
            "max_price": max(prices) if prices else None,
            "median_price": sorted(prices)[len(prices) // 2] if prices else None,
            "buckets": dict(sorted(buckets.items())),
        },
        "books": [book(core, "Core (phantom)"),
                  book(split, "Split dips"),
                  book(coredip, "Core dips")],
        "by_day": sorted(by_day.values(), key=lambda x: x["day"], reverse=True)[:14],
        "recent": recent,
        "generated": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/api/poly-data")
def api_data():
    with _lock:
        if _cache["data"] and time.time() - _cache["at"] < 20:
            return JSONResponse(_cache["data"])
        d = build_report()
        _cache.update({"at": time.time(), "data": d})
        return JSONResponse(d)


@app.get("/", response_class=HTMLResponse)
def index():
    try:
        with open(_HTML) as f:
            return HTMLResponse(f.read())
    except FileNotFoundError:
        return HTMLResponse("<h1>poly_dashboard.html missing</h1>", status_code=500)


@app.get("/health")
def health():
    return {"ok": True, "mode": "LIVE" if PC.is_live() else "READ-ONLY"}


def _bot_thread():
    try:
        B.main()
    except Exception as e:
        B.P(f"  [BOT THREAD] died: {e}")


@app.on_event("startup")
def _startup():
    if os.environ.get("POLY_RUN_BOT", "1") == "1":
        threading.Thread(target=_bot_thread, daemon=True).start()
        B.P("  [SERVER] bot thread started")
