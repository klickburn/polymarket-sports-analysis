#!/usr/bin/env python3
"""Live-vs-backtest parity check for the count-based rebuilt strategy.

Compares what the LIVE bot actually did (recorded `contracts` per trade) against
the WINDOW-AWARE backtest of the deployed strategy (what it SHOULD have done),
trade-for-trade. With count-based protection the sizing decisions key off the
win/loss SEQUENCE (identical live and backtest), so the gap should be ~$0 and any
residual should be a labeled `scale_state` or `split_guard` row, not mystery drift.

Usage:
    python3 parity_check.py            # all days in the (post-reset) live file
    python3 parity_check.py 2026-07-27 # one UTC day

Config is read from env so it stays in sync with the deployed Railway vars.
"""
import os, sys, json, math, time, urllib.request
from collections import OrderedDict

HERE = os.path.dirname(os.path.abspath(__file__))
BETS_FILE = os.environ.get("SCORE_BETS_FILE", os.path.join(HERE, "crypto_score_bets.json"))
KALSHI = "https://api.elections.kalshi.com/trade-api/v2/markets/"
DASH_URL = os.environ.get("DASHBOARD_URL", "https://ankurkalshi.up.railway.app")


def get_reset_ts():
    """The bot drops trades older than the last reset, so the parity check must
    too — otherwise a stale git copy of the bets file (pre-reset) is included.
    Order: RESET_TS env > local .reset_ts file > dashboard /api/score-debug."""
    if os.environ.get("RESET_TS"):
        return os.environ["RESET_TS"]
    for p in (os.path.join(os.environ.get("SCORE_DATA_DIR", "/data"), ".reset_ts"),
              os.path.join(HERE, ".reset_ts")):
        try:
            return open(p).read().strip()
        except Exception:
            pass
    try:
        req = urllib.request.Request(DASH_URL + "/api/score-debug", headers={"Accept": "application/json"})
        return json.load(urllib.request.urlopen(req, timeout=15)).get("reset_ts")
    except Exception:
        return None

# ── Strategy config — defaults MUST match the deployed Railway env, else the
# check reports spurious 'scale_state' gaps. Current deployed: 30x, press OFF,
# vol gate 0.20. Override any of these via env when live config changes.
SCALE_UP       = int(os.environ.get("SCALE_UP_COUNT", "30"))
PROTECT_MODE   = os.environ.get("PROTECT_MODE", "count")
COUNT_SOFT_C   = int(os.environ.get("COUNT_SOFT_C", "3"))
COUNT_REACT_K  = int(os.environ.get("COUNT_REACT_K", "1"))
PRESS_STREAK_N = int(os.environ.get("PRESS_STREAK_N", "0"))
PRESS_MULT     = float(os.environ.get("PRESS_MULT", "1.5"))
CO_WR          = float(os.environ.get("COOL_OFF_WR", "0.8"))
CO_WIN         = int(os.environ.get("COOL_OFF_WINDOW", "10"))
CO_STREAK      = int(os.environ.get("COOL_OFF_BYPASS_STREAK", "6"))
SPLIT_GUARD    = os.environ.get("SPLIT_GUARD", "1") == "1"
VOL_GATE       = float(os.environ.get("VOL_GATE", "0.20"))
GROUPS = {"A": {"sig": {4, 5}, "uw": 4, "ut": 3, "dw": 16, "dt": 3},
          "B": {"sig": {1, 2, 3}, "uw": 6, "ut": 4, "dw": 16, "dt": 4}}


def net_pnl(price, won, n):
    if not price or not (0 < price < 1):
        return 0.0
    gross = n * ((1 - price) if won else -price)
    fee = math.ceil(0.07 * n * price * (1 - price) * 100) / 100.0
    return gross - fee


def group_of(score):
    sig = score + 3
    for name, c in GROUPS.items():
        if sig in c["sig"]:
            return name, c
    return None, None


def kalshi_result(tkr, _cache={}):
    if tkr in _cache:
        return _cache[tkr]
    for attempt in (1, 2):
        try:
            req = urllib.request.Request(KALSHI + tkr, headers={"Accept": "application/json"})
            m = json.load(urllib.request.urlopen(req, timeout=15))["market"]
            _cache[tkr] = (m.get("status"), m.get("result"))
            return _cache[tkr]
        except Exception:
            if attempt == 1:
                time.sleep(0.5)
                continue
            _cache[tkr] = ("err", None)
            return _cache[tkr]


def load_live_core(reset_ts=None):
    d = json.load(open(BETS_FILE))
    bets = d if isinstance(d, list) else d.get("bets", [])
    core = [b for b in bets if b.get("action") == "trade" and b.get("crypto") in ("BTC", "ETH")
            and not b.get("dip_add") and b.get("window_end")
            and (not reset_ts or b.get("timestamp", "") >= reset_ts)]
    seen = {}  # dedup by timestamp, prefer a resolved copy
    for b in core:
        ts = b["timestamp"]
        if ts not in seen or (seen[ts].get("result") == "open" and b.get("result") in ("win", "loss")):
            seen[ts] = b
    return sorted(seen.values(), key=lambda b: (b.get("window_end"), b.get("timestamp")))


def resolve_opens(core):
    n = 0
    for b in core:
        if b.get("result") == "open":
            st, rv = kalshi_result(b.get("ticker", ""))
            time.sleep(0.1)
            if st in ("finalized", "settled") and rv in ("yes", "no"):
                b["result"] = "win" if rv == b.get("side") else "loss"
                n += 1
    return n


def expected_sizing(core):
    """Window-aware replay of the deployed strategy -> expected size per trade.
    Pooled scaling + press-winners + cool-off + split-guard, then the per-day
    count floor. Mirrors the bot's live logic exactly."""
    windows = OrderedDict()
    for b in core:
        windows.setdefault(b.get("window_end"), []).append(b)
    guard = set()
    if SPLIT_GUARD:
        for we, legs in windows.items():
            sd = {b["crypto"]: b for b in legs}
            if "BTC" in sd and "ETH" in sd and sd["BTC"].get("side") != sd["ETH"].get("side"):
                two = sorted([sd["BTC"], sd["ETH"]], key=lambda b: b.get("timestamp", ""))
                guard.add(id(two[1]))  # 2nd-placed leg is the one live can guard

    state = {"A": 1, "B": 1}; up_at = {"A": 0, "B": 0}; grp = {"A": [], "B": []}; allr = []
    base = {}   # id(bet) -> pre-floor scale (group scale + press + cool-off + split-guard)
    grp_of = {}
    for we, legs in windows.items():
        pa = list(allr); pg = {k: list(v) for k, v in grp.items()}
        streak = 0
        for w in reversed(pa):
            if w:
                streak += 1
            else:
                break
        results = []
        for b in legs:
            s = b.get("score", 0); name, cfg = group_of(s); scale = 1
            if name is not None:
                cur = state[name]; g = pg[name]
                if cur == 1:
                    if len(g) >= cfg["uw"] and sum(1 for w in g[-cfg["uw"]:] if w) >= cfg["ut"]:
                        state[name] = SCALE_UP; up_at[name] = len(g); cur = SCALE_UP
                else:
                    since = g[up_at[name]:]
                    if len(since) >= cfg["dw"] and sum(1 for w in since[-cfg["dw"]:] if not w) >= cfg["dt"]:
                        state[name] = 1; cur = 1
                scale = cur
                if scale > 1:
                    if PRESS_STREAK_N > 0 and streak >= PRESS_STREAK_N:
                        scale = max(scale, int(round(scale * PRESS_MULT)))       # press-winners
                    elif CO_WR > 0 and len(pa) >= CO_WIN:
                        wr = sum(1 for w in pa[-CO_WIN:] if w) / CO_WIN
                        if wr >= CO_WR and not (CO_STREAK > 0 and streak >= CO_STREAK):
                            scale = 1                                            # cool-off lid
            if id(b) in guard and scale > 1:
                scale = 1                                                        # split-guard
            if VOL_GATE > 0 and scale > 1:
                vol = (b.get("indicators") or {}).get("vol_6h")
                if vol is not None and vol < VOL_GATE:
                    scale = 1                                                    # vol gate
            base[id(b)] = scale
            grp_of[id(b)] = name
            results.append((name, b["result"] == "win"))
        for name, won in results:
            allr.append(won)
            if name in grp:
                grp[name].append(won)

    # Per-day count floor (window-grouped, matches the bot's _count_replay)
    final = {}   # id(bet) -> expected size after the count floor
    days = OrderedDict()
    for b in core:
        days.setdefault(b["timestamp"][:10], OrderedDict()).setdefault(b.get("window_end"), []).append(b)
    for day, wins in days.items():
        s_day = False; st = 0; nc = 0
        for we, legs in wins.items():
            if s_day and COUNT_REACT_K > 0 and st >= COUNT_REACT_K:
                s_day = False                     # reactivation at window start
            for b in legs:
                final[id(b)] = 1 if s_day else base[id(b)]
            for b in legs:
                won = b["result"] == "win"
                st = st + 1 if won else 0
                nc += 1 if won else -1
                if nc <= -COUNT_SOFT_C:
                    s_day = True
    return final, base, grp_of


def main():
    only_day = sys.argv[1] if len(sys.argv) > 1 else None
    reset_ts = get_reset_ts()
    core = load_live_core(reset_ts)
    if only_day:
        core = [b for b in core if b["timestamp"][:10] == only_day]
    if not core:
        print("No core trades found" + (f" for {only_day}" if only_day else "") + f" in {BETS_FILE}")
        return
    n = resolve_opens(core)
    core = [b for b in core if b.get("result") in ("win", "loss")]
    expected, base, grp_of = expected_sizing(core)

    print(f"Parity check — {BETS_FILE}")
    print(f"post-reset window since: {reset_ts or '(no reset_ts — using all trades)'}")
    print(f"config: base {SCALE_UP}x, PROTECT_MODE={PROTECT_MODE}, soft={COUNT_SOFT_C}/react={COUNT_REACT_K}, "
          f"press streak>={PRESS_STREAK_N}->{PRESS_MULT}x, split_guard={SPLIT_GUARD}")
    print(f"resolved {n} open trades via Kalshi; {len(core)} core trades\n")

    days = sorted(set(b["timestamp"][:10] for b in core))
    grand_live = grand_bt = 0.0
    rows = []
    for day in days:
        dtr = [b for b in core if b["timestamp"][:10] == day]
        live_tot = bt_tot = 0.0
        buckets = {}
        for b in dtr:
            price = b.get("fill_price") or b.get("price"); won = b["result"] == "win"
            live_n = b.get("contracts", 1) or 1
            bt_n = expected[id(b)]
            lp = net_pnl(price, won, live_n); bp = net_pnl(price, won, bt_n)
            live_tot += lp; bt_tot += bp
            diff = round(lp - bp, 2)
            if live_n == bt_n:
                cat = "match"
            elif base.get(id(b)) != bt_n and live_n == base.get(id(b)):
                cat = "live_not_floored"   # bot didn't apply the floor the backtest did
            elif base.get(id(b)) == bt_n and live_n != bt_n:
                cat = "scale_state"        # bot's group scale/press differed
            else:
                cat = "other"
            c = buckets.setdefault(cat, [0, 0.0]); c[0] += 1; c[1] += diff
            if abs(diff) > 0.005:
                rows.append((day, b, live_n, bt_n, base.get(id(b)), grp_of.get(id(b)), diff, cat))
        gap = live_tot - bt_tot
        grand_live += live_tot; grand_bt += bt_tot
        flag = "OK" if abs(gap) < 0.01 else "GAP"
        bkt = " ".join(f"{k}:{v[0]}(${v[1]:+.2f})" for k, v in buckets.items() if k != "match")
        print(f"  {day}  LIVE ${live_tot:+8.2f}   BACKTEST ${bt_tot:+8.2f}   gap ${gap:+7.2f}  [{flag}]  {bkt}")

    print(f"\n  TOTAL   LIVE ${grand_live:+8.2f}   BACKTEST ${grand_bt:+8.2f}   gap ${grand_live-grand_bt:+7.2f}")
    if rows:
        print(f"\nDivergent trades ({len(rows)}):")
        print(f"  {'day':<11}{'time':<6}{'cr':<4}{'res':<5}{'liveN':>6}{'btN':>5}{'base':>5}{'grp':>4}{'diff':>8}  cause")
        rows.sort(key=lambda r: -abs(r[6]))
        for day, b, ln, bn, bs, gp, diff, cat in rows[:25]:
            tm = (b.get("window_end") or "")[11:16]
            print(f"  {day:<11}{tm:<6}{b.get('crypto'):<4}{b['result']:<5}{ln:>6}{bn:>5}{str(bs):>5}{str(gp):>4}{diff:>+8.2f}  {cat}")
    else:
        print("\n✓ No divergent trades — live matches the backtest exactly.")


if __name__ == "__main__":
    main()
