"""Window-aware backtest harness for the CORE crypto-score strategy.

Mirrors crypto_score_bot.py's sizing brain exactly, in the same order:

    group scale (+press, +cool-off)  ->  count soft floor  ->  split guard
      ->  vol gate  ->  sig weights  ->  WR cap  ->  WR boost

WINDOW AWARENESS IS THE WHOLE POINT. BTC and ETH in the same 15-minute window
settle simultaneously, so live cannot know leg 1's result when it sizes leg 2.
Every size in a window is therefore decided from state frozen BEFORE the window,
and state only advances once the whole window has been consumed. A sequential
replay leaks the first leg's outcome into the second leg's size and inflated
earlier backtests roughly 3x.

Usage:
    from core_harness import load_core, run
    trades = load_core()
    res = run(trades, CFG)          # CFG defaults to the deployed config
"""
import json
import math
from collections import OrderedDict, defaultdict
from datetime import datetime, timedelta

BETS_FILE = "crypto_score_bets_combined.json"

# Deployed configuration (Railway, as of 2026-08-10)
CFG = {
    "base": 5,                     # SCALE_UP_COUNT
    "groups": {
        "A": {"signals": {4, 5}, "up_window": 4, "up_thresh": 3,
              "down_window": 16, "down_thresh": 3},
        "B": {"signals": {1, 2, 3}, "up_window": 6, "up_thresh": 4,
              "down_window": 16, "down_thresh": 4},
    },
    "cool_off_wr": 0.80,           # 0 disables
    "cool_off_window": 10,
    "cool_off_bypass_streak": 6,   # 0 disables
    "press_streak_n": 0,           # 0 = off (deployed)
    "press_mult": 1.5,
    "protect_mode": "count",       # "count" | "off"
    "count_soft_c": 3,
    "count_react_k": 1,
    "split_guard": True,
    "vol_gate": 0.20,              # 0 disables
    "sig_weights": {},             # removed from Railway
    "wr_cap_n": 150, "wr_cap": 0.75,          # 0 disables
    "wr_boost_n": 100, "wr_boost_lo": 0.55,
    "wr_boost_hi": 0.70, "wr_boost_mult": 2.0,  # <=1 disables
    # Burst: when the trailing burst_n core results are >= burst_thr, the next
    # burst_m WINDOWS trade at burst_size, overriding every other rule.
    # Counted in windows, not trades: both legs of a window settle together, so
    # consuming the counter leg-by-leg would make the result depend on which leg
    # the poll saw first (measured: a $340 swing on an arbitrary tiebreak).
    "burst_n": 0, "burst_thr": 1.0, "burst_m": 0, "burst_size": 30,
}


def fee(contracts, price):
    if not (0 < price < 1):
        return 0.0
    return math.ceil(0.07 * contracts * price * (1 - price) * 100) / 100.0


def _ts(s):
    return datetime.fromisoformat(s)


def _window_of(b):
    """window_end, reconstructing it for records that lost it (the Jun 26-Jul 17
    ticker-less set). Entries land in the last minutes of a 15-min window, so the
    next 15-minute boundary is the settle time."""
    we = b.get("window_end")
    if we:
        return we
    t = _ts(b["timestamp"])
    m = (t.minute // 15 + 1) * 15
    return (t.replace(minute=0, second=0, microsecond=0) + timedelta(minutes=m)).isoformat()


def load_core(path=BETS_FILE):
    """Resolved BTC/ETH core trades, grouped into settlement windows in time order."""
    raw = json.load(open(path))
    rows = [b for b in raw
            if b.get("action") == "trade" and b.get("result") in ("win", "loss")
            and b.get("crypto") in ("BTC", "ETH") and not b.get("dip_add")]
    for b in rows:
        b["_we"] = _window_of(b)
        b["_sig"] = (b.get("score") or 0) + 3
        b["_win"] = b["result"] == "win"
        b["_px"] = b.get("fill_price") or b.get("price")
        b["_day"] = b["timestamp"][:10]
    rows.sort(key=lambda b: (b["_we"], b["timestamp"]))
    windows = OrderedDict()
    for b in rows:
        windows.setdefault(b["_we"], []).append(b)
    return windows


def _wr(seq, n):
    if n <= 0 or len(seq) < n:
        return None
    last = seq[-n:]
    return sum(1 for x in last if x) / n


def _count_clip(day_windows, soft_c, react_k):
    """Net-win-count soft floor replayed window-by-window (mirrors _count_replay).
    day_windows: list of lists of bool wins, for windows already CONSUMED today."""
    s = False
    st = 0
    nc = 0
    for legs in day_windows:
        if s and react_k > 0 and st >= react_k:
            s = False
        for won in legs:
            st = st + 1 if won else 0
            nc += 1 if won else -1
            if nc <= -soft_c:
                s = True
    return s and not (react_k > 0 and st >= react_k)


def run(windows, cfg=None, size_only=False, trace=None):
    """Replay the strategy window-aware. Returns a dict of results."""
    c = dict(CFG)
    if cfg:
        c.update(cfg)
    groups = c["groups"]

    scale_state = {g: 1 for g in groups}
    scale_up_at = {g: 0 for g in groups}
    grp_hist = {g: [] for g in groups}      # resolved wins per group, in order
    core_hist = []                          # resolved wins, all core, in order
    day_windows = defaultdict(list)         # day -> list of consumed windows (bools)
    burst_left = 0                          # windows remaining in the current burst

    total = 0.0
    peak = 0.0
    dd = 0.0
    by_day = defaultdict(float)
    clip_counts = defaultdict(int)
    sizes = []
    equity = []

    for we, legs in windows.items():
        day = legs[0]["_day"]
        # ---- state frozen BEFORE this window ----
        floor_clip = False
        if c["protect_mode"] == "count":
            floor_clip = _count_clip(day_windows[day], c["count_soft_c"], c["count_react_k"])
        streak = 0
        for won in reversed(core_hist):
            if won:
                streak += 1
            else:
                break
        cool_wr = _wr(core_hist, c["cool_off_window"])
        wr_cap_val = _wr(core_hist, c["wr_cap_n"])
        wr_boost_val = _wr(core_hist, c["wr_boost_n"])
        # Burst arming reads ONLY pre-window history, same as every other rule.
        if c["burst_n"] and c["burst_m"] and burst_left <= 0:
            bv = _wr(core_hist, c["burst_n"])
            if bv is not None and bv >= c["burst_thr"]:
                burst_left = c["burst_m"]
        in_burst = burst_left > 0
        if in_burst:
            burst_left -= 1
        # split window = both cryptos, opposite sides, same window
        cryptos = {b["crypto"] for b in legs}
        sides = {b["side"] for b in legs}
        is_split = len(cryptos) > 1 and len(sides) > 1

        for i, b in enumerate(legs):
            if in_burst:
                n = c["burst_size"]
                reason = "burst"
                clip_counts[reason] += 1
                sizes.append(n)
                if trace is not None:
                    trace[(b["timestamp"], b["crypto"], b["side"])] = (n, reason)
                if not size_only:
                    px = b["_px"]
                    f = fee(n, px)
                    pnl = (n * (1 - px) - f) if b["_win"] else (-n * px - f)
                    total += pnl
                    by_day[day] += pnl
                    peak = max(peak, total)
                    dd = min(dd, total - peak)
                continue
            sig = b["_sig"]
            # -- group scale --
            gname = next((g for g, cf in groups.items() if sig in cf["signals"]), None)
            n = 1
            if sig != 0 and gname:
                cf = groups[gname]
                cur = scale_state[gname]
                hist = grp_hist[gname]
                if cur == 1:
                    if len(hist) >= cf["up_window"] and \
                       sum(1 for x in hist[-cf["up_window"]:] if x) >= cf["up_thresh"]:
                        scale_state[gname] = c["base"]
                        scale_up_at[gname] = len(hist)
                        cur = c["base"]
                else:
                    since = hist[scale_up_at[gname]:]
                    if len(since) >= cf["down_window"] and \
                       sum(1 for x in since[-cf["down_window"]:] if not x) >= cf["down_thresh"]:
                        scale_state[gname] = 1
                        cur = 1
                n = cur
            reason = "none"
            # -- press / cool-off --
            if n > 1:
                if c["press_streak_n"] > 0 and streak >= c["press_streak_n"]:
                    n = max(n, int(round(n * c["press_mult"])))
                    reason = "press"
                elif c["cool_off_wr"] > 0 and cool_wr is not None and cool_wr >= c["cool_off_wr"]:
                    if not (c["cool_off_bypass_streak"] > 0 and streak >= c["cool_off_bypass_streak"]):
                        n = 1
                        reason = "cool_off"
            # -- count soft floor --
            if n > 1 and floor_clip:
                n = 1
                reason = "soft_floor"
            # -- split guard (2nd leg only, matching live) --
            if n > 1 and c["split_guard"] and is_split and i > 0:
                n = 1
                reason = "split_guard"
            # -- vol gate --
            if n > 1 and c["vol_gate"] > 0:
                v = (b.get("indicators") or {}).get("vol_6h")
                if v is not None and v < c["vol_gate"]:
                    n = 1
                    reason = "vol_gate"
            # -- signal weights --
            if n > 1 and c["sig_weights"]:
                w = c["sig_weights"].get(sig)
                if w is not None and w != 1.0:
                    n = max(1, int(round(n * w)))
                    reason = "sig_weight"
            # -- WR ladder --
            if n > 1 and c["wr_cap"] > 0 and wr_cap_val is not None and wr_cap_val > c["wr_cap"]:
                n = 1
                reason = "wr_cap"
            if n > 1 and c["wr_boost_mult"] > 1 and wr_boost_val is not None and \
               c["wr_boost_lo"] <= wr_boost_val <= c["wr_boost_hi"]:
                n = max(1, int(round(n * c["wr_boost_mult"])))
                reason = "wr_boost"

            clip_counts[reason] += 1
            sizes.append(n)
            if trace is not None:
                trace[(b["timestamp"], b["crypto"], b["side"])] = (n, reason)
            if not size_only:
                px = b["_px"]
                f = fee(n, px)
                pnl = (n * (1 - px) - f) if b["_win"] else (-n * px - f)
                total += pnl
                by_day[day] += pnl
                peak = max(peak, total)
                dd = min(dd, total - peak)
        equity.append(total)
        # ---- advance state AFTER the whole window ----
        for b in legs:
            core_hist.append(b["_win"])
            g = next((gg for gg, cf in groups.items() if b["_sig"] in cf["signals"]), None)
            if g:
                grp_hist[g].append(b["_win"])
        day_windows[day].append([b["_win"] for b in legs])

    days = sorted(by_day)
    return {
        "pnl": round(total, 2),
        "maxdd": round(dd, 2),
        "ratio": round(total / abs(dd), 2) if dd else 0.0,
        "green": sum(1 for d in days if by_day[d] > 0),
        "red": sum(1 for d in days if by_day[d] < 0),
        "days": len(days),
        "worst_day": round(min(by_day.values()), 2) if by_day else 0.0,
        "best_day": round(max(by_day.values()), 2) if by_day else 0.0,
        "avg_size": round(sum(sizes) / len(sizes), 2) if sizes else 0,
        "trades": len(sizes),
        "clips": dict(clip_counts),
        "by_day": dict(by_day),
        "equity": equity,
    }


def subset(windows, lo=None, hi=None):
    """Windows whose day falls in [lo, hi]."""
    out = OrderedDict()
    for we, legs in windows.items():
        d = legs[0]["_day"]
        if (lo is None or d >= lo) and (hi is None or d <= hi):
            out[we] = legs
    return out


if __name__ == "__main__":
    w = load_core()
    r = run(w)
    print(f"windows {len(w)}  trades {r['trades']}  days {r['days']}")
    print(f"P&L ${r['pnl']:,.2f}   maxDD ${r['maxdd']:,.2f}   ratio {r['ratio']}")
    print(f"green/red {r['green']}/{r['red']}   avg size {r['avg_size']}")
    print(f"clips {r['clips']}")
