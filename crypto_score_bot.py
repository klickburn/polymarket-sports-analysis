"""
Kalshi 15-Minute Crypto Score Bot (v4)
=======================================
Trades all 7 cryptos on Kalshi's 15-min prediction markets using a
3-filter hard-kill scoring engine.

Strategy: "v4 Hard Filter"
  - In the last 4 minutes of each 15-min window (minute 11+)
  - Check which side is ≥78¢ (dominant side)
  - 3 hard filters: BTC Against (±0.3%), 3h Extended (>2%), High Vol (>0.6)
  - Score = 0 → GO, Score < 0 → SKIP

Usage:
    python3 crypto_score_bot.py              # Dry run
    python3 crypto_score_bot.py --live       # Place real bets
"""

import os
import sys
import json
import time
import math
import uuid
import urllib.request
from collections import OrderedDict, defaultdict
from datetime import datetime, timezone, timedelta

from crypto_15m_bot import (
    auth_get, auth_post, auth_delete, public_get,
    get_balance, get_existing_positions, get_open_orders,
    place_order, get_current_window, minutes_until_strike,
    find_current_market, get_dominant_side, P,
)

# ── Config ──────────────────────────────────────────────────────────────
BET_AMOUNT = float(os.environ.get("SCORE_BET_AMOUNT", "0.10"))
CONTRACT_COUNT = int(os.environ.get("SCORE_CONTRACT_COUNT", "1"))
ENTRY_AFTER_MINUTES = int(os.environ.get("SCORE_ENTRY_MINUTES", "11"))
POLL_INTERVAL = int(os.environ.get("SCORE_POLL_INTERVAL", "20"))
# Settlement drain: bounded so a backlog can never eat the pre-entry minutes, and
# checkpointed so an interrupted pass keeps what it resolved. 120 x ~0.5s ~= 1 min,
# which clears a normal ~40/window load with headroom and still chews a large
# backlog down over a few hours instead of never.
RESOLVE_MAX_PER_CYCLE = int(os.environ.get("RESOLVE_MAX_PER_CYCLE", "120"))
RESOLVE_SAVE_EVERY = int(os.environ.get("RESOLVE_SAVE_EVERY", "20"))
RESOLVE_SLEEP = float(os.environ.get("RESOLVE_SLEEP", "0.15"))
# Phantom trading: record a trade in full but place no order. The record still
# feeds the sizing brain (group scaling, cool-off, count floor, WR ladder), so
# the strategy's state evolves exactly as if it had traded — only the money is
# withheld. Modes:
#   "off"           every core trade is ordered for real
#   "boosted_only"  real orders solely for WR-boosted trades
#   "all"           no core trade is ever ordered; the book runs on paper
# Backtest over 77d: 1x trades net -$153, boosted net +$475.
# WARNING: "boosted_only" concentrates 100% of realised P&L into ~15% of trades,
# all of them selected by the WR ladder. PHANTOM_MODE=off reverts instantly.
# NOTE: this gate covers the CORE book only. Split dips and core dips are placed
# by _place_split_dips/_place_core_dips and stay REAL MONEY under every mode.
PHANTOM_MODE = os.environ.get("PHANTOM_MODE", "off").strip().lower()
MIN_PRICE = float(os.environ.get("SCORE_MIN_PRICE", "0.78"))
MAX_PRICE = float(os.environ.get("SCORE_MAX_PRICE", "0.99"))
MIN_SCORE = int(os.environ.get("SCORE_MIN_SCORE", "-3"))  # Signal count 0 = pts-3 = -3
MAX_SCORE = int(os.environ.get("SCORE_MAX_SCORE", "4"))  # Signal count 7 = pts-3 = 4
TAKE_PROFIT_PRICE = float(os.environ.get("SCORE_TAKE_PROFIT", "0.95"))
SCORE_VERSION = os.environ.get("SCORE_VERSION", "v4")
# Split-window dip orders: in windows where BTC and ETH are on opposite sides,
# rest limit buys at multiple price/size tiers on each crypto's own side —
# bounded-risk contrarian adds that fill only if that side collapses.
# Split-guard: in split windows (BTC/ETH opposite sides) one leg almost always
# loses (correlated underlyings), so trade split-window legs at 1x instead of the
# group scale. Legs land in separate polls ~96% of the time, so we can only guard
# the *second* leg (the first is already placed) — but that captures ~91% of the
# benefit in backtest (+$649 of +$712 over 57 days).
SPLIT_GUARD = os.environ.get("SPLIT_GUARD", "0") == "1"
# Volatility gate: momentum needs the market to move. Red days are LOW-volatility,
# choppy days where momentum whipsaws. So don't scale up when 6h volatility is
# below VOL_GATE — trade 1x instead. Backtest-validated (out-of-sample): fewer red
# days, lower drawdown, softer worst day, for ~4% total. vol_6h is recorded at
# entry, so this is deterministic -> parity-safe. 0 disables.
VOL_GATE = float(os.environ.get("VOL_GATE", "0"))
# Signal weights: signals inside a group all get the same scale, but their edges
# differ ~28x (signal 1 earns ~0.001/trade vs signal 2's 0.033 among scaled
# trades). Removing signal 1 hurts — its results feed group B's momentum history
# — so instead keep it in the history and trade it SMALLER. Format "1:0.5,6:0.5".
# Applied to the group scale at sizing time; signal is known at entry so this is
# deterministic -> parity-safe. Empty disables.
def _parse_sig_weights(s):
    out = {}
    for part in (s or "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            sig, w = part.split(":")
            out[int(sig)] = float(w)
        except ValueError:
            P(f"  [CONFIG] bad SIG_WEIGHTS entry '{part}' — ignored")
    return out
SIG_WEIGHTS = _parse_sig_weights(os.environ.get("SIG_WEIGHTS", ""))
# ── Win-rate ladder ────────────────────────────────────────────────────────
# Forward edge depends on the trailing win rate: it peaks around 0.70 and goes
# NEGATIVE above ~0.80 (euphoria — the run is exhausted). So size by where the
# rolling WR sits:
#     WR > WR_CAP           -> 1x   (euphoric, stop scaling)
#     WR_BOOST_LO..HI       -> xMULT (the productive band — lean in)
#     otherwise             -> normal group scale
# Windows are in TRADES (not time): ~142 core trades/day, so N=100 ~ 13h and
# N=150 ~ 21h. Pure win/loss counts -> deterministic -> parity-safe. 0 disables.
WR_CAP_N = int(os.environ.get("WR_CAP_N", "150"))
WR_CAP = float(os.environ.get("WR_CAP", "0"))          # 0 disables the cap
WR_BOOST_N = int(os.environ.get("WR_BOOST_N", "100"))
WR_BOOST_LO = float(os.environ.get("WR_BOOST_LO", "0.55"))
WR_BOOST_HI = float(os.environ.get("WR_BOOST_HI", "0.70"))
WR_BOOST_MULT = float(os.environ.get("WR_BOOST_MULT", "0"))  # 0/1 disables the boost


def _trailing_wr(bets, n):
    """Win rate over the last n RESOLVED core trades. Returns None until there
    are n of them. Window-aware by construction: only resolved (settled) trades
    count, and the current window's legs are still open when this is called."""
    if n <= 0:
        return None
    res = [b for b in bets
           if b.get("action") == "trade" and b.get("result") in ("win", "loss")
           and b.get("crypto") in ("BTC", "ETH") and not b.get("dip_add")]
    if len(res) < n:
        return None
    last = res[-n:]
    return sum(1 for b in last if b["result"] == "win") / n
SPLIT_DIP_ENABLED = os.environ.get("SPLIT_DIP_ENABLED", "0") == "1"
SPLIT_DIP_PRICE = float(os.environ.get("SPLIT_DIP_PRICE", "0.10"))
SPLIT_DIP_COUNT = int(os.environ.get("SPLIT_DIP_COUNT", "10"))
# Extra tiers as "price:count,price:count" — none; only the primary 10c tier
# (from SPLIT_DIP_PRICE/COUNT) is active. 20c and all others turned off.
_extra = os.environ.get("SPLIT_DIP_EXTRA_TIERS", "")
def _parse_tiers(s):
    tiers = []
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            p, c = part.split(":")
            tiers.append((float(p), int(c)))
        except Exception:
            pass
    return tiers
# Full tier list: primary (from SPLIT_DIP_PRICE/COUNT) + extras
SPLIT_DIP_TIERS = [(SPLIT_DIP_PRICE, SPLIT_DIP_COUNT)] + _parse_tiers(_extra)
# Everything beyond the primary tier is an EXPERIMENT. Its fills are tagged so
# the production book stays clean: mixing an untested 20c tier into the 10c
# numbers would silently change the win rate and break-even of the only book
# with a demonstrated edge. Filtering on dip_tier alone would be fragile -- the
# primary tier can be re-priced -- so intent is recorded explicitly.
_EXPERIMENT_TIERS = {p for p, _ in _parse_tiers(_extra)}

# ── Core dips: the same 10c recovery bet on NON-split windows ────────────
# Split dips only ever covered windows where BTC and ETH took opposite sides.
# A candlestick replay showed the other ~85% of windows are the larger and (for
# ETH-yes) stronger population. Now running all four cells -- BTC yes/no and
# ETH yes/no -- flat at 1 contract each, so every cell generates real fills
# instead of only the one the simulation liked. Sizing follows later, once the
# live win rates say which cells deserve it. CORE_DIP_ENABLED=0 turns it off.
CORE_DIP_ENABLED = os.environ.get("CORE_DIP_ENABLED", "0") == "1"
CORE_DIP_PRICE = float(os.environ.get("CORE_DIP_PRICE", "0.10"))
CORE_DIP_COUNT = int(os.environ.get("CORE_DIP_COUNT", "1"))
CORE_DIP_CRYPTOS = [c.strip().upper() for c in
                    os.environ.get("CORE_DIP_CRYPTOS", "BTC,ETH").split(",") if c.strip()]
# Per-side size override, "yes:25,no:1". Empty by default: every cell runs flat
# at CORE_DIP_COUNT so the four books stay comparable. Set it only to promote a
# cell once its live fills justify the size. Falls back to CORE_DIP_COUNT for
# any side not listed.
def _parse_side_sizes(sv):
    out = {}
    for part in (sv or "").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            side, n = part.split(":")
            out[side.strip().lower()] = int(n)
        except ValueError:
            pass
    return out
CORE_DIP_SIDE_SIZES = _parse_side_sizes(os.environ.get("CORE_DIP_SIDE_SIZES", ""))
# When a window reveals itself as a split AFTER a core dip is already resting,
# CORE_DIP_ABSORB=1 hands the contracts to the split tier (tags them
# split_window and shortens the top-up). Default 0: the core dip stays a core
# dip and the split tier rests its full size on top. Absorbing made sense while
# core was 25 contracts against a 100 split; at 1 contract it costs the core
# experiment ~18% of its fills to save 1% of split size.
CORE_DIP_ABSORB = os.environ.get("CORE_DIP_ABSORB", "0") == "1"


def _core_dip_size(side):
    """Contracts for this side — per-side override, else the flat count."""
    return CORE_DIP_SIDE_SIZES.get((side or "").lower(), CORE_DIP_COUNT)

DATA_DIR = os.environ.get("SCORE_DATA_DIR", "/data")
if not os.path.isdir(DATA_DIR):
    DATA_DIR = "."  # Fallback to current dir if volume not mounted
BETS_FILE = os.path.join(DATA_DIR, "crypto_score_bets.json")
STATUS_FILE = os.path.join(DATA_DIR, "crypto_score_status.json")

CRYPTOS = {
    "BTC":  {"series": "KXBTC15M"},
    "ETH":  {"series": "KXETH15M"},
    "SOL":  {"series": "KXSOL15M"},
    "XRP":  {"series": "KXXRP15M"},
    "DOGE": {"series": "KXDOGE15M"},
    "BNB":  {"series": "KXBNB15M"},
    "HYPE": {"series": "KXHYPE15M"},
}

COIN_IDS = {
    "BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana",
    "XRP": "ripple", "DOGE": "dogecoin",
    "BNB": "binancecoin", "HYPE": "hyperliquid",
}

COINGECKO = "https://api.coingecko.com/api/v3"
CG_API_KEYS = [
    os.environ.get("CG_API_KEY", "CG-djNqgGcv7UfYvqDfKsxWX1ii"),
    os.environ.get("CG_API_KEY_2", "CG-hx9L9wzotJeCZ1xeeLoJqJT9"),
    os.environ.get("CG_API_KEY_3", "CG-5sTc7yccYpF1zWVWfDduHT8i"),
]
_cg_key_index = 0

# ── CoinGecko data fetching ────────────────────────────────────────────
def fetch_coingecko(url, retries=3):
    global _cg_key_index
    sep = "&" if "?" in url else "?"
    for attempt in range(retries + 1):
        key = CG_API_KEYS[_cg_key_index % len(CG_API_KEYS)]
        _cg_key_index += 1
        full_url = f"{url}{sep}x_cg_demo_api_key={key}"
        try:
            req = urllib.request.Request(full_url, headers={"User-Agent": "Mozilla/5.0"})
            resp = urllib.request.urlopen(req, timeout=15)
            data = json.loads(resp.read().decode())
            if isinstance(data, dict) and "status" in data and "error_code" in data.get("status", {}):
                raise Exception(data["status"].get("error_message", "API error"))
            return data
        except Exception as e:
            if attempt < retries:
                wait = 1 + attempt * 2
                P(f"    CoinGecko retry {attempt+1}/{retries} with next key ({e})")
                time.sleep(wait)
            else:
                P(f"    CoinGecko FAILED for {url.split('/')[-2]}: {e}")
                return None


def aggregate_to_15m(prices_with_ts):
    """Aggregate ~5-min CoinGecko data into 15-min candle closes."""
    if not prices_with_ts:
        return []
    # Group by 15-min bucket, take last price in each bucket as close
    buckets = {}
    for ts_ms, price in prices_with_ts:
        bucket = (ts_ms // (15 * 60 * 1000)) * (15 * 60 * 1000)
        buckets[bucket] = price  # last price in bucket = close
    return [buckets[k] for k in sorted(buckets)]


def fetch_crypto_prices(skip_coins=None):
    """Fetch 24h price data from CoinGecko, aggregated to 15-min candles."""
    crypto_data = {}
    for i, (sym, cid) in enumerate(COIN_IDS.items()):
        if skip_coins and sym in skip_coins:
            continue
        url = f"{COINGECKO}/coins/{cid}/market_chart?vs_currency=usd&days=1"
        data = fetch_coingecko(url)
        if data and "prices" in data:
            raw = sorted(data["prices"], key=lambda x: x[0])
            candles = aggregate_to_15m(raw)
            if len(candles) >= 8:
                crypto_data[sym] = candles
        time.sleep(0.3)  # 100 req/min limit with API key
    return crypto_data


# ── Indicators ──────────────────────────────────────────────────────────
def calc_rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [max(d, 0) for d in deltas]
    losses = [abs(min(d, 0)) for d in deltas]
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    for i in range(period, len(deltas)):
        ag = (ag * (period - 1) + gains[i]) / period
        al = (al * (period - 1) + losses[i]) / period
    if al == 0:
        return 100
    return 100 - 100 / (1 + ag / al)


def calc_stoch(prices, period=14):
    if len(prices) < period:
        return 50
    window = prices[-period:]
    hi, lo = max(window), min(window)
    return (prices[-1] - lo) / (hi - lo) * 100 if hi != lo else 50


def compute_indicators(crypto_data):
    """Compute RSI, stochastic, momentum, volatility on 15-min candles."""
    indicators = {}
    for sym in CRYPTOS:
        if sym not in crypto_data:
            continue
        pr = crypto_data[sym]  # 15-min candle closes
        # 4 candles = 1h, 12 candles = 3h
        n4 = min(4, len(pr) - 1)
        n12 = min(12, len(pr) - 1)
        ret_1h = (pr[-1] - pr[-n4]) / pr[-n4] * 100 if pr[-n4] else 0
        ret_3h = (pr[-1] - pr[-n12]) / pr[-n12] * 100 if pr[-n12] else 0

        # vol_6h: avg absolute hourly returns over last 6h (6 hourly windows, each = 4 candles)
        hourly_rets = []
        for i in range(4, min(24, len(pr)), 4):
            idx = len(pr) - 1 - i
            idx_prev = len(pr) - 1 - i - 4
            if idx_prev >= 0 and pr[idx_prev]:
                hourly_rets.append(abs((pr[idx] - pr[idx_prev]) / pr[idx_prev] * 100))

        vol_6h = sum(hourly_rets) / len(hourly_rets) if hourly_rets else 0.5
        rsi = calc_rsi(pr[-min(60, len(pr)):], 14)     # RSI-14 on 15-min candles (~3.5h lookback)
        stoch = calc_stoch(pr[-min(30, len(pr)):], 14)  # Stoch-14 on 15-min candles (~3.5h lookback)
        indicators[sym] = {
            "ret_1h": ret_1h, "ret_3h": ret_3h,
            "vol_6h": vol_6h, "rsi": rsi, "stoch": stoch,
            "current_price": pr[-1],
        }

    # Pack agreement
    for sym in indicators:
        same = sum(1 for o in indicators if o != sym
                   and (indicators[o]["ret_1h"] >= 0) == (indicators[sym]["ret_1h"] >= 0))
        total = sum(1 for o in indicators if o != sym)
        indicators[sym]["pack_agreement"] = same / total if total else 0.5

    return indicators


# ── Scoring engines ────────────────────────────────────────────────────

def compute_score_v1(sym, side, price, indicators):
    """v1 — Original scoring engine. MIN_SCORE=0."""
    if sym not in indicators:
        return None, []
    ind = indicators[sym]
    now = datetime.now(timezone(timedelta(hours=-5)))
    s = 0
    reasons = []

    if price >= 0.97:
        s -= 3; reasons.append(("Price", f"{price:.0%}", "-3"))
    elif price >= 0.95:
        s -= 2; reasons.append(("Price", f"{price:.0%}", "-2"))
    elif price >= 0.93:
        s -= 1; reasons.append(("Price", f"{price:.0%}", "-1"))
    elif price < 0.70:
        s -= 1; reasons.append(("Price", f"{price:.0%}", "-1"))

    if sym == "HYPE":
        s -= 2; reasons.append(("Crypto", "HYPE", "-2"))
    elif sym == "BNB":
        s -= 1; reasons.append(("Crypto", "BNB", "-1"))
    elif sym == "XRP":
        s -= 1; reasons.append(("Crypto", "XRP", "-1"))
    elif sym == "ETH":
        s += 1; reasons.append(("Crypto", "ETH", "+1"))

    ret = ind["ret_1h"]
    if ret > 0.5 and side == "no":
        s -= 2; reasons.append(("Momentum", f"UP {ret:+.2f}% vs NO", "-2"))
    elif ret < -0.5 and side == "yes":
        s -= 2; reasons.append(("Momentum", f"DOWN {ret:+.2f}% vs YES", "-2"))

    btc_ret = indicators.get("BTC", {}).get("ret_1h", 0)
    if sym != "BTC":
        if btc_ret > 0.3 and side == "no":
            s -= 2; reasons.append(("BTC", "UP vs NO", "-2"))
        elif btc_ret < -0.3 and side == "yes":
            s -= 2; reasons.append(("BTC", "DOWN vs YES", "-2"))

    vol = ind["vol_6h"]
    if vol > 1.0:
        s -= 1; reasons.append(("Vol", "high", "-1"))
    elif vol < 0.3 and side == "no":
        s -= 1; reasons.append(("Vol", "calm+NO", "-1"))
    elif vol < 0.3 and side == "yes":
        s += 1; reasons.append(("Vol", "calm+YES", "+1"))

    rsi = ind["rsi"]
    if rsi > 70 and side == "yes":
        s -= 1; reasons.append(("RSI", f"{rsi:.0f}+YES", "-1"))
    elif rsi < 30 and side == "no":
        s -= 1; reasons.append(("RSI", f"{rsi:.0f}+NO", "-1"))

    if now.weekday() == 5:
        s -= 1; reasons.append(("Day", "Saturday", "-1"))

    stoch = ind["stoch"]
    if stoch > 80 and side == "yes":
        s -= 1; reasons.append(("Stoch", f"{stoch:.0f}+YES", "-1"))
    elif stoch < 20 and side == "no":
        s -= 1; reasons.append(("Stoch", f"{stoch:.0f}+NO", "-1"))

    pa = ind["pack_agreement"]
    if pa > 0.7:
        if ind["ret_1h"] > 0 and side == "no":
            s -= 1; reasons.append(("Pack", "up vs NO", "-1"))
        elif ind["ret_1h"] < 0 and side == "yes":
            s -= 1; reasons.append(("Pack", "down vs YES", "-1"))

    if abs(ind["ret_3h"]) > 2.0:
        s -= 1; reasons.append(("3h", f"{ind['ret_3h']:+.1f}%", "-1"))

    return s, reasons


def compute_score_v2(sym, side, price, indicators):
    """v2 — Safe mode weights. MIN_SCORE=-2."""
    if sym not in indicators:
        return None, []
    ind = indicators[sym]
    now = datetime.now(timezone(timedelta(hours=-5)))
    s = 0
    reasons = []

    if price >= 0.97:
        s -= 2; reasons.append(("Price", f"{price:.0%}", "-2"))
    elif price >= 0.95:
        s -= 2; reasons.append(("Price", f"{price:.0%}", "-2"))
    elif price >= 0.93:
        s -= 2; reasons.append(("Price", f"{price:.0%}", "-2"))
    elif price < 0.70:
        s -= 1; reasons.append(("Price", f"{price:.0%}", "-1"))

    if sym == "HYPE":
        s -= 2; reasons.append(("Crypto", "HYPE", "-2"))
    elif sym == "BNB":
        s -= 1; reasons.append(("Crypto", "BNB", "-1"))
    elif sym == "XRP":
        s -= 1; reasons.append(("Crypto", "XRP", "-1"))
    elif sym == "DOGE":
        s += 1; reasons.append(("Crypto", "DOGE", "+1"))
    elif sym == "ETH":
        s += 1; reasons.append(("Crypto", "ETH", "+1"))

    ret = ind["ret_1h"]
    if ret > 0.6 and side == "no":
        s -= 2; reasons.append(("Momentum", f"UP {ret:+.2f}% vs NO", "-2"))
    elif ret < -0.6 and side == "yes":
        s -= 2; reasons.append(("Momentum", f"DOWN {ret:+.2f}% vs YES", "-2"))

    btc_ret = indicators.get("BTC", {}).get("ret_1h", 0)
    if sym != "BTC":
        if btc_ret > 0.3 and side == "no":
            s -= 3; reasons.append(("BTC", "UP vs NO", "-3"))
        elif btc_ret < -0.3 and side == "yes":
            s -= 3; reasons.append(("BTC", "DOWN vs YES", "-3"))

    vol = ind["vol_6h"]
    if vol > 1.0:
        s -= 1; reasons.append(("Vol", "high", "-1"))
    elif vol < 0.3 and side == "yes":
        s -= 1; reasons.append(("Vol", "calm+YES", "-1"))

    rsi = ind["rsi"]
    if rsi > 65 and side == "yes":
        s -= 3; reasons.append(("RSI", f"{rsi:.0f}+YES", "-3"))
    elif rsi < 25 and side == "no":
        s -= 3; reasons.append(("RSI", f"{rsi:.0f}+NO", "-3"))

    stoch = ind["stoch"]
    if stoch > 80 and side == "yes":
        s -= 3; reasons.append(("Stoch", f"{stoch:.0f}+YES", "-3"))
    elif stoch < 10 and side == "no":
        s -= 3; reasons.append(("Stoch", f"{stoch:.0f}+NO", "-3"))

    pa = ind["pack_agreement"]
    if pa > 0.8:
        if ind["ret_1h"] > 0 and side == "no":
            s -= 1; reasons.append(("Pack", "up vs NO", "-1"))
        elif ind["ret_1h"] < 0 and side == "yes":
            s -= 1; reasons.append(("Pack", "down vs YES", "-1"))

    return s, reasons


def compute_score_v3(sym, side, price, indicators):
    """v3 — Optimizer weights round 2. MIN_SCORE=-2."""
    if sym not in indicators:
        return None, []
    ind = indicators[sym]
    s = 0
    reasons = []

    if price >= 0.97:
        s -= 2; reasons.append(("Price", f"{price:.0%}", "-2"))
    elif price >= 0.95:
        s -= 2; reasons.append(("Price", f"{price:.0%}", "-2"))
    elif price >= 0.93:
        s -= 2; reasons.append(("Price", f"{price:.0%}", "-2"))
    elif price < 0.70:
        s -= 1; reasons.append(("Price", f"{price:.0%}", "-1"))

    if sym == "HYPE":
        s -= 2; reasons.append(("Crypto", "HYPE", "-2"))
    elif sym == "BNB":
        s -= 1; reasons.append(("Crypto", "BNB", "-1"))
    elif sym == "XRP":
        s -= 1; reasons.append(("Crypto", "XRP", "-1"))
    elif sym == "DOGE":
        s += 1; reasons.append(("Crypto", "DOGE", "+1"))
    elif sym == "ETH":
        s += 2; reasons.append(("Crypto", "ETH", "+2"))

    ret = ind["ret_1h"]
    if ret > 0.6 and side == "no":
        s -= 1; reasons.append(("Momentum", f"UP {ret:+.2f}% vs NO", "-1"))
    elif ret < -0.6 and side == "yes":
        s -= 1; reasons.append(("Momentum", f"DOWN {ret:+.2f}% vs YES", "-1"))

    btc_ret = indicators.get("BTC", {}).get("ret_1h", 0)
    if sym != "BTC":
        if btc_ret > 0.3 and side == "no":
            s -= 3; reasons.append(("BTC", "UP vs NO", "-3"))
        elif btc_ret < -0.3 and side == "yes":
            s -= 3; reasons.append(("BTC", "DOWN vs YES", "-3"))

    vol = ind["vol_6h"]
    if vol > 1.0:
        s -= 1; reasons.append(("Vol", "high", "-1"))
    elif vol < 0.2 and side == "yes":
        s -= 1; reasons.append(("Vol", "calm+YES", "-1"))

    rsi = ind["rsi"]
    if rsi > 65 and side == "yes":
        s -= 3; reasons.append(("RSI", f"{rsi:.0f}+YES", "-3"))
    elif rsi < 25 and side == "no":
        s -= 3; reasons.append(("RSI", f"{rsi:.0f}+NO", "-3"))

    stoch = ind["stoch"]
    if stoch > 90 and side == "yes":
        s -= 3; reasons.append(("Stoch", f"{stoch:.0f}+YES", "-3"))
    elif stoch < 30 and side == "no":
        s -= 3; reasons.append(("Stoch", f"{stoch:.0f}+NO", "-3"))

    pa = ind["pack_agreement"]
    if pa > 0.5:
        if ind["ret_1h"] > 0 and side == "no":
            s -= 2; reasons.append(("Pack", "up vs NO", "-2"))
        elif ind["ret_1h"] < 0 and side == "yes":
            s -= 2; reasons.append(("Pack", "down vs YES", "-2"))

    return s, reasons


def compute_score_v4(sym, side, price, indicators):
    """v4 — 3 hard filters. MIN_SCORE=0. Score=0→GO, <0→SKIP."""
    if sym not in indicators:
        return None, []
    ind = indicators[sym]
    s = 0
    reasons = []

    btc_ret = indicators.get("BTC", {}).get("ret_1h", 0)
    if sym != "BTC":
        if btc_ret > 0.3 and side == "no":
            s -= 1; reasons.append(("BTC Against", f"BTC UP {btc_ret:+.2f}% vs NO", "-1"))
        elif btc_ret < -0.3 and side == "yes":
            s -= 1; reasons.append(("BTC Against", f"BTC DOWN {btc_ret:+.2f}% vs YES", "-1"))

    if abs(ind["ret_3h"]) > 2.0:
        s -= 1; reasons.append(("3h Extended", f"{ind['ret_3h']:+.1f}%", "-1"))

    vol = ind["vol_6h"]
    if vol > 0.6:
        s -= 1; reasons.append(("High Vol", f"{vol:.2f}", "-1"))

    btc_abs = abs(indicators.get("BTC", {}).get("ret_1h", 0))
    if btc_abs > 0.15:
        s -= 1; reasons.append(("BTC Move", f"|ret_1h|={btc_abs:.2f}% >0.15%", "-1"))
    else:
        reasons.append(("BTC OK", f"|ret_1h|={btc_abs:.2f}%", "pass"))

    return s, reasons


def compute_score_v5(sym, side, price, indicators):
    """v5 — Stoch + consensus + RSI alignment + vol filter. Score=0→GO, <0→SKIP.
    Filters:
      1. Stoch < 20 (oversold confirmation)
      2. All cryptos in window agree on direction (same side)
      3. RSI alignment: YES→RSI<50, NO→RSI>50
      4. vol_6h >= 0.2
    """
    if sym not in indicators:
        return None, []
    ind = indicators[sym]
    s = 0
    reasons = []

    # Filter 1: Stoch must be < 20
    stoch = ind["stoch"]
    if stoch >= 20:
        s -= 1; reasons.append(("Stoch High", f"{stoch:.1f} ≥20", "-1"))
    else:
        reasons.append(("Stoch OK", f"{stoch:.1f}", "pass"))

    # Filter 2: All cryptos must agree on direction (same side)
    # window_sides is injected into indicators by the trading loop
    window_sides = indicators.get("_window_sides", {})
    if window_sides:
        sides = [s for s in window_sides.values() if s]
        if sides and not all(s == sides[0] for s in sides):
            s -= 1; reasons.append(("No Consensus", f"{window_sides}", "-1"))
        elif sides:
            reasons.append(("Consensus", f"all {sides[0].upper()}", "pass"))

    # Filter 3: RSI alignment — YES needs RSI<50, NO needs RSI>50
    rsi = ind.get("rsi")
    if rsi is not None:
        if side == "yes" and rsi >= 50:
            s -= 1; reasons.append(("RSI Misalign", f"YES but RSI={rsi:.1f}≥50", "-1"))
        elif side == "no" and rsi <= 50:
            s -= 1; reasons.append(("RSI Misalign", f"NO but RSI={rsi:.1f}≤50", "-1"))
        else:
            reasons.append(("RSI Aligned", f"{side.upper()} RSI={rsi:.1f}", "pass"))

    # Filter 4: vol_6h must be >= 0.2
    vol = ind.get("vol_6h", 0)
    if vol < 0.2:
        s -= 1; reasons.append(("Vol Low", f"{vol:.2f} <0.2", "-1"))
    else:
        reasons.append(("Vol OK", f"{vol:.2f}", "pass"))

    return s, reasons


def compute_score_v6(sym, side, price, indicators):
    """v6 — Calm Overbought. Complementary to v5.
    When market is calm and price is extended, dominant side holds.
    Filters:
      1. Stoch > 85 (overbought — price near top of range)
      2. Vol < 0.4 (calm market)
      3. Price > 82c (strong conviction)
    """
    if sym not in indicators:
        return None, []
    ind = indicators[sym]
    s = 0
    reasons = []

    stoch = ind["stoch"]
    if stoch <= 85:
        s -= 1; reasons.append(("Stoch Low", f"{stoch:.1f} ≤85", "-1"))
    else:
        reasons.append(("Stoch OB", f"{stoch:.1f}", "pass"))

    vol = ind["vol_6h"]
    if vol >= 0.4:
        s -= 1; reasons.append(("Vol High", f"{vol:.2f} ≥0.4", "-1"))
    else:
        reasons.append(("Vol Calm", f"{vol:.2f}", "pass"))

    if price <= 0.82:
        s -= 1; reasons.append(("Price Low", f"{price:.2f} ≤82c", "-1"))
    else:
        reasons.append(("Price OK", f"{price:.2f}", "pass"))

    return s, reasons


def compute_score_v7(sym, side, price, indicators):
    """v7 — Signal Score: mean reversion with confirmation.
    6 independent checks, max 8 points. Need 3+ to trade.
    Signals:
      RSI Aligned (+2):      YES+RSI<50 or NO+RSI>50
      RSI Strong (+1 bonus): YES+RSI<35 or NO+RSI>65
      1h Mean Rev (+1):      YES+ret_1h<0 or NO+ret_1h>0
      Strong Mean Rev (+1):  same but |ret_1h|>0.3%
      BTC Against Side (+1): YES+btc_ret_1h<0 or NO+btc_ret_1h>0
      High Vol (+1 bonus):   vol_6h >= 0.3
      Consensus (+1):        pack_agreement = 1.0
      Stoch Oversold (+1):   stoch < 30
    """
    if sym not in indicators:
        return None, []
    ind = indicators[sym]
    pts = 0
    reasons = []

    rsi = ind.get("rsi")
    ret_1h = ind.get("ret_1h", 0)
    btc_ret_1h = ind.get("btc_ret_1h", 0)
    vol = ind.get("vol_6h", 0)
    stoch = ind.get("stoch", 50)
    pack = ind.get("pack_agreement", 0)

    # 1. RSI Aligned (+2)
    if (side == "yes" and rsi is not None and rsi < 50) or (side == "no" and rsi is not None and rsi > 50):
        pts += 2; reasons.append(("RSI Aligned", f"{side.upper()} RSI={rsi:.1f}", "+2"))
    else:
        reasons.append(("RSI Misalign", f"{side.upper()} RSI={rsi:.1f}" if rsi else "no RSI", "0"))

    # 2. RSI Strong (+1 bonus) — only if aligned
    if rsi is not None:
        if (side == "yes" and rsi < 35) or (side == "no" and rsi > 65):
            pts += 1; reasons.append(("RSI Strong", f"{rsi:.1f}", "+1"))

    # 3. 1h Mean Reversion (+1)
    if (side == "yes" and ret_1h < 0) or (side == "no" and ret_1h > 0):
        pts += 1; reasons.append(("1h MeanRev", f"ret={ret_1h:+.2f}%", "+1"))
    else:
        reasons.append(("1h NoRev", f"ret={ret_1h:+.2f}%", "0"))

    # 4. Strong Mean Rev (+1 bonus) — only if mean rev triggered and |ret_1h| > 0.3%
    if ((side == "yes" and ret_1h < 0) or (side == "no" and ret_1h > 0)) and abs(ret_1h) > 0.3:
        pts += 1; reasons.append(("Strong MR", f"|{ret_1h:+.2f}%|>0.3%", "+1"))

    # 5. BTC Against Side (+1)
    if (side == "yes" and btc_ret_1h < 0) or (side == "no" and btc_ret_1h > 0):
        pts += 1; reasons.append(("BTC Against", f"btc={btc_ret_1h:+.2f}%", "+1"))
    else:
        reasons.append(("BTC Same", f"btc={btc_ret_1h:+.2f}%", "0"))

    # 6. High Volatility (+1 bonus)
    if vol >= 0.3:
        pts += 1; reasons.append(("High Vol", f"{vol:.2f}≥0.3", "+1"))

    # 7. Consensus (disabled — always 0)
    window_sides = indicators.get("_window_sides", {})
    if window_sides:
        sides_list = [s for s in window_sides.values() if s]
        if sides_list and all(s == sides_list[0] for s in sides_list):
            reasons.append(("Consensus", f"all {sides_list[0].upper()}", "0"))
        else:
            reasons.append(("No Consensus", f"{window_sides}", "0"))

    # 8. Stoch Oversold (+1)
    if stoch < 30:
        pts += 1; reasons.append(("Stoch OS", f"{stoch:.1f}<30", "+1"))
    else:
        reasons.append(("Stoch High", f"{stoch:.1f}", "0"))

    # Need 3+ to trade — return pts-3 so >=0 means GO
    reasons.insert(0, ("Signal Score", f"{pts}/8", f"{pts}"))
    return pts - 3, reasons


def compute_score_v5v6(sym, side, price, indicators):
    """v5+v6 — Run both strategies, trade if EITHER passes."""
    s5, r5 = compute_score_v5(sym, side, price, indicators)
    s6, r6 = compute_score_v6(sym, side, price, indicators)

    if s5 is not None and s5 >= 0:
        return s5, [("Strategy", "v5", "pass")] + r5
    if s6 is not None and s6 >= 0:
        return s6, [("Strategy", "v6", "pass")] + r6

    # Both failed — return the one that was closer to passing
    if s5 is not None and s6 is not None:
        if s5 >= s6:
            return s5, [("Strategy", "v5 (best)", "pass")] + r5
        return s6, [("Strategy", "v6 (best)", "pass")] + r6
    return s5 or s6, r5 or r6


# ── Version dispatcher ─────────────────────────────────────────────────
SCORE_VERSIONS = {"v1": compute_score_v1, "v2": compute_score_v2,
                  "v3": compute_score_v3, "v4": compute_score_v4,
                  "v5": compute_score_v5, "v6": compute_score_v6,
                  "v5v6": compute_score_v5v6, "v7": compute_score_v7}

def compute_score(sym, side, price, indicators):
    fn = SCORE_VERSIONS.get(SCORE_VERSION, compute_score_v4)
    return fn(sym, side, price, indicators)


# ── Take profit ────────────────────────────────────────────────────────
# Kalshi splits markets across exchanges and the V2 order endpoint resolves the
# ticker WITHIN the exchange named by exchange_index -- omit it and you get a
# 404 market_not_found for a market that GET /markets happily returns. The 15m
# crypto series moved to exchange_index 2 on 2026-08-25; it is read from the
# market rather than hardcoded so a future move needs no code change.
_XIDX_CACHE = {}


def _exchange_index(ticker):
    """exchange_index for a ticker, or None if it cannot be determined."""
    if ticker in _XIDX_CACHE:
        return _XIDX_CACHE[ticker]
    xi = None
    try:
        m = (public_get(f"/markets/{ticker}") or {}).get("market") or {}
        xi = m.get("exchange_index")
    except Exception as e:
        P(f"    [XIDX] lookup failed for {ticker}: {e}")
    if xi is not None:
        _XIDX_CACHE[ticker] = xi
    return xi


def _cancel_order(order_id, ticker=None):
    """Cancel via the V2 endpoint, routed to the shard holding the order.

    exchange_index is required here too, and -1 is NOT usable: the API answers
    market_ticker_is_required_when_exchange_index=-1. So resolve the real shard
    from the ticker and pass that."""
    params = {}
    xi = _exchange_index(ticker) if ticker else None
    if xi is not None:
        params["exchange_index"] = xi
    return auth_delete(f"/portfolio/events/orders/{order_id}", params=params or None)


def place_dip_order(ticker, side, count, dip_price):
    """Rest a limit BUY of `count` contracts of `side` at `dip_price` (fills at
    that price or cheaper). Bounded-risk contrarian add for split windows."""
    # V2 API price is always the YES price
    api_side = "bid" if side == "yes" else "ask"
    if side == "yes":
        cents = max(1, int(round(dip_price * 100)))
    else:
        cents = min(99, int(round((1.0 - dip_price) * 100)))
    order = {
        "ticker": ticker,
        "side": api_side,
        "count": str(count),
        "price": f"{cents / 100:.2f}",
        "time_in_force": "good_till_canceled",
        "self_trade_prevention_type": "taker_at_cross",
        "client_order_id": str(uuid.uuid4()),
    }
    order["exchange_index"] = -1          # auto-route; see place_order
    try:
        P(f"    [DIP] resting BUY {count} {side.upper()} @ {dip_price*100:.0f}c ({ticker})")
        result = auth_post("/portfolio/events/orders", data=order)
        oid = result.get("order_id", "")
        if not oid:
            # Accepted but no id: the caller writes no record, so without this
            # the leg vanishes silently.
            P(f"    [DIP-FAIL] no order_id  {ticker} {side} n={count} @ "
              f"{dip_price*100:.0f}c  resp={str(result)[:200]}")
            return None
        return oid
    except Exception as e:
        # Full context on failure. 86.5% of split windows ended up with only ONE
        # leg dipped (577 of 667), and the cause was not diagnosable after the
        # fact because the old line logged the exception with no ticker, side,
        # size or response body. A dropped leg leaves no record at all, so this
        # log is the only trace it ever happened.
        body = ""
        try:
            body = f"  body={e.response.text[:200]}"
        except Exception:
            pass
        P(f"    [DIP-FAIL] {ticker} {side} n={count} @ {dip_price*100:.0f}c  "
          f"{type(e).__name__}: {e}{body}")
        return None


def _place_split_dips(bets, window_end_iso):
    """SPLIT windows only (BTC/ETH opposite sides): rest dip buys at every
    configured price/size tier on each crypto's own side, once both legs exist.
    Detects from recorded trades (legs land in separate polls). Returns True
    once dips are placed for the window."""
    if not SPLIT_DIP_ENABLED:
        return False
    # Idempotency: if a SPLIT dip already exists for this window, don't place
    # again (survives bot restarts mid-window, unlike the in-memory flag).
    # Must match on dip_type: a core dip in the same window is a different book,
    # and treating it as "already placed" silently cancelled the split dip.
    if any(b.get("dip_add") and (b.get("dip_type") or "split") == "split"
           and b.get("window_end") == window_end_iso for b in bets):
        return True
    wtr = {}
    for b in bets:
        if (b.get("action") == "trade" and b.get("crypto") in ("BTC", "ETH")
                and not b.get("dip_add") and b.get("window_end") == window_end_iso
                and b.get("result") not in ("unfilled",)):
            wtr[b["crypto"]] = b
    if len(wtr) < 2:
        return False
    sides = [wtr["BTC"].get("side"), wtr["ETH"].get("side")]
    if not all(sides) or sides[0] == sides[1]:
        return False  # not a split — no dips
    # This window is a confirmed split. A core dip may already be resting here
    # from before the second leg landed; absorb it rather than cancel (see
    # _absorb_core_dips) and subtract what it already holds from each tier.
    absorbed = _absorb_core_dips(bets, window_end_iso) if CORE_DIP_ENABLED else {}
    _split_leg_results = []
    tier_str = ", ".join(f"{c}@{p*100:.0f}c" for p, c in SPLIT_DIP_TIERS)
    P(f"  [DIP] split window ({sides[0]}/{sides[1]}) — resting tiers [{tier_str}] on both sides")
    for cr, b in wtr.items():
        for price, count in SPLIT_DIP_TIERS:
            # Only the tier at the core-dip price overlaps; deeper tiers are
            # untouched. Never go below zero, and skip the order entirely if the
            # core dip already covers the tier.
            if absorbed and abs(price - CORE_DIP_PRICE) < 1e-9:
                have = absorbed.get((cr, b["side"]), 0)
                if have:
                    count = max(0, count - have)
                    P(f"  [DIP] {cr} {price*100:.0f}c tier reduced by {have} "
                      f"already held from the core dip -> {count}")
            if count <= 0:
                continue
            oid = place_dip_order(b["ticker"], b["side"], count, price)
            _split_leg_results.append((cr, b["side"], price, count, bool(oid)))
            if oid:
                bets.append({
                    "crypto": cr, "ticker": b["ticker"], "side": b["side"],
                    "price": price, "score": b.get("score", 0),
                    "action": "trade", "result": "dip_pending", "dip_add": True,
                    "dip_type": "split", "dip_tier": f"{price*100:.0f}c",
                    "experiment": price in _EXPERIMENT_TIERS,
                    "order_id": oid, "contracts": count,
                    "event_ticker": b.get("event_ticker", ""),
                    "window_end": window_end_iso,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "strategy_version": SCORE_VERSION,
                })
                save_bets(bets)
            time.sleep(0.25)
    ok = [r for r in _split_leg_results if r[4]]
    bad = [r for r in _split_leg_results if not r[4]]
    if bad:
        P(f"  [DIP] SPLIT PARTIAL — {len(ok)}/{len(_split_leg_results)} legs placed. "
          f"missing: {[(r[0], r[1], f'{r[2]*100:.0f}c') for r in bad]}")
    else:
        P(f"  [DIP] split complete — {len(ok)}/{len(_split_leg_results)} legs placed")
    return True


def _absorb_core_dips(bets, window_end_iso):
    """A core dip was rested on a window that has since revealed itself as a
    split. Rather than cancel it — order cancellation on Kalshi has been
    unreliable here, and a failed cancel leaves us over-sized with no record of
    it — we KEEP the contracts and let the split tier top up the remainder.

    Two things happen:
      * the core dip is tagged split_window=True, so the core-dip experiment can
        exclude it (a split-window fill says nothing about non-split behaviour)
        while its P&L still counts as real money;
      * the contracts already resting are returned, so the split tier places
        SPLIT_DIP_COUNT minus what is already on the book and the window ends up
        at the intended size rather than core + split stacked on top.

    Nothing is deleted and no API call can fail: worst case the top-up is short
    by the core amount, never long.

    Only when CORE_DIP_ABSORB=1. By default the core dip is left alone: it keeps
    its own identity, stays in the core-dip book, and the split tier rests its
    full size on top. The window then carries CORE_DIP_COUNT extra contracts at
    the 10c tier -- 1 on top of 100 today. Handing the trade to the split book
    was worth it when core rested 25; at 1 contract it costs the core experiment
    roughly 18% of its fills to save 1% of split size. `became_split` is still
    recorded either way so these windows stay findable, but nothing filters on
    it."""
    absorbed = defaultdict(int)
    touched = 0
    for b in bets:
        if not (b.get("dip_add") and b.get("dip_type") == "core"
                and b.get("window_end") == window_end_iso):
            continue
        if b.get("result") in ("dip_expired", "unfilled"):
            continue                      # never made it to the book
        if not b.get("became_split"):
            b["became_split"] = True      # informational; nothing filters on it
            touched += 1
        if CORE_DIP_ABSORB:
            if not b.get("split_window"):
                b["split_window"] = True  # the tag the core book excludes on
            absorbed[(b.get("crypto"), b.get("side"))] += (b.get("contracts") or 0)
    if touched:
        save_bets(bets)
        if CORE_DIP_ABSORB:
            P(f"  [CORE-DIP] window became a split — tagged {touched} core dip(s) "
              f"split_window; split tier will top up the remainder")
        else:
            P(f"  [CORE-DIP] window became a split — keeping {touched} core dip(s) "
              f"as core; split tier rests its full size on top")
    return absorbed


def _place_core_dips(bets, window_end_iso):
    """NON-split windows: rest a dip buy on our own side, same idea as the split
    dip but on the windows the split rule never covered.

    Motivated by a candlestick replay of every core trade since 2026-07-09:
    99.9% of LOSING core trades collapse through 10c (they settle at zero, so
    they must), against 7.3% of winners. So a resting 10c buy is a bet on
    RECOVERY, and the recovery rate splits sharply by market:
        ETH yes  25.4% (n=390, z=+10.1, both split-halves significant)
        ETH no   10.1% | BTC yes 10.6% | BTC no 9.5%   (all ~breakeven)
    The simulation reproduces the live split-dip win rate to within 0.6pp
    overall and 0.1pp on the ETH-yes cell, and the dip candles carry real
    volume (median ~12.6k contracts, zero phantom prints).

    Running all four cells flat at 1 contract rather than only ETH-yes: the
    other three are expected to be coin flips, but a simulation that reproduces
    the split book to 0.6pp is still a simulation, and 1 contract is a cheap
    price for a real-fill answer on every cell. CORE_DIP_ENABLED=0 reverts."""
    if not CORE_DIP_ENABLED:
        return False
    # Per-CRYPTO, not per-window. The two legs of a window routinely arrive in
    # different polls (measured 22s apart), so latching the whole window on the
    # first placement meant a later-arriving leg never got a dip at all. BTC
    # would then only be sampled in windows where BTC happened to enter first,
    # which is not a random subset -- it would bias the four-cell comparison the
    # flat 1-contract run exists to make.
    done = {b.get("crypto") for b in bets
            if b.get("dip_add") and b.get("dip_type") == "core"
            and b.get("window_end") == window_end_iso}
    if done >= set(CORE_DIP_CRYPTOS):
        return True                      # every configured cell already covered
    wtr = {}
    for b in bets:
        if (b.get("action") == "trade" and b.get("crypto") in ("BTC", "ETH")
                and not b.get("dip_add") and b.get("window_end") == window_end_iso
                and b.get("result") not in ("unfilled",)):
            wtr[b["crypto"]] = b
    if not wtr:
        return False
    # NO time delay. A window with one leg may still become a split, but waiting
    # for certainty costs fills: the dip's whole job is to be resting when the
    # position collapses, and collapses in the held-back minutes would be missed
    # outright. That is tolerable for a 1-contract probe and not tolerable once
    # core dips are scaled. Instead we place immediately and let
    # _place_split_dips CANCEL this order if the window turns out to be a split
    # (see _cancel_core_dips). Second legs land a median 0s apart and 89% within
    # two minutes, so the cancel path is rare and the exposure is one resting
    # limit order for a couple of minutes.
    #
    # Skip SPLIT windows — those are the existing rule's territory. A split is
    # both cryptos on opposite sides; anything else is ours.
    if len(wtr) > 1:
        sides = [wtr["BTC"].get("side"), wtr["ETH"].get("side")]
        if all(sides) and sides[0] != sides[1]:
            return False
    for cr in CORE_DIP_CRYPTOS:
        if cr in done:
            continue                     # already rested for this cell
        b = wtr.get(cr)
        if not b or not b.get("side"):
            continue
        _n = _core_dip_size(b["side"])
        if _n <= 0:
            continue
        oid = place_dip_order(b["ticker"], b["side"], _n, CORE_DIP_PRICE)
        if oid:
            bets.append({
                "crypto": cr, "ticker": b["ticker"], "side": b["side"],
                "price": CORE_DIP_PRICE, "score": b.get("score", 0),
                "action": "trade", "result": "dip_pending", "dip_add": True,
                "dip_type": "core", "dip_tier": f"{CORE_DIP_PRICE*100:.0f}c",
                "order_id": oid, "contracts": _n,
                "event_ticker": b.get("event_ticker", ""),
                "window_end": window_end_iso,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "strategy_version": SCORE_VERSION,
                "indicators": b.get("indicators"),
            })
            save_bets(bets)
            done.add(cr)
            P(f"  [CORE-DIP] {cr} {b['side']} — rested {_n}x @ "
              f"{CORE_DIP_PRICE*100:.0f}c (non-split window)")
        time.sleep(0.25)
    # Latch the window only when every configured cell is covered. Returning
    # True as soon as ANYTHING was placed is what stranded the second leg: the
    # caller set core_dips_done_this_window and never called again. A crypto
    # that never trades this window simply keeps this False, which costs one
    # cheap re-check per poll until the window rolls.
    return done >= set(CORE_DIP_CRYPTOS)


def _is_split_leg(bets, trade_queue, crypto, side, window_end_iso):
    """True if the OTHER crypto (BTC<->ETH) is on the opposite side in this same
    window — i.e. this trade is a leg of a split. Checks both already-recorded
    trades (the common case: the other leg landed in an earlier poll) and the
    current trade_queue (the rare same-poll case, which guards both legs)."""
    other = "ETH" if crypto == "BTC" else "BTC"
    for b in bets:
        if (b.get("action") == "trade" and b.get("crypto") == other
                and not b.get("dip_add") and b.get("window_end") == window_end_iso
                and b.get("result") not in ("unfilled",)
                and b.get("side") and b.get("side") != side):
            return True
    for tq in trade_queue:
        if tq.get("crypto") == other and tq.get("side") and tq.get("side") != side:
            return True
    return False


def place_take_profit(ticker, side, count):
    """Place a limit sell order at TAKE_PROFIT_PRICE to lock in gains."""
    if TAKE_PROFIT_PRICE <= 0:
        return None
    # V2 API price is always YES price
    # Selling YES = ask at TP price, Selling NO = bid at (1-TP) price
    api_side = "ask" if side == "yes" else "bid"
    if side == "yes":
        tp_cents = int(round(TAKE_PROFIT_PRICE * 100))
    else:
        tp_cents = int(round((1.0 - TAKE_PROFIT_PRICE) * 100))
    price_dollar_str = f"{tp_cents / 100:.2f}"
    order = {
        "ticker": ticker,
        "side": api_side,
        "count": str(count),
        "price": price_dollar_str,
        "time_in_force": "good_till_canceled",
        "self_trade_prevention_type": "taker_at_cross",
        "client_order_id": str(uuid.uuid4()),
    }
    order["exchange_index"] = -1          # auto-route; see place_order
    try:
        P(f"    Take-profit: SELL {count} @ {tp_cents}c ({side.upper()})")
        result = auth_post("/portfolio/events/orders", data=order)
        order_id = result.get("order_id", "")
        P(f"    TP order {order_id}: placed")
        return {"order": {"order_id": order_id, "status": "resting"}}
    except Exception as e:
        P(f"    TP ORDER FAILED: {e}")
        return None


# ── Load/save bets ──────────────────────────────────────────────────────
def _try_load_json(path):
    """Try to load JSON from a file, return (data, error) tuple."""
    if not os.path.exists(path):
        return None, "not found"
    try:
        with open(path) as f:
            data = json.load(f)
        return data, None
    except json.JSONDecodeError as e:
        # Try to salvage by reading up to the last valid ']'
        try:
            with open(path) as f:
                raw = f.read()
            last_bracket = raw.rfind(']')
            if last_bracket > 0:
                repaired = json.loads(raw[:last_bracket + 1])
                return repaired, None
        except Exception:
            pass
        return None, str(e)
    except Exception as e:
        return None, str(e)


def _fetch_github_bets():
    """Fetch bets from GitHub repo backup via raw URL."""
    repo = os.environ.get("GITHUB_REPO", "klickburn/polymarket-sports-analysis")
    urls = [
        f"https://raw.githubusercontent.com/{repo}/main/crypto_score_bets.json",
        f"https://api.github.com/repos/{repo}/contents/crypto_score_bets.json",
    ]
    for url in urls:
        try:
            P(f"  Trying GitHub restore: {url[:60]}...")
            req = urllib.request.Request(url, headers={"User-Agent": "score-bot"})
            resp = urllib.request.urlopen(req, timeout=120)
            raw = resp.read().decode()
            # Contents API returns JSON with download_url for large files
            if '"download_url"' in raw[:500]:
                import base64
                meta = json.loads(raw)
                if meta.get("encoding") == "base64" and meta.get("content"):
                    raw = base64.b64decode(meta["content"]).decode()
                elif meta.get("download_url"):
                    P(f"  Following download_url...")
                    req2 = urllib.request.Request(meta["download_url"], headers={"User-Agent": "score-bot"})
                    resp2 = urllib.request.urlopen(req2, timeout=120)
                    raw = resp2.read().decode()
            data = json.loads(raw)
            P(f"  GitHub backup: fetched {len(data)} bets")
            return data
        except Exception as e:
            P(f"  GitHub fetch failed ({url[:40]}...): {e}")
    return None


def _merge_bets(local, remote):
    """Merge two bet lists, dedup by ticker+timestamp, sorted by timestamp."""
    seen = set()
    merged = []
    for b in local + remote:
        if not b.get("ticker"):
            continue
        key = (b.get("ticker", ""), b.get("timestamp", ""))
        if key not in seen:
            seen.add(key)
            merged.append(b)
    merged.sort(key=lambda x: x.get("timestamp", ""))
    return merged


_github_merged = False

def _reset_cutoff():
    """Timestamp of the last reset; bets older than this are permanently dropped."""
    try:
        with open(os.path.join(DATA_DIR, ".reset_ts")) as f:
            return f.read().strip()
    except Exception:
        return ""

def load_bets():
    global _github_merged, _scale_state, _scale_up_at
    # Check for reset flag from dashboard
    reset_flag = os.path.join(DATA_DIR, ".reset_flag")
    if os.path.exists(reset_flag):
        P("  [RESET] Reset flag detected — clearing all bets and scale state")
        os.remove(reset_flag)
        save_bets([])
        # Streak history is gone; scale state derived from it must reset too,
        # otherwise a group stuck at 10x could never scale down
        _scale_state = {k: 1 for k in SCALE_GROUPS}
        _scale_up_at = {}
        _persist_scale_state()
        return []

    # Try main file first
    data, err = _try_load_json(BETS_FILE)

    if data is None:
        if err and err != "not found":
            P(f"  WARNING: Bets file corrupted ({err})")
            corrupted = BETS_FILE + ".corrupted"
            try:
                os.rename(BETS_FILE, corrupted)
            except Exception:
                pass

        # Try .bak backup
        bak_file = BETS_FILE + ".bak"
        data, err2 = _try_load_json(bak_file)
        if data is not None:
            P(f"  RECOVERED {len(data)} bets from .bak backup!")

    if data is None:
        # Try .tmp
        tmp_file = BETS_FILE + ".tmp"
        data, err3 = _try_load_json(tmp_file)
        if data is not None:
            P(f"  RECOVERED {len(data)} bets from .tmp file!")

    if data is None:
        data = []

    before_filter = len(data)
    data = [b for b in data if b.get("ticker")]
    if len(data) < before_filter:
        P(f"  Removed {before_filter - len(data)} entries with no ticker")
        save_bets(data)

    # Merge with GitHub backup once on startup to recover missing historical bets
    if not _github_merged:
        _github_merged = True
        github_bets = _fetch_github_bets()
        if github_bets is not None and len(github_bets) == 0 and len(data) > 0:
            P(f"  GitHub file is empty — reset detected, clearing {len(data)} local bets")
            data = []
            save_bets(data)
        elif github_bets:
            before = len(data)
            data = _merge_bets(data, github_bets)
            added = len(data) - before
            if added > 0:
                P(f"  Merged {added} missing bets from GitHub backup (total: {len(data)})")
                save_bets(data)
            elif before == 0:
                P(f"  RESTORED {len(data)} bets from GitHub backup!")
                save_bets(data)
            else:
                P(f"  GitHub backup checked — no missing bets ({len(data)} total)")
        elif len(data) == 0:
            P(f"  No local or GitHub bets found — starting fresh")

    # Permanently drop anything older than the last reset (guards against
    # resurrection from GitHub backups or stale in-memory copies)
    cutoff = _reset_cutoff()
    if cutoff:
        before = len(data)
        data = [b for b in data if b.get("timestamp", "") >= cutoff]
        if len(data) < before:
            P(f"  [RESET] Dropped {before - len(data)} bets older than reset at {cutoff}")
            save_bets(data)

    return data


_last_seen_mtime = 0.0


def _adopt_foreign_resolutions(bets):
    """dashboard_server runs score_resolve_loop in a sibling thread that rewrites
    this same file every 60s. The bot loads `bets` once per 15-minute window and
    re-saves after every trade, so a blind write discards every outcome that
    thread booked while we held a stale copy — which is why the open-bet backlog
    grew instead of draining, and why not one 'expired' record ever survived.

    Re-read only when someone else has actually touched the file (mtime check, so
    this costs a stat() rather than a 20MB read on every save), and adopt any
    outcome that landed underneath us. New trades in memory always win; we only
    ever take resolutions, never rows, so a reset can't be resurrected."""
    global _last_seen_mtime
    try:
        mt = os.path.getmtime(BETS_FILE)
    except OSError:
        return bets
    if mt <= _last_seen_mtime:
        return bets
    disk, _ = _try_load_json(BETS_FILE)
    _last_seen_mtime = mt
    if not disk:
        return bets
    RESOLVED = ("win", "loss", "expired", "unfilled")
    idx = {}
    for b in disk:
        k = (b.get("timestamp"), b.get("crypto"), b.get("side"))
        if None not in k and b.get("result") in RESOLVED:
            idx[k] = b
    adopted = 0
    for b in bets:
        if b.get("result") != "open":
            continue
        d = idx.get((b.get("timestamp"), b.get("crypto"), b.get("side")))
        if not d:
            continue
        for f in ("result", "pnl", "market_result", "fill_price", "filled_count", "status"):
            if d.get(f) is not None:
                b[f] = d[f]
        adopted += 1
    if adopted:
        P(f"  [MERGE] adopted {adopted} resolution(s) booked by the resolve thread")
    return bets


def _note_own_write():
    """Record our own mtime so the next save doesn't re-read a file we just wrote."""
    global _last_seen_mtime
    try:
        _last_seen_mtime = os.path.getmtime(BETS_FILE)
    except OSError:
        pass


def save_bets(bets):
    # Fold in anything the sibling resolve thread settled while we held this list,
    # otherwise this write silently reverts it.
    bets = _adopt_foreign_resolutions(bets)
    # Atomic write: write to temp file, then rename (prevents corruption on crash)
    tmp_file = BETS_FILE + ".tmp"
    try:
        with open(tmp_file, "w") as f:
            json.dump(bets, f, indent=2, default=str)
            f.flush()
            os.fsync(f.fileno())
        # Keep one rolling backup
        if os.path.exists(BETS_FILE):
            backup_file = BETS_FILE + ".bak"
            try:
                os.replace(BETS_FILE, backup_file)
            except Exception:
                pass
        os.replace(tmp_file, BETS_FILE)
        _note_own_write()
    except OSError:
        # Fallback: direct write if atomic rename fails (Railway volume issue)
        with open(BETS_FILE, "w") as f:
            json.dump(bets, f, indent=2, default=str)
            f.flush()
            os.fsync(f.fileno())
        _note_own_write()


def save_status(status):
    tmp = STATUS_FILE + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(status, f, indent=2, default=str)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, STATUS_FILE)
    except OSError:
        with open(STATUS_FILE, "w") as f:
            json.dump(status, f, indent=2, default=str)
            f.flush()
            os.fsync(f.fileno())


# ── Git backup ─────────────────────────────────────────────────────────
GIT_BETS_FILE = os.path.join(os.path.dirname(__file__) or ".", "crypto_score_bets.json")
_last_git_backup = 0
GIT_BACKUP_INTERVAL = 900  # 15 min

def git_backup_bets(bets, force=False):
    """Back up bets to GitHub repo via API (works without git CLI credentials)."""
    global _last_git_backup
    now = time.time()
    if not force and now - _last_git_backup < GIT_BACKUP_INTERVAL:
        return
    _last_git_backup = now
    gh_token = os.environ.get("GITHUB_TOKEN", "")
    repo = os.environ.get("GITHUB_REPO", "klickburn/polymarket-sports-analysis")
    if not gh_token:
        return
    try:
        import base64
        content = json.dumps(bets, indent=2, default=str)
        encoded = base64.b64encode(content.encode()).decode()
        file_path = "crypto_score_bets.json"
        api_url = f"https://api.github.com/repos/{repo}/contents/{file_path}"
        # Get current file SHA (needed for updates)
        req = urllib.request.Request(api_url, headers={
            "Authorization": f"token {gh_token}",
            "Accept": "application/vnd.github.v3+json",
        })
        sha = None
        try:
            resp = urllib.request.urlopen(req, timeout=15)
            data = json.loads(resp.read())
            sha = data.get("sha", "")
        except Exception:
            pass  # File doesn't exist yet
        # Create/update file
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        payload = {
            "message": f"data: score bot bets {ts}",
            "content": encoded,
            "committer": {"name": "score-bot", "email": "bot@kalshi-bot.local"},
        }
        if sha:
            payload["sha"] = sha
        body = json.dumps(payload).encode()
        req = urllib.request.Request(api_url, data=body, method="PUT", headers={
            "Authorization": f"token {gh_token}",
            "Accept": "application/vnd.github.v3+json",
            "Content-Type": "application/json",
        })
        urllib.request.urlopen(req, timeout=30)
        P(f"  [GIT] Backed up {len(bets)} bets to GitHub")
    except Exception as e:
        P(f"  [GIT] Backup failed: {e}")


# ── Dynamic contract sizing (grouped signals, BTC+ETH combined) ────────
# Split up/down windows; down-check only counts trades since last scale-up
# Group A: signals 4,5 — up: 3/4 wins, down: 3 losses in 16 (since scale-up)
# Group B: signals 1,2,3 — up: 4/6 wins, down: 4 losses in 16 (since scale-up)
# Signals 0,6,7 never scale and don't feed streak history
# (0 = coin-flip noise; 6,7 = negative EV in both halves of backtest data)
SCALE_GROUPS = {
    "A": {"signals": {4, 5}, "up_window": 4, "up_thresh": 3, "down_window": 16, "down_thresh": 3},
    "B": {"signals": {1, 2, 3}, "up_window": 6, "up_thresh": 4, "down_window": 16, "down_thresh": 4},
}
SCALE_UP_COUNT = int(os.environ.get("SCALE_UP_COUNT", 5))
COOL_OFF_WR = float(os.environ.get("COOL_OFF_WR", "0.8"))       # 0 disables
COOL_OFF_WINDOW = int(os.environ.get("COOL_OFF_WINDOW", "10"))
# Bypass the lid during a clean consecutive win streak of >= N (0 disables).
# Distinguishes a durable run (ride it at full size) from choppy euphoria.
COOL_OFF_BYPASS_STREAK = int(os.environ.get("COOL_OFF_BYPASS_STREAK", "6"))
# Intraday trailing stop: once the day's CORE P&L peaks >= ARM, if it gives back
# GIVEBACK from that peak, drop to 1x for the rest of the day. Dips excluded so
# they can't arm or trigger it.
TRAIL_STOP_ENABLED = os.environ.get("TRAIL_STOP_ENABLED", "0") == "1"
# Dynamic thresholds: scale arm/giveback with SCALE_UP_COUNT so the stop stays
# calibrated automatically when the multiplier changes — no manual re-tuning.
# Anchored on the backtest-optimal 20/18 ratio at 30x: arm = 0.667/x,
# giveback = 0.60/x  ->  20/18 at 30x, 13.3/12 at 20x, 6.7/6 at 10x.
# TRAIL_STOP_DYNAMIC=0 falls back to the fixed TRAIL_STOP_ARM/GIVEBACK dollars.
TRAIL_STOP_DYNAMIC = os.environ.get("TRAIL_STOP_DYNAMIC", "0") == "1"
TRAIL_ARM_PER_X = float(os.environ.get("TRAIL_ARM_PER_X", "0.6667"))
TRAIL_GIVE_PER_X = float(os.environ.get("TRAIL_GIVE_PER_X", "0.60"))
if TRAIL_STOP_DYNAMIC:
    TRAIL_STOP_ARM = round(TRAIL_ARM_PER_X * SCALE_UP_COUNT, 2)
    TRAIL_STOP_GIVEBACK = round(TRAIL_GIVE_PER_X * SCALE_UP_COUNT, 2)
else:
    TRAIL_STOP_ARM = float(os.environ.get("TRAIL_STOP_ARM", "40"))
    TRAIL_STOP_GIVEBACK = float(os.environ.get("TRAIL_STOP_GIVEBACK", "20"))
# Streak-hold override: while on a K-consecutive-win streak, lift the trailing stop
# (a durable run keeps its edge — same idea as the cool-off bypass). 0 disables it,
# leaving the plain one-way daily latch. K>0 turns the stop into a per-trade toggle
# that can fire, lift on a K-win streak, and re-fire when the streak breaks.
TRAIL_STREAK_HOLD_K = int(os.environ.get("TRAIL_STREAK_HOLD_K", "0"))
# Daily loss floor: the downside stop. The trailing stop only protects gains, so a
# day that's red from the open bleeds unprotected. The floor drops to 1x once the
# day's core P&L falls to -FLOOR, and reactivates on a FLOOR_REACT_K-win streak (a
# recovery signal — same idea as the streak-hold, applied to the downside). It
# re-fires if the day bleeds back below the floor. Scales with SCALE_UP like the
# trailing stop. TRAIL_LOSS_FLOOR_PER_X=0 disables it.
TRAIL_LOSS_FLOOR_PER_X = float(os.environ.get("TRAIL_LOSS_FLOOR_PER_X", "0"))
TRAIL_FLOOR_REACT_K = int(os.environ.get("TRAIL_FLOOR_REACT_K", "1"))
TRAIL_LOSS_FLOOR = round(TRAIL_LOSS_FLOOR_PER_X * SCALE_UP_COUNT, 2)
# Hard backstop: a deeper floor that latches with NO reactivation. The soft floor
# reactivates on a win (to catch dip-then-rip days), but a day that bleeds all the
# way to -HARD_FLOOR is genuinely bad — stop for the rest of the day, full stop.
# Prevents a runaway day from re-exposing over and over. Scales with SCALE_UP too.
TRAIL_HARD_FLOOR_PER_X = float(os.environ.get("TRAIL_HARD_FLOOR_PER_X", "0"))
TRAIL_HARD_FLOOR = round(TRAIL_HARD_FLOOR_PER_X * SCALE_UP_COUNT, 2)
# ── Count-based protection (parity-safe rebuilt strategy) ──────────────────
# The dollar trailing stop/floors made live diverge from backtest because their
# thresholds depend on realized P&L, which differs by trade SIZE. Count-based
# protection keys off the win/loss SEQUENCE (identical live vs backtest), so the
# clip decisions match exactly. PROTECT_MODE="count" uses the day's net-win-count
# (+1 win / -1 loss): clip to 1x once it falls to -COUNT_SOFT_C, reactivate on a
# COUNT_REACT_K-win streak, re-fire on the next dip. No dollar/trailing/hard floor.
PROTECT_MODE = os.environ.get("PROTECT_MODE", "dollar")   # "count" for rebuilt
COUNT_SOFT_C = int(os.environ.get("COUNT_SOFT_C", "3"))
COUNT_REACT_K = int(os.environ.get("COUNT_REACT_K", "1"))
# Press-winners: a CLEAN consecutive-win streak carries persistent momentum, so on
# a streak >= PRESS_STREAK_N boost the group scale by PRESS_MULT (and bypass the
# cool-off lid — a durable run keeps its edge). 0 disables. Streak counts resolved
# BTC/ETH core wins in a row. Backtest: streak>=5 -> 1.5x is the robust choice.
PRESS_STREAK_N = int(os.environ.get("PRESS_STREAK_N", "0"))
PRESS_MULT = float(os.environ.get("PRESS_MULT", "1.5"))
_trail_day = None
_trail_peak = 0.0
_trailing_stopped = False
# Snapshot of the trailing-stop/floor state from the last _update_trailing_stop
# call, so per-trade sizing can stamp exactly what the protection saw. Makes
# live-vs-backtest reconciliation a lookup instead of a forensic replay.
_trail_dbg = {"peak": 0.0, "day_pnl": 0.0, "armed": False, "stopped": False,
              "floored": False, "hard_stopped": False, "streak": 0, "resolved": 0}


def _trail_replay(seq, arm, give, k, floor=0.0, react_k=0, hard_floor=0.0):
    """Replay one UTC day's realized CORE trades through the trailing stop + loss
    floor and return whether the NEXT trade should be clipped to 1x, plus diagnostics.

    seq: list of (realized_pnl, won_bool) in order.
      - Trailing stop: arm at `arm`, fire on `give` giveback from peak; k>0 lifts it
        on a k-win streak (streak-hold), k<=0 is a one-way daily latch.
      - Loss floor: fire (1x) once cum <= -floor; react_k>0 reactivates on a
        react_k-win streak; re-fires if it bleeds back below -floor. floor<=0 off.
      - Hard backstop: fire (1x) once cum <= -hard_floor and LATCH — never
        reactivates for the rest of the day. hard_floor<=0 off.
    Pure function of the realized sequence -> deterministic and restart-proof.
    Returns (clip_next, peak, cum, streak, ever_stopped)."""
    cum = 0.0; peak = 0.0; s = False; st = 0; fl = False; hf = False; ever = False
    for pnl, won in seq:
        if k > 0 and s and st >= k:
            s = False                       # trailing lift: the run is alive again
        if fl and react_k > 0 and st >= react_k:
            fl = False                      # soft floor reactivation
        cum += pnl
        if cum > peak:
            peak = cum
        st = st + 1 if won else 0
        if not s and (k <= 0 or st < k) and peak >= arm and (peak - cum) >= give:
            s = True; ever = True           # trailing fire (latched until lifted/day end)
        if floor > 0 and cum <= -floor:
            fl = True                       # soft floor fire (re-fires each dip below -floor)
        if hard_floor > 0 and cum <= -hard_floor:
            hf = True                       # hard backstop: latches, no reactivation
    trail_clip = s and not (k > 0 and st >= k)
    floor_clip = fl and not (react_k > 0 and st >= react_k)
    clip_next = trail_clip or floor_clip or hf
    if fl or hf:
        ever = True
    return clip_next, peak, cum, st, ever

def _count_replay(day_trades, soft_c, react_k):
    """Count-based soft floor, replayed WINDOW-BY-WINDOW to match the backtest
    exactly (BTC+ETH in one window settle together, so both legs share the clip
    decided from PRIOR windows — no within-window peek). net-win-count = +1 win /
    -1 loss on the day; clip to 1x once it reaches -soft_c, reactivate on a
    react_k-win streak, re-fire on the next dip. Returns (clip_next, nc, streak).
    Pure function of the resolved win/loss sequence -> identical live and backtest."""
    windows = OrderedDict()
    for b in day_trades:
        windows.setdefault(b.get("window_end") or b.get("timestamp"), []).append(b)
    s = False; st = 0; nc = 0
    for we, legs in windows.items():
        if s and react_k > 0 and st >= react_k:
            s = False                       # reactivation at window start
        for b in legs:
            won = b.get("result") == "win"
            st = st + 1 if won else 0
            nc += 1 if won else -1
            if nc <= -soft_c:
                s = True                    # soft floor fire (re-fires each dip)
    clip_next = s and not (react_k > 0 and st >= react_k)
    return clip_next, nc, st


def _update_trailing_stop(bets):
    """Track today's realized CORE (non-dip) P&L, arm at TRAIL_STOP_ARM, and set
    the stop flag once it gives back TRAIL_STOP_GIVEBACK from the day's peak.
    Split-dip P&L is excluded so dips can't accidentally arm/trigger the stop.

    The peak and latch are recomputed each poll by replaying the day's persisted
    trades in timestamp order, NOT carried in mutable globals. This makes the stop
    deterministic and immune to bot restarts/redeploys — a restart used to zero the
    in-memory peak and un-latch the stop, letting it re-arm and scale back to 30x."""
    global _trail_day, _trail_peak, _trailing_stopped, _trail_dbg
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    # Replay today's core trades in order -> clip decision (redeploy-proof)
    day_trades = sorted(
        (b for b in bets
         if b.get("action") == "trade" and b.get("result") in ("win", "loss")
         and b.get("crypto") in ("BTC", "ETH") and not b.get("dip_add")
         and b.get("timestamp", "")[:10] == today),
        key=lambda b: b.get("timestamp", ""))
    # ── Count-based protection (parity-safe): net-win-count soft floor ──
    if PROTECT_MODE == "count":
        was_stopped = _trailing_stopped and _trail_day == today
        clip, nc, streak = _count_replay(day_trades, COUNT_SOFT_C, COUNT_REACT_K)
        _trail_day = today; _trail_peak = 0.0; _trailing_stopped = clip
        _trail_dbg = {"peak": 0.0, "day_pnl": 0.0, "armed": False, "stopped": clip,
                      "floored": clip, "hard_stopped": False, "streak": streak,
                      "resolved": len(day_trades), "net_count": nc}
        if clip and not was_stopped:
            P(f"  [FLOOR] Day net-win-count {nc} <= -{COUNT_SOFT_C} — clipping to 1x "
              f"(reactivates on {COUNT_REACT_K}-win streak)")
        try:
            status = {}
            if os.path.exists(STATUS_FILE):
                with open(STATUS_FILE) as f:
                    status = json.load(f)
            status["trail"] = {"day": today, "mode": "count", "stopped": clip,
                               "net_count": nc, "soft_c": COUNT_SOFT_C,
                               "react_k": COUNT_REACT_K, "streak": streak,
                               "resolved": len(day_trades)}
            save_status(status)
        except Exception:
            pass
        return
    if not TRAIL_STOP_ENABLED:
        _trailing_stopped = False
        _trail_dbg = {"peak": 0.0, "day_pnl": 0.0, "armed": False, "stopped": False,
                      "floored": False, "hard_stopped": False, "streak": 0, "resolved": 0}
        return
    was_stopped = _trailing_stopped and _trail_day == today
    seq = [(b.get("pnl", 0), b.get("result") == "win") for b in day_trades]
    stopped, peak, day_pnl, streak, ever = _trail_replay(
        seq, TRAIL_STOP_ARM, TRAIL_STOP_GIVEBACK, TRAIL_STREAK_HOLD_K,
        TRAIL_LOSS_FLOOR, TRAIL_FLOOR_REACT_K, TRAIL_HARD_FLOOR)
    _trail_day = today; _trail_peak = peak; _trailing_stopped = stopped
    hard_stopped = TRAIL_HARD_FLOOR > 0 and day_pnl <= -TRAIL_HARD_FLOOR
    floored = TRAIL_LOSS_FLOOR > 0 and day_pnl <= -TRAIL_LOSS_FLOOR
    _trail_dbg = {"peak": round(peak, 2), "day_pnl": round(day_pnl, 2),
                  "armed": peak >= TRAIL_STOP_ARM, "stopped": stopped,
                  "floored": floored, "hard_stopped": hard_stopped,
                  "streak": streak, "resolved": len(seq)}
    if stopped and not was_stopped:
        if hard_stopped:
            P(f"  [TRAIL] Day at +${day_pnl:.0f} hit the -${TRAIL_HARD_FLOOR:.0f} "
              f"HARD backstop — 1x for the rest of the day (no reactivation)")
        elif floored:
            P(f"  [TRAIL] Day at +${day_pnl:.0f} hit the -${TRAIL_LOSS_FLOOR:.0f} "
              f"loss floor — clipping to 1x (reactivates on {TRAIL_FLOOR_REACT_K}-win)")
        else:
            mode = f"streak-hold k={TRAIL_STREAK_HOLD_K}" if TRAIL_STREAK_HOLD_K > 0 else "latch"
            P(f"  [TRAIL] Day peaked +${peak:.0f}, gave back to +${day_pnl:.0f} "
              f"(>= ${TRAIL_STOP_GIVEBACK:.0f}) — clipping to 1x ({mode})")
    # Persist state so the dashboard (separate process) can show the indicator
    try:
        status = {}
        if os.path.exists(STATUS_FILE):
            with open(STATUS_FILE) as f:
                status = json.load(f)
        status["trail"] = {"day": today, "peak": round(peak, 2),
                           "day_pnl": round(day_pnl, 2), "stopped": stopped,
                           "armed": peak >= TRAIL_STOP_ARM,
                           "streak_hold_k": TRAIL_STREAK_HOLD_K,
                           "streak": streak, "ever_stopped": ever,
                           "loss_floor": TRAIL_LOSS_FLOOR,
                           "floor_react_k": TRAIL_FLOOR_REACT_K, "floored": floored,
                           "hard_floor": TRAIL_HARD_FLOOR, "hard_stopped": hard_stopped}
        save_status(status)
    except Exception:
        pass

_scale_state = {}
_scale_up_at = {}  # group -> trade count when scaled up (for down-check offset)

def _groups_signature():
    """Fingerprint of group definitions; saved indexes are invalid if this changes."""
    return json.dumps({k: [sorted(v["signals"]), v["up_window"], v["up_thresh"],
                           v["down_window"], v["down_thresh"]]
                       for k, v in sorted(SCALE_GROUPS.items())})

def _persist_scale_state():
    """Save scale state to status file so dashboard can read it."""
    try:
        if os.path.exists(STATUS_FILE):
            with open(STATUS_FILE) as f:
                status = json.load(f)
        else:
            status = {}
        status["scale_state"] = dict(_scale_state)
        status["scale_up_at"] = dict(_scale_up_at)
        status["scale_groups_sig"] = _groups_signature()
        save_status(status)
    except Exception:
        pass

def _restore_scale_state():
    """Restore scale state from status file, or derive from trade history."""
    global _scale_state, _scale_up_at
    try:
        if os.path.exists(STATUS_FILE):
            with open(STATUS_FILE) as f:
                status = json.load(f)
            saved = status.get("scale_state", {})
            saved_up_at = status.get("scale_up_at", {})
            if status.get("scale_groups_sig") != _groups_signature():
                P("  [SCALE] Group config changed — discarding saved state, replaying history")
                saved = {}
            if saved and saved_up_at:
                _scale_state = {k: int(v) for k, v in saved.items()}
                _scale_up_at = {k: int(v) for k, v in saved_up_at.items()}
                # If SCALE_UP_COUNT changed, update scaled groups to new value
                for k, v in _scale_state.items():
                    if v > 1 and v != SCALE_UP_COUNT:
                        _scale_state[k] = SCALE_UP_COUNT
                P(f"  [SCALE] Restored state: {_scale_state}, up_at: {_scale_up_at}")
                _persist_scale_state()
                return
    except Exception:
        pass
    # No saved state — replay trade history with correct up/down logic
    try:
        if os.path.exists(BETS_FILE):
            with open(BETS_FILE) as f:
                bets = json.load(f)
            resolved = [b for b in bets
                        if b.get("action") == "trade" and b.get("result") in ("win", "loss")
                        and b.get("crypto") in ("BTC", "ETH") and not b.get("dip_add")]
            for gname, cfg in SCALE_GROUPS.items():
                g_trades = [b for b in resolved if (b.get("score", 0) + 3) in cfg["signals"]]
                scale = 1
                up_at = 0
                for i in range(len(g_trades)):
                    if scale == 1:
                        wt = g_trades[:i+1]
                        if len(wt) >= cfg["up_window"]:
                            wins = sum(1 for b in wt[-cfg["up_window"]:] if b["result"] == "win")
                            if wins >= cfg["up_thresh"]:
                                scale = SCALE_UP_COUNT
                                up_at = i + 1
                    else:
                        since = g_trades[up_at:i+1]
                        if len(since) >= cfg["down_window"]:
                            losses = sum(1 for b in since[-cfg["down_window"]:] if b["result"] == "loss")
                            if losses >= cfg["down_thresh"]:
                                scale = 1
                _scale_state[gname] = scale
                if scale > 1:
                    _scale_up_at[gname] = up_at
            P(f"  [SCALE] Derived from history: {_scale_state}, up_at: {_scale_up_at}")
            _persist_scale_state()
    except Exception:
        pass

def _get_scale_group(signal_count):
    for name, cfg in SCALE_GROUPS.items():
        if signal_count in cfg["signals"]:
            return name, cfg
    return None, None

_cool_off_blocked = 0  # >0: last get_dynamic_contracts call was capped by the lid
_last_base_contracts = 1  # scale the last call would use if the trailing stop were OFF
_last_group_scale = 1  # raw group scale before the cool-off lid (for debug logging)

def get_dynamic_contracts(bets, crypto, signal_count):
    """Grouped scaling with cool-off lid, then the intraday trailing-stop clip.
    Records the pre-stop scale in _last_base_contracts so the dashboard can draw
    an accurate 'without trailing stop' P&L line even when trades are clipped."""
    global _last_base_contracts
    base = _base_dynamic_contracts(bets, crypto, signal_count)
    _last_base_contracts = base
    # Intraday trailing stop: if the day gave back its gains, trade 1x
    if _trailing_stopped:
        return 1
    return base

def _base_dynamic_contracts(bets, crypto, signal_count):
    """Grouped scaling with cool-off lid: BTC+ETH combined, split up/down windows.
    This is the scale absent the trailing stop."""
    global _cool_off_blocked, _last_group_scale
    _cool_off_blocked = 0
    contracts = _group_scale_contracts(bets, crypto, signal_count)
    _last_group_scale = contracts
    if contracts <= 1:
        return contracts
    resolved = [b for b in bets if b.get("action") == "trade"
                and b.get("result") in ("win", "loss")
                and b.get("crypto") in ("BTC", "ETH") and not b.get("dip_add")]
    # Clean consecutive-win streak over resolved core trades (window-aware: the
    # current window's legs are still open, so this only sees prior windows)
    streak = 0
    for b in reversed(resolved):
        if b["result"] == "win":
            streak += 1
        else:
            break
    # Press-winners: a clean streak carries persistent momentum -> boost the scale
    # and BYPASS the cool-off lid (a durable run keeps its edge)
    if PRESS_STREAK_N > 0 and streak >= PRESS_STREAK_N:
        pressed = max(contracts, int(round(contracts * PRESS_MULT)))
        if pressed != contracts:
            P(f"  [SCALE] Press-winners: {streak}-win streak — {contracts}x -> {pressed}x")
        return pressed
    # Cool-off: trailing WR >= threshold means the run is euphoric — forward
    # edge is ~zero there, so cap at 1x until it cools (group states unaffected)
    if COOL_OFF_WR > 0:
        last = resolved[-COOL_OFF_WINDOW:]
        if len(last) >= COOL_OFF_WINDOW:
            wr = sum(1 for b in last if b["result"] == "win") / len(last)
            if wr >= COOL_OFF_WR:
                if COOL_OFF_BYPASS_STREAK > 0 and streak >= COOL_OFF_BYPASS_STREAK:
                    P(f"  [SCALE] Cool-off bypass: {streak}-win streak — staying {contracts}x")
                else:
                    P(f"  [SCALE] Cool-off: trailing WR {wr:.0%} >= {COOL_OFF_WR:.0%} — trading 1x")
                    _cool_off_blocked = contracts
                    return 1
    return contracts

def _group_scale_contracts(bets, crypto, signal_count):
    if signal_count == 0:
        return 1

    group_name, cfg = _get_scale_group(signal_count)
    if cfg is None:
        return 1

    current = _scale_state.get(group_name, 1)

    resolved = [b for b in bets
                if b.get("action") == "trade" and b.get("result") in ("win", "loss")
                and b.get("crypto") in ("BTC", "ETH") and not b.get("dip_add")
                and (b.get("score", 0) + 3) in cfg["signals"]]

    if current == 1:
        if len(resolved) < cfg["up_window"]:
            return current
        last_n = resolved[-cfg["up_window"]:]
        wins = sum(1 for b in last_n if b["result"] == "win")
        if wins >= cfg["up_thresh"]:
            _scale_state[group_name] = SCALE_UP_COUNT
            _scale_up_at[group_name] = len(resolved)
            P(f"  [SCALE] Group {group_name}: {wins}/{cfg['up_window']} wins — UP to {SCALE_UP_COUNT}")
            _persist_scale_state()
            return SCALE_UP_COUNT
    else:
        # Only check trades AFTER the scale-up transition
        up_idx = _scale_up_at.get(group_name, 0)
        since_up = resolved[up_idx:]
        if len(since_up) < cfg["down_window"]:
            return current
        last_n = since_up[-cfg["down_window"]:]
        losses = sum(1 for b in last_n if b["result"] == "loss")
        if losses >= cfg["down_thresh"]:
            _scale_state[group_name] = 1
            P(f"  [SCALE] Group {group_name}: {losses}/{cfg['down_window']} losses (since scale-up) — DOWN to 1")
            _persist_scale_state()
            return 1

    return current

def _resolve_dip_orders(bets):
    """Resolve resting dip orders against the FILLS feed (ground truth): an
    order in the feed filled -> becomes an open position; a dip whose window
    has closed with no fill -> expired. The orders API returns minimal records
    post-close and can't be trusted, so we match order_ids in the fills feed."""
    pending = [b for b in bets if b.get("result") == "dip_pending"]
    if not pending:
        return bets
    # Pull recent fills once, aggregate filled qty per order_id
    fills_qty = {}
    cursor = None
    for _ in range(4):  # ~800 recent fills covers many windows
        try:
            params = {"limit": 200}
            if cursor:
                params["cursor"] = cursor
            resp = auth_get("/portfolio/fills", params=params)
        except Exception:
            break
        fl = resp.get("fills", [])
        for f in fl:
            oid = f.get("order_id")
            q = float(f.get("count_fp") or f.get("count") or 0)
            fills_qty[oid] = fills_qty.get(oid, 0.0) + q
        cursor = resp.get("cursor")
        if not fl or not cursor:
            break
        time.sleep(0.2)

    now = datetime.now(timezone.utc)
    changed = False
    for bet in pending:
        qty = fills_qty.get(bet.get("order_id"), 0.0)
        if qty > 0:
            bet["fill_price"] = bet.get("price", SPLIT_DIP_PRICE)
            bet["filled_count"] = int(round(qty))
            bet["result"] = "open"   # normal settlement resolves it next
            P(f"  [DIP] {bet.get('crypto','')} filled {bet['filled_count']} @ {bet['fill_price']:.2f}")
            changed = True
        else:
            we = bet.get("window_end", "")
            try:
                closed = bool(we) and datetime.fromisoformat(we) < now
            except Exception:
                closed = True
            if closed:
                bet["result"] = "dip_expired"   # window closed, never filled
                changed = True
    if changed:
        save_bets(bets)
    return bets

def _resolve_open_bets(bets):
    """Resolve open bets from past windows so scaling/cool-off/trailing see the
    COMPLETE realized P&L sequence. Unresolved trades are invisible to the sizing
    logic (it only reads win/loss), so a backlog of stuck 'open' trades makes the
    bot size off a partial view — the single biggest live-vs-backtest gap driver.

    Three defenses against a starving backlog:
      1. Settle-time gate: only query markets whose window has actually ended, so
         we never waste calls (or rate-limit budget) on un-settleable markets.
      2. Loud failures: API errors are logged with the ticker (never swallowed),
         and the bet is left open for the next cycle instead of vanishing.
      3. Deterministic drain: process oldest-window-first with one retry/backoff,
         so a backlog empties from the front instead of the tail starving.
      4. Bounded + checkpointed: a full pass over a large backlog takes longer
         than a trading window, so an unbounded pass is ALWAYS interrupted — and
         with a single save at the end, every resolution in that pass was lost.
         That made a backlog self-sustaining: the bigger it got, the longer the
         pass, the less likely it ever reached the save. Now we cap the work per
         cycle and checkpoint as we go, so progress always survives."""
    now = datetime.now(timezone.utc)
    # Always run (no-op when none pending) so dips resolve even if the feature
    # is toggled off while orders are still outstanding
    if any(b.get("result") == "dip_pending" for b in bets):
        bets = _resolve_dip_orders(bets)

    def _we(bet):
        try:
            return datetime.fromisoformat(bet.get("window_end", ""))
        except Exception:
            return now  # no parseable window_end -> treat as due now

    # Candidates: open trade bets, confirmed-filled, whose window has ended.
    # Never book settlement P&L without confirmed fill evidence — EXCEPT for
    # phantoms, which by definition have none. They must still settle: their
    # win/loss is what keeps the sizing brain's history identical to a fully
    # traded run, and an unresolved phantom is invisible to every scaling rule.
    pending = [b for b in bets
               if b.get("result") == "open" and b.get("action") == "trade"
               and (b.get("filled_count", 0) > 0 or b.get("phantom"))
               and _we(b) <= now]
    # Oldest window first so a backlog drains from the front deterministically
    pending.sort(key=_we)

    backlog = len(pending)
    if backlog > RESOLVE_MAX_PER_CYCLE:
        pending = pending[:RESOLVE_MAX_PER_CYCLE]
        P(f"  [RESOLVE] backlog {backlog} open — draining oldest {len(pending)} this cycle "
          f"(cap RESOLVE_MAX_PER_CYCLE={RESOLVE_MAX_PER_CYCLE})")

    changed = False
    failures = 0
    resolved_since_save = 0
    for bet in pending:
        ticker = bet.get("ticker", "")
        market = None
        for attempt in (1, 2):  # one retry with backoff on transient errors
            try:
                mkt = public_get(f"/markets/{ticker}")
                market = mkt.get("market", {})
                break
            except Exception as e:
                if attempt == 1:
                    time.sleep(0.6)
                    continue
                failures += 1
                P(f"  [RESOLVE] WARN {bet.get('crypto','')} {ticker}: API error, left open — {e}")
        if market is None:
            continue  # left open, retried next cycle (visibly)
        mkt_status = market.get("status", "")
        result_val = market.get("result", "")
        if mkt_status in ("settled", "finalized") and result_val:
            side = bet.get("side", "")
            won = (result_val == "yes" and side == "yes") or \
                  (result_val == "no" and side == "no")
            bet["result"] = "win" if won else "loss"
            bet["market_result"] = result_val
            price = bet.get("fill_price", bet.get("price", 0))
            contracts = bet.get("filled_count", bet.get("contracts", 1))
            fee = bet.get("fee")
            if fee is None:
                fee = math.ceil(0.07 * contracts * price * (1 - price) * 100) / 100.0 if 0 < price < 1 else 0.0
            if won:
                bet["pnl"] = round(contracts * (1.0 - price) - fee, 2)
            else:
                bet["pnl"] = round(-contracts * price - fee, 2)
            changed = True
            resolved_since_save += 1
            P(f"  [RESOLVE] {bet.get('crypto','')} {ticker}: {bet['result']}")
            # Checkpoint: without this, an interrupted pass discards everything
            # it just resolved and the backlog never shrinks.
            if resolved_since_save >= RESOLVE_SAVE_EVERY:
                save_bets(bets)
                resolved_since_save = 0
        time.sleep(RESOLVE_SLEEP)
    if failures:
        P(f"  [RESOLVE] {failures} bet(s) still open after API errors — will retry next window")
    if changed:
        save_bets(bets)
    if backlog > len(pending):
        P(f"  [RESOLVE] {backlog - len(pending)} still queued — next cycle continues from the front")
    return bets

# ── Main loop ───────────────────────────────────────────────────────────
def run(live=False):
    P("=" * 65)
    P(f"  CRYPTO SCORE BOT — {SCORE_VERSION.upper()} Scoring Engine")
    P(f"  Mode: {'LIVE' if live else 'DRY RUN'}")
    P(f"  Strategy: {SCORE_VERSION} | Entry: minute {ENTRY_AFTER_MINUTES}+ | Signal count: {MIN_SCORE+3}-{MAX_SCORE+3}")
    tp_str = f"{TAKE_PROFIT_PRICE*100:.0f}c" if TAKE_PROFIT_PRICE > 0 else "OFF"
    P(f"  Price range: {MIN_PRICE*100:.0f}-{MAX_PRICE*100:.0f}c | {CONTRACT_COUNT} contracts | TP: {tp_str}")
    P(f"  Cryptos: {', '.join(CRYPTOS.keys())}")
    P("=" * 65)

    _restore_scale_state()

    if live:
        bal = get_balance()
        if bal:
            P(f"  Balance: ${bal['balance']:.2f} | Portfolio: ${bal['portfolio_value']:.2f}")

    bets = load_bets()
    total_new = 0
    last_window_end = None
    placed_this_window = set()
    dips_done_this_window = False
    core_dips_done_this_window = False
    skip_tickers = set()

    # Indicators cache
    indicators = {}
    checked_positions = False
    fetched_indicators = False
    PREFETCH_MINUTE = ENTRY_AFTER_MINUTES  # Fetch CoinGecko right at entry time

    P(f"\n  Running continuously — polling every {POLL_INTERVAL}s...")

    while True:
        try:
            window_start, window_end = get_current_window()
            mins_left = minutes_until_strike()
            mins_in = 15 - mins_left

            # New window? Reset and re-read bets from disk (respects external resets)
            if window_end != last_window_end:
                last_window_end = window_end
                placed_this_window = set()
                dips_done_this_window = False
                core_dips_done_this_window = False
                locked_side = None  # Lock direction after first trade
                checked_positions = False
                fetched_indicators = False
                bets = load_bets()
                bets = _resolve_open_bets(bets)
                P(f"\n  -- Window {window_start.strftime('%H:%M')}-{window_end.strftime('%H:%M')} UTC ({len(bets)} bets on file) --")

            # Too early — sleep until prefetch time
            if mins_in < PREFETCH_MINUTE:
                time.sleep(POLL_INTERVAL)
                continue

            # Fetch CoinGecko right before trading (skip paused cryptos to save API calls)
            TRADE_PAUSED = {"SOL", "DOGE", "BNB", "HYPE", "XRP"}
            if not fetched_indicators:
                P("  Fetching CoinGecko data...")
                crypto_data = fetch_crypto_prices(skip_coins=TRADE_PAUSED)
                if crypto_data:
                    indicators = compute_indicators(crypto_data)
                    P(f"  Got indicators for {len(indicators)} cryptos")

                    # Save status for dashboard (preserve existing keys like scale_state)
                    status = {}
                    if os.path.exists(STATUS_FILE):
                        try:
                            with open(STATUS_FILE) as f:
                                status = json.load(f)
                        except Exception:
                            status = {}
                    status["last_update"] = datetime.now(timezone.utc).isoformat()
                    status["indicators"] = {}
                    for sym, ind in indicators.items():
                        status["indicators"][sym] = {
                            "price": ind["current_price"],
                            "ret_1h": round(ind["ret_1h"], 3),
                            "ret_3h": round(ind["ret_3h"], 3),
                            "vol_6h": round(ind["vol_6h"], 3),
                            "rsi": round(ind["rsi"], 1),
                            "stoch": round(ind["stoch"], 1),
                            "pack_agreement": round(ind["pack_agreement"], 2),
                        }
                    if _scale_state:
                        status["scale_state"] = dict(_scale_state)
                    if _scale_up_at:
                        status["scale_up_at"] = dict(_scale_up_at)
                    save_status(status)
                    fetched_indicators = True
                else:
                    P("  WARNING: CoinGecko fetch failed, will retry next poll")

            # Wait until entry time (minute 11)
            if mins_in < ENTRY_AFTER_MINUTES:
                time.sleep(POLL_INTERVAL)
                continue

            # Fetch positions/orders once per window, right before trading
            if not checked_positions:
                P("  Checking open orders/positions...")
                open_order_tickers = get_open_orders()
                time.sleep(1)
                existing_positions = get_existing_positions()
                skip_tickers = open_order_tickers | existing_positions
                if skip_tickers:
                    P(f"  Skipping {len(skip_tickers)} tickers with open orders/positions")
                else:
                    P("  No existing orders/positions to skip")
                checked_positions = True
                time.sleep(1)

            if not indicators:
                time.sleep(POLL_INTERVAL)
                continue

            # Single pass: collect sides for consensus + evaluate trades
            CONSENSUS_EXCLUDE = {"BNB", "HYPE"}
            crypto_snapshots = {}
            P(f"  Scanning {len([c for c in CRYPTOS if c not in TRADE_PAUSED])} markets...")
            for c, cfg2 in CRYPTOS.items():
                if c in placed_this_window:
                    continue
                # Paused cryptos are never traded, and the only thing their market
                # scan feeds is `window_sides` -> the Consensus signal, which is
                # DISABLED (0 points). pack_agreement comes from CoinGecko (already
                # BTC/ETH-only via skip_coins). Scanning them was ~10 extra Kalshi
                # calls per window and was triggering 429 rate limits that made the
                # bot miss real BTC/ETH markets. Skipping them is strategy-neutral.
                if c in TRADE_PAUSED:
                    continue
                time.sleep(0.5)
                mkt2, ev2 = find_current_market(cfg2["series"])
                if not mkt2:
                    P(f"    {c}: No market found")
                    continue
                time.sleep(0.5)
                s2, p2 = get_dominant_side(mkt2["ticker"])
                P(f"    {c}: {s2} @ {p2}" if s2 else f"    {c}: No dominant side")
                crypto_snapshots[c] = {"market": mkt2, "event": ev2, "side": s2, "price": p2}

            # Build consensus from snapshots (excluding BNB/HYPE)
            window_sides = {}
            for c, snap in crypto_snapshots.items():
                if c in CONSENSUS_EXCLUDE:
                    continue
                if snap["side"] and snap["price"] and snap["price"] >= 0.60:
                    window_sides[c] = snap["side"]
            indicators["_window_sides"] = window_sides
            if window_sides:
                P(f"  Window sides: {window_sides}")

            # ── Phase 1: Evaluate scores and build trade list ──────────
            trade_queue = []  # [{crypto, ticker, side, price, score, bet_record}, ...]
            for crypto, snap in crypto_snapshots.items():
                if crypto in placed_this_window:
                    continue
                if crypto in TRADE_PAUSED:
                    continue
                market = snap["market"]
                event = snap["event"]
                ticker = market["ticker"]

                if ticker in skip_tickers:
                    placed_this_window.add(crypto)
                    continue
                if any(b.get("ticker") == ticker for b in bets):
                    placed_this_window.add(crypto)
                    continue

                side, price = snap["side"], snap["price"]
                if not side or not price:
                    continue

                # Only trade dominant side in price range
                if price < MIN_PRICE or price > MAX_PRICE:
                    continue

                # Compute score
                score, reasons = compute_score(crypto, side, price, indicators)
                if score is None:
                    continue

                reasons_str = ", ".join(f"{r[0]}:{r[2]}" for r in reasons)
                P(f"    {crypto} {side.upper()} @ {price:.2f} | Score: {score:+d} [{reasons_str}]")

                # Build detailed score breakdown for weight analysis
                ind = indicators.get(crypto, {})
                score_breakdown = {}
                for factor, detail, pts in reasons:
                    try:
                        score_breakdown[factor] = {"detail": detail, "points": int(pts)}
                    except (ValueError, TypeError):
                        score_breakdown[factor] = {"detail": detail, "points": 0}

                bet_record = {
                    "crypto": crypto,
                    "ticker": ticker,
                    "event_ticker": event["event_ticker"],
                    "side": side,
                    "price": price,
                    "score": score,
                    "reasons": reasons,
                    "score_breakdown": score_breakdown,
                    "indicators": {
                        "ret_1h": round(ind.get("ret_1h", 0), 4),
                        "ret_3h": round(ind.get("ret_3h", 0), 4),
                        "vol_6h": round(ind.get("vol_6h", 0), 4),
                        "rsi": round(ind.get("rsi", 50), 2),
                        "stoch": round(ind.get("stoch", 50), 2),
                        "pack_agreement": round(ind.get("pack_agreement", 0.5), 3),
                        "btc_ret_1h": round(indicators.get("BTC", {}).get("ret_1h", 0), 4),
                    },
                    "bet_amount": BET_AMOUNT,
                    "contracts": CONTRACT_COUNT,
                    "entry_minute": round(mins_in, 1),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "window_end": window_end.isoformat(),
                    "result": "open",
                    "strategy_version": SCORE_VERSION,
                }

                signal_count = score + 3
                SKIP_SIGNALS = set()  # Trading all signal counts 0-7

                if signal_count in SKIP_SIGNALS:
                    P(f"    {crypto}: SKIP (signal count {signal_count} in skip list)")
                    bet_record["action"] = "skip"
                    bets.append(bet_record)
                    save_bets(bets)
                    placed_this_window.add(crypto)
                    continue

                if score > MAX_SCORE:
                    P(f"    {crypto}: SKIP (signal count {signal_count} > {MAX_SCORE+3})")
                    bet_record["action"] = "skip"
                    bets.append(bet_record)
                    save_bets(bets)
                    placed_this_window.add(crypto)
                    continue

                bet_record["action"] = "trade"

                if not live:
                    P(f"    {crypto}: [DRY RUN] {side.upper()} @ {price:.2f} | Score {score:+d}")
                    bet_record["status"] = "dry_run"
                    bets.append(bet_record)
                    save_bets(bets)
                    total_new += 1
                    placed_this_window.add(crypto)
                    continue

                trade_queue.append({
                    "crypto": crypto, "ticker": ticker, "side": side,
                    "price": price, "score": score, "bet_record": bet_record,
                })

            # ── Phase 2: Fire all orders at once ──────────────────────
            # Re-resolve open bets right before sizing: markets from the previous
            # window often haven't settled at window start but have by entry time,
            # and a stale open loss would be invisible to the scale-down check
            if trade_queue:
                bets = _resolve_open_bets(bets)
                # Update the intraday trailing stop from today's realized core P&L
                _update_trailing_stop(bets)
            pending_orders = []  # [{crypto, ticker, side, price, order_id, bet_record}, ...]
            for tq in trade_queue:
                sig_count = tq["score"] + 3
                current_contracts = get_dynamic_contracts(bets, tq["crypto"], sig_count)
                # Split-guard: trade split-window legs at 1x — the losing leg can't
                # do full-scale damage. Detected from the other crypto's recorded/
                # queued side; guards the 2nd leg (the 1st is already placed).
                if (SPLIT_GUARD and current_contracts > 1
                        and _is_split_leg(bets, trade_queue, tq["crypto"], tq["side"],
                                          tq["bet_record"].get("window_end"))):
                    P(f"    {tq['crypto']}: split-guard — {current_contracts}x -> 1x")
                    current_contracts = 1
                    tq["bet_record"]["split_guard"] = True
                # Vol gate: don't scale into low-volatility chop (momentum whipsaws
                # there -> red days). vol_6h is recorded at entry -> parity-safe.
                if VOL_GATE > 0 and current_contracts > 1:
                    _vol = (tq["bet_record"].get("indicators") or {}).get("vol_6h")
                    if _vol is not None and _vol < VOL_GATE:
                        P(f"    {tq['crypto']}: vol-gate — {current_contracts}x -> 1x "
                          f"(vol {_vol:.3f} < {VOL_GATE})")
                        current_contracts = 1
                        tq["bet_record"]["vol_gated"] = True
                # Signal weight: trade low-edge signals smaller while keeping them
                # in the group's momentum history. Deterministic -> parity-safe.
                if SIG_WEIGHTS and current_contracts > 1:
                    _w = SIG_WEIGHTS.get(sig_count)
                    if _w is not None and _w != 1.0:
                        _weighted = max(1, int(round(current_contracts * _w)))
                        if _weighted != current_contracts:
                            P(f"    {tq['crypto']}: sig-weight — sig{sig_count} "
                              f"{current_contracts}x -> {_weighted}x (x{_w})")
                            current_contracts = _weighted
                            tq["bet_record"]["sig_weighted"] = True
                # ── Win-rate ladder ──────────────────────────────────────
                # Cap: euphoric stretch -> stop scaling entirely.
                if WR_CAP > 0 and current_contracts > 1:
                    _wr = _trailing_wr(bets, WR_CAP_N)
                    if _wr is not None and _wr > WR_CAP:
                        P(f"    {tq['crypto']}: wr-cap — trailing-{WR_CAP_N} WR "
                          f"{_wr:.0%} > {WR_CAP:.0%} — {current_contracts}x -> 1x")
                        current_contracts = 1
                        tq["bet_record"]["wr_capped"] = True
                # Boost: productive band -> lean in.
                if WR_BOOST_MULT > 1 and current_contracts > 1:
                    _wrb = _trailing_wr(bets, WR_BOOST_N)
                    if _wrb is not None and WR_BOOST_LO <= _wrb <= WR_BOOST_HI:
                        _b = max(1, int(round(current_contracts * WR_BOOST_MULT)))
                        if _b != current_contracts:
                            P(f"    {tq['crypto']}: wr-boost — trailing-{WR_BOOST_N} WR "
                              f"{_wrb:.0%} in band — {current_contracts}x -> {_b}x")
                            current_contracts = _b
                            tq["bet_record"]["wr_boosted"] = True
                tq["bet_record"]["contracts"] = current_contracts
                # Record the scale absent the trailing stop, so the dashboard can
                # draw an accurate "without trailing stop" P&L line.
                tq["bet_record"]["base_contracts"] = _last_base_contracts
                if _trailing_stopped:
                    tq["bet_record"]["trail_stopped"] = True
                if _cool_off_blocked > 0:
                    tq["bet_record"]["cool_off"] = True
                    tq["bet_record"]["blocked_scale"] = _cool_off_blocked
                # ── Decision record: everything the sizing brain saw, stamped on
                # the trade, so live-vs-backtest reconciliation is a lookup rather
                # than a forensic replay. clip_reason names the single binding
                # constraint; resolved/open_today expose the partial-view effect.
                _td = _trail_dbg
                if tq["bet_record"].get("wr_boosted"):
                    _clip = "wr_boost"
                elif tq["bet_record"].get("wr_capped"):
                    _clip = "wr_cap"
                elif tq["bet_record"].get("split_guard"):
                    _clip = "split_guard"
                elif tq["bet_record"].get("vol_gated"):
                    _clip = "vol_gate"
                elif tq["bet_record"].get("sig_weighted"):
                    _clip = "sig_weight"
                elif _trailing_stopped:
                    _clip = ("hard_floor" if _td.get("hard_stopped")
                             else "soft_floor" if _td.get("floored") else "trailing")
                elif _cool_off_blocked > 0:
                    _clip = "cool_off"
                else:
                    _clip = "none"
                _today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                _core_today = [b for b in bets if b.get("action") == "trade"
                               and b.get("crypto") in ("BTC", "ETH") and not b.get("dip_add")
                               and b.get("timestamp", "")[:10] == _today]
                _res_today = sum(1 for b in _core_today if b.get("result") in ("win", "loss"))
                _open_today = sum(1 for b in _core_today if b.get("result") == "open")
                tq["bet_record"]["decision"] = {
                    "group_scale": _last_group_scale, "cool_off": bool(_cool_off_blocked),
                    "base": _last_base_contracts, "trail_stopped": bool(_trailing_stopped),
                    "trail_peak": _td.get("peak"), "trail_day_pnl": _td.get("day_pnl"),
                    "trail_armed": _td.get("armed"), "floored": _td.get("floored"),
                    "hard_stopped": _td.get("hard_stopped"),
                    "split_guard": bool(tq["bet_record"].get("split_guard")),
                    "vol_gated": bool(tq["bet_record"].get("vol_gated")),
                    "sig_weighted": bool(tq["bet_record"].get("sig_weighted")),
                    "wr_capped": bool(tq["bet_record"].get("wr_capped")),
                    "wr_boosted": bool(tq["bet_record"].get("wr_boosted")),
                    "wr_cap_val": _trailing_wr(bets, WR_CAP_N) if WR_CAP > 0 else None,
                    "wr_boost_val": _trailing_wr(bets, WR_BOOST_N) if WR_BOOST_MULT > 1 else None,
                    "final": current_contracts, "clip_reason": _clip,
                    "resolved_today": _res_today, "open_today": _open_today,
                }
                P(f"    [SIZE] {tq['crypto']} sig{sig_count}: group {_last_group_scale}x "
                  f"-> final {current_contracts}x [{_clip}] "
                  f"(day resolved {_res_today}/open {_open_today}, "
                  f"peak ${_td.get('peak',0):.0f} pnl ${_td.get('day_pnl',0):.0f})")
                # ── Phantom gate ─────────────────────────────────────────
                # Must sit AFTER the WR boost, since that is the only point
                # where "is this boosted" is known. The record keeps its full
                # size and enters `bets` normally, so every downstream reader
                # (group scaling, cool-off, count floor, WR ladder) sees the
                # identical history — only the order is withheld.
                _ph_all = PHANTOM_MODE == "all"
                if _ph_all or (PHANTOM_MODE == "boosted_only"
                               and not tq["bet_record"].get("wr_boosted")):
                    tq["bet_record"]["phantom"] = True
                    tq["bet_record"]["status"] = "phantom"
                    tq["bet_record"]["order_id"] = None
                    tq["bet_record"]["fill_price"] = tq["price"]
                    tq["bet_record"]["filled_count"] = current_contracts
                    bets.append(tq["bet_record"])
                    save_bets(bets)
                    placed_this_window.add(tq["crypto"])
                    P(f"    {tq['crypto']}: PHANTOM {current_contracts}x @ {tq['price']:.2f} "
                      f"({'all-phantom' if _ph_all else 'not boosted'} — recorded, no order placed)")
                    continue
                try:
                    result = place_order(tq["ticker"], tq["side"], tq["price"], BET_AMOUNT, count=current_contracts)
                    if not result:
                        P(f"    {tq['crypto']}: Order failed")
                        tq["bet_record"]["action"] = "unfilled"
                        tq["bet_record"]["status"] = "failed"
                        bets.append(tq["bet_record"])
                        placed_this_window.add(tq["crypto"])
                        continue
                    order = result.get("order", {})
                    order_id = order.get("order_id", "")
                    order_status = order.get("status", "")
                    tq["bet_record"]["order_id"] = order_id
                    tq["bet_record"]["status"] = order_status

                    if order_status in ("executed", "partial"):
                        avg_p = order.get("avg_price")
                        if avg_p is not None and avg_p > 1:
                            avg_p = avg_p / 100
                        if avg_p is not None and tq["side"] == "no":
                            avg_p = 1.0 - avg_p
                        tq["bet_record"]["fill_price"] = avg_p if avg_p else tq["price"]
                        remaining = int(order.get("remaining_count", 0))
                        total = int(order.get("count", tq["bet_record"].get("contracts", 1)))
                        tq["bet_record"]["filled_count"] = total - remaining
                        bets.append(tq["bet_record"])
                        save_bets(bets)
                        total_new += 1
                        placed_this_window.add(tq["crypto"])
                        P(f"    {tq['crypto']}: FILLED @ {tq['bet_record']['fill_price']:.2f}")
                    else:
                        # Resting — add to pending for bulk check
                        pending_orders.append({
                            "crypto": tq["crypto"], "ticker": tq["ticker"],
                            "side": tq["side"], "price": tq["price"],
                            "order_id": order_id, "bet_record": tq["bet_record"],
                            "attempt": 1,
                        })
                        P(f"    {tq['crypto']}: Order placed, resting...")
                except Exception as e:
                    P(f"    {tq['crypto']}: Order error: {e}")
                    tq["bet_record"]["action"] = "unfilled"
                    tq["bet_record"]["status"] = "error"
                    bets.append(tq["bet_record"])
                    placed_this_window.add(tq["crypto"])
                time.sleep(0.3)  # Small delay between orders to avoid rate limits

            # ── Split-window dip orders ──
            # The two legs of a split are usually placed in separate polls, so
            # detect from this window's RECORDED trades, not the same-poll queue.
            if SPLIT_DIP_ENABLED and not dips_done_this_window:
                if _place_split_dips(bets, window_end.isoformat()):
                    dips_done_this_window = True
            # Core dips cover the NON-split windows the rule above skips, so it
            # runs independently of dips_done_this_window (which tracks splits).
            # _place_core_dips returns True only once every configured crypto
            # has a dip (or the window is a split, which is the split rule's
            # territory), so a leg that shows up in a later poll still gets one.
            if CORE_DIP_ENABLED and not core_dips_done_this_window:
                if _place_core_dips(bets, window_end.isoformat()):
                    core_dips_done_this_window = True

            # ── Phase 3: Check pending orders once, cancel unfilled ──
            if pending_orders:
                P(f"  Waiting 15s to check {len(pending_orders)} pending orders...")
                time.sleep(15)

                for po in pending_orders:
                    try:
                        check = auth_get(f"/portfolio/orders/{po['order_id']}")
                        check_order = check.get("order", check)
                        check_status = check_order.get("status", "")
                        remaining = int(check_order.get("remaining_count", 0))
                        ordered = po["bet_record"].get("contracts", 1)
                        total_count = int(check_order.get("count", ordered))
                        filled_count = min(total_count - remaining, ordered)

                        if check_status == "executed" or filled_count > 0:
                            # Filled
                            avg_p = check_order.get("avg_price", None)
                            if avg_p is not None and avg_p > 1:
                                avg_p = avg_p / 100
                            # V2 avg_price is always YES price; convert for NO side
                            if avg_p is not None and po["side"] == "no":
                                avg_p = 1.0 - avg_p
                            po["bet_record"]["status"] = check_status
                            po["bet_record"]["fill_price"] = avg_p if avg_p else po["price"]
                            if filled_count > 0:
                                po["bet_record"]["filled_count"] = filled_count
                            elif check_status == "executed":
                                # Executed but count fields missing — fully filled
                                po["bet_record"]["filled_count"] = po["bet_record"].get("contracts", 1)
                            bets.append(po["bet_record"])
                            save_bets(bets)
                            total_new += 1
                            placed_this_window.add(po["crypto"])
                            P(f"    {po['crypto']}: FILLED (status={check_status})")

                            # Place take-profit
                            if TAKE_PROFIT_PRICE > 0 and po["price"] < TAKE_PROFIT_PRICE:
                                try:
                                    tp_result = place_take_profit(po["ticker"], po["side"], CONTRACT_COUNT)
                                    if tp_result:
                                        tp_order = tp_result.get("order", {})
                                        po["bet_record"]["tp_order_id"] = tp_order.get("order_id", "")
                                        po["bet_record"]["tp_price"] = TAKE_PROFIT_PRICE
                                except Exception:
                                    pass
                        else:
                            P(f"    {po['crypto']}: Unfilled (status={check_status}), canceling order {po['order_id']}...")
                            try:
                                cancel_resp = _cancel_order(po["order_id"], po.get("ticker"))
                                P(f"    {po['crypto']}: Canceled (reduced_by={cancel_resp.get('reduced_by', '?')})")
                            except Exception as ce:
                                P(f"    {po['crypto']}: Cancel error: {ce}")
                                try:
                                    time.sleep(1)
                                    cancel_resp = _cancel_order(po["order_id"], po.get("ticker"))
                                    P(f"    {po['crypto']}: Cancel retry ok")
                                except Exception as ce2:
                                    P(f"    {po['crypto']}: Cancel retry also failed: {ce2}")
                            po["bet_record"]["action"] = "unfilled"
                            po["bet_record"]["status"] = "canceled"
                            bets.append(po["bet_record"])
                            placed_this_window.add(po["crypto"])
                        time.sleep(0.3)
                    except Exception as e:
                        P(f"    {po['crypto']}: Check error: {e}")
                        po["bet_record"]["action"] = "unfilled"
                        po["bet_record"]["status"] = "error"
                        bets.append(po["bet_record"])
                        placed_this_window.add(po["crypto"])
                save_bets(bets)

            # Periodic git backup
            git_backup_bets(bets)

            time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            P("\n  Stopped by user")
            break
        except Exception as e:
            P(f"  ERROR: {e}")
            import traceback
            traceback.print_exc()
            time.sleep(30)

    save_bets(bets)
    P(f"\n  Score bot stopped. Total new bets: {total_new}")


if __name__ == "__main__":
    live = "--live" in sys.argv
    run(live=live)
