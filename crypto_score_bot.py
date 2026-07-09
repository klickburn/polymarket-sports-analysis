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
MIN_PRICE = float(os.environ.get("SCORE_MIN_PRICE", "0.78"))
MAX_PRICE = float(os.environ.get("SCORE_MAX_PRICE", "0.99"))
MIN_SCORE = int(os.environ.get("SCORE_MIN_SCORE", "-3"))  # Signal count 0 = pts-3 = -3
MAX_SCORE = int(os.environ.get("SCORE_MAX_SCORE", "4"))  # Signal count 7 = pts-3 = 4
TAKE_PROFIT_PRICE = float(os.environ.get("SCORE_TAKE_PROFIT", "0.95"))
SCORE_VERSION = os.environ.get("SCORE_VERSION", "v4")
# Split-window dip orders: in windows where BTC and ETH are on opposite sides,
# rest a limit buy of N contracts at DIP_PRICE on each crypto's own side —
# a cheap bounded-risk contrarian add that fills only if that side collapses.
SPLIT_DIP_ENABLED = os.environ.get("SPLIT_DIP_ENABLED", "0") == "1"
SPLIT_DIP_PRICE = float(os.environ.get("SPLIT_DIP_PRICE", "0.10"))
SPLIT_DIP_COUNT = int(os.environ.get("SPLIT_DIP_COUNT", "10"))
# Non-split (same-side) dip orders: smaller bounded-risk add on consensus windows
NONSPLIT_DIP_ENABLED = os.environ.get("NONSPLIT_DIP_ENABLED", "0") == "1"
NONSPLIT_DIP_COUNT = int(os.environ.get("NONSPLIT_DIP_COUNT", "1"))

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
    try:
        P(f"    [DIP] resting BUY {count} {side.upper()} @ {dip_price*100:.0f}c ({ticker})")
        result = auth_post("/portfolio/events/orders", data=order)
        oid = result.get("order_id", "")
        return oid or None
    except Exception as e:
        P(f"    [DIP] order failed: {e}")
        return None


def _place_split_dips(bets, window_end_iso):
    """Rest dip buys for the window once both BTC/ETH legs exist:
      - split (opposite sides): SPLIT_DIP_COUNT each (if SPLIT_DIP_ENABLED)
      - non-split (same side):  NONSPLIT_DIP_COUNT each (if NONSPLIT_DIP_ENABLED)
    Detects from recorded trades (legs land in separate polls). Returns True
    once dips are placed for the window."""
    # Idempotency: if a dip already exists for this window, don't place again
    # (survives bot restarts mid-window, unlike the in-memory flag)
    if any(b.get("dip_add") and b.get("window_end") == window_end_iso for b in bets):
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
    if not all(sides):
        return False
    is_split = sides[0] != sides[1]
    if is_split:
        if not SPLIT_DIP_ENABLED:
            return False
        count, dip_type = SPLIT_DIP_COUNT, "split"
    else:
        if not NONSPLIT_DIP_ENABLED:
            return False
        count, dip_type = NONSPLIT_DIP_COUNT, "nonsplit"
    P(f"  [DIP] {dip_type} window ({sides[0]}/{sides[1]}) — resting {count}x "
      f"@ {SPLIT_DIP_PRICE*100:.0f}c on both sides")
    for cr, b in wtr.items():
        oid = place_dip_order(b["ticker"], b["side"], count, SPLIT_DIP_PRICE)
        if oid:
            bets.append({
                "crypto": cr, "ticker": b["ticker"], "side": b["side"],
                "price": SPLIT_DIP_PRICE, "score": b.get("score", 0),
                "action": "trade", "result": "dip_pending", "dip_add": True,
                "dip_type": dip_type,
                "order_id": oid, "contracts": count,
                "event_ticker": b.get("event_ticker", ""),
                "window_end": window_end_iso,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "strategy_version": SCORE_VERSION,
            })
            save_bets(bets)
        time.sleep(0.3)
    return True


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


def save_bets(bets):
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
    except OSError:
        # Fallback: direct write if atomic rename fails (Railway volume issue)
        with open(BETS_FILE, "w") as f:
            json.dump(bets, f, indent=2, default=str)
            f.flush()
            os.fsync(f.fileno())


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
# Group A: signals 4,5 — up: 3/4 wins, down: 4 losses in 16 (since scale-up)
# Group B: signals 1,2,3 — up: 4/6 wins, down: 4 losses in 16 (since scale-up)
# Signals 0,6,7 never scale and don't feed streak history
# (0 = coin-flip noise; 6,7 = negative EV in both halves of backtest data)
SCALE_GROUPS = {
    "A": {"signals": {4, 5}, "up_window": 4, "up_thresh": 3, "down_window": 16, "down_thresh": 4},
    "B": {"signals": {1, 2, 3}, "up_window": 6, "up_thresh": 4, "down_window": 16, "down_thresh": 4},
}
SCALE_UP_COUNT = int(os.environ.get("SCALE_UP_COUNT", 5))
COOL_OFF_WR = float(os.environ.get("COOL_OFF_WR", "0.8"))       # 0 disables
COOL_OFF_WINDOW = int(os.environ.get("COOL_OFF_WINDOW", "10"))
# Bypass the lid during a clean consecutive win streak of >= N (0 disables).
# Distinguishes a durable run (ride it at full size) from choppy euphoria.
COOL_OFF_BYPASS_STREAK = int(os.environ.get("COOL_OFF_BYPASS_STREAK", "6"))
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

def get_dynamic_contracts(bets, crypto, signal_count):
    """Grouped scaling with cool-off lid: BTC+ETH combined, split up/down windows."""
    global _cool_off_blocked
    _cool_off_blocked = 0
    contracts = _group_scale_contracts(bets, crypto, signal_count)
    # Cool-off: trailing WR >= threshold means the run is euphoric — forward
    # edge is ~zero there, so cap at 1x until it cools (group states unaffected)
    if contracts > 1 and COOL_OFF_WR > 0:
        resolved = [b for b in bets if b.get("action") == "trade"
                    and b.get("result") in ("win", "loss")
                    and b.get("crypto") in ("BTC", "ETH") and not b.get("dip_add")]
        last = resolved[-COOL_OFF_WINDOW:]
        if len(last) >= COOL_OFF_WINDOW:
            wr = sum(1 for b in last if b["result"] == "win") / len(last)
            if wr >= COOL_OFF_WR:
                # Bypass the lid on a clean consecutive win streak — a durable
                # run keeps its edge, unlike choppy 80% (wins with losses mixed)
                streak = 0
                for b in reversed(resolved):
                    if b["result"] == "win":
                        streak += 1
                    else:
                        break
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
    """Check resting split-window dip orders: filled -> becomes an open position,
    unfilled+closed -> discarded. Called at window start."""
    changed = False
    for bet in bets:
        if bet.get("result") != "dip_pending":
            continue
        oid = bet.get("order_id")
        if not oid:
            bet["result"] = "dip_expired"; changed = True; continue
        try:
            od = auth_get(f"/portfolio/orders/{oid}").get("order", {})
            status = od.get("status", "")
            # Require POSITIVE fill confirmation — never default remaining to 0,
            # or a canceled order (minimal API record) is falsely booked as filled
            has_counts = od.get("count") is not None and od.get("remaining_count") is not None
            filled = 0
            if has_counts:
                filled = int(od["count"]) - int(od["remaining_count"])
            if filled > 0:
                avg_p = od.get("avg_price")
                if avg_p is not None and avg_p > 1:
                    avg_p = avg_p / 100
                if avg_p is not None and bet.get("side") == "no":
                    avg_p = 1.0 - avg_p
                bet["fill_price"] = avg_p if avg_p else bet.get("price", SPLIT_DIP_PRICE)
                bet["filled_count"] = filled
                bet["result"] = "open"   # normal settlement resolves it next
                P(f"  [DIP] {bet.get('crypto','')} filled {filled} @ {bet['fill_price']:.2f}")
                changed = True
            elif status in ("canceled", "expired") or (has_counts and filled == 0):
                # market closed / order canceled without the dip triggering
                bet["result"] = "dip_expired"
                changed = True
            # else: unknown (no counts, still open) — leave pending, retry next
            time.sleep(0.2)
        except Exception:
            pass
    if changed:
        save_bets(bets)
    return bets

def _resolve_open_bets(bets):
    """Quickly resolve open bets from past windows so scaling has fresh data."""
    now = datetime.now(timezone.utc)
    # Always run (no-op when none pending) so dips resolve even if the feature
    # is toggled off while orders are still outstanding
    if any(b.get("result") == "dip_pending" for b in bets):
        bets = _resolve_dip_orders(bets)
    changed = False
    for bet in bets:
        if bet.get("result") != "open" or bet.get("action") != "trade":
            continue
        # Never book settlement P&L without confirmed fill evidence — the
        # dashboard resolver verifies unconfirmed orders against the API
        if bet.get("filled_count", 0) <= 0:
            continue
        # Only resolve bets whose window has ended (ticker encodes the time)
        ticker = bet.get("ticker", "")
        try:
            mkt = public_get(f"/markets/{ticker}")
            market = mkt.get("market", {})
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
                P(f"  [RESOLVE] {bet.get('crypto','')} {ticker}: {bet['result']}")
            time.sleep(0.3)
        except Exception:
            pass
    if changed:
        save_bets(bets)
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
            P(f"  Scanning {len(CRYPTOS)} markets...")
            for c, cfg2 in CRYPTOS.items():
                if c in placed_this_window:
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
            pending_orders = []  # [{crypto, ticker, side, price, order_id, bet_record}, ...]
            for tq in trade_queue:
                sig_count = tq["score"] + 3
                current_contracts = get_dynamic_contracts(bets, tq["crypto"], sig_count)
                tq["bet_record"]["contracts"] = current_contracts
                if _cool_off_blocked > 0:
                    tq["bet_record"]["cool_off"] = True
                    tq["bet_record"]["blocked_scale"] = _cool_off_blocked
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
            if (SPLIT_DIP_ENABLED or NONSPLIT_DIP_ENABLED) and not dips_done_this_window:
                if _place_split_dips(bets, window_end.isoformat()):
                    dips_done_this_window = True

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
                                cancel_resp = auth_delete(f"/portfolio/events/orders/{po['order_id']}")
                                P(f"    {po['crypto']}: Canceled (reduced_by={cancel_resp.get('reduced_by', '?')})")
                            except Exception as ce:
                                P(f"    {po['crypto']}: Cancel error: {ce}")
                                try:
                                    time.sleep(1)
                                    cancel_resp = auth_delete(f"/portfolio/events/orders/{po['order_id']}")
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
