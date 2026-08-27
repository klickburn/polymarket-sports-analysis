"""Scoring engine for the Polymarket bot — a deliberate, standalone COPY.

Extracted verbatim from crypto_score_bot.py so the two services share no files.
The duplication is intentional: a change to the Kalshi bot must not be able to
alter Polymarket behaviour, or vice versa. If you change a scoring rule and want
it in both places, change it twice on purpose.

Pure functions over CoinGecko data — no venue, no exchange, no order logic.
"""
import os
import time
import json
import urllib.request
from datetime import datetime, timezone

COINGECKO = "https://api.coingecko.com/api/v3"
SCORE_VERSION = "v7"     # matches the deployed Kalshi SCORE_VERSION

# Only the assets Polymarket lists 15m up/down windows for. The Kalshi copy
# carries a `series` ticker per crypto; that is Kalshi-specific and dropped here.
CRYPTOS = ["BTC", "ETH"]

COIN_IDS = {
    "BTC": "bitcoin", "ETH": "ethereum", "SOL": "solana",
    "XRP": "ripple", "DOGE": "dogecoin",
    "BNB": "binancecoin", "HYPE": "hyperliquid",
}

# Own env var names so the two services can hold different CoinGecko keys.
CG_API_KEYS = [
    os.environ.get("POLY_CG_API_KEY", "CG-djNqgGcv7UfYvqDfKsxWX1ii"),
    os.environ.get("POLY_CG_API_KEY_2", "CG-hx9L9wzotJeCZ1xeeLoJqJT9"),
    os.environ.get("POLY_CG_API_KEY_3", "CG-5sTc7yccYpF1zWVWfDduHT8i"),
]
_cg_key_index = 0


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
                print(f"    CoinGecko retry {attempt+1}/{retries} with next key ({e})")
                time.sleep(wait)
            else:
                print(f"    CoinGecko FAILED for {url.split('/')[-2]}: {e}")
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


def compute_score(sym, side, price, indicators):
    """Single-version dispatch; the Kalshi service is on v7."""
    return compute_score_v7(sym, side, price, indicators)
