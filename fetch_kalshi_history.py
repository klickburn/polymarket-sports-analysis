"""
Kalshi Historical Sports Data Fetcher
=====================================
Pulls all settled sports markets from Kalshi and records:
  - Pre-game closing odds (yes price in cents right before game action)
  - Winner (market.result: "yes" or "no")
  - Teams / title / timing metadata

Heuristic for pre-game price: hourly candlesticks over the market's lifetime,
take the close of the candle immediately BEFORE the candle with the highest
volume (peak volume = in-game trading, so the candle before it is the last
pre-game snapshot).

Output: kalshi_history.json — a list of game records.
"""

import os
import json
import time
from datetime import datetime

from kalshi_bot import auth_get, public_get


# Series tickers for each sport on Kalshi (verified via /series API).
SPORTS_SERIES = [
    # US major leagues
    "KXNBAGAME",       # NBA
    "KXNFLGAME",       # NFL
    "KXMLBGAME",       # MLB
    "KXNHLGAME",       # NHL
    "KXWNBAGAME",      # WNBA
    "KXMLSGAME",       # MLS
    # College
    "KXNCAAFGAME",     # College football
    "KXNCAABGAME",     # College basketball (men)
    "KXNCAAWBGAME",    # College basketball (women)
    # Soccer
    "KXEPLGAME",       # English Premier League
    "KXUCLGAME",       # UEFA Champions League
    "KXLIGUE1GAME",    # Ligue 1
    "KXSAUDIPLGAME",   # Saudi Pro League
    "KXLIGAMXGAME",    # Liga MX
    # Cricket
    "KXIPLGAME",       # IPL
    "KXPSLGAME",       # Pakistan Super League
    # Esports
    "KXCSGOGAME",      # Counter-Strike 2 (legacy ticker)
    "KXCS2GAME",       # Counter-Strike 2
    "KXLOLGAME",       # League of Legends
    "KXDOTA2GAME",     # Dota 2
    # Tennis
    "KXATPGAME",       # ATP
    "KXWTAGAME",       # WTA
    # Other
    "KXUFLGAME",       # UFL
    "KXKBOGAME",       # Korea KBO Baseball
    "KXNPBGAME",       # Japan NPB Baseball
]

OUT_FILE = os.environ.get("KALSHI_HISTORY_FILE", "kalshi_history.json")
RATE_LIMIT_SLEEP = float(os.environ.get("HISTORY_RATE_LIMIT", "0.5"))
MAX_RETRIES = 5


def P(msg=""):
    print(msg, flush=True)


def _api_call(path, params=None):
    """public_get with retry + exponential backoff on 429."""
    for attempt in range(MAX_RETRIES):
        try:
            return public_get(path, params=params)
        except Exception as e:
            if "429" in str(e):
                wait = (2 ** attempt) + 1
                P(f"  [HISTORY] 429 rate-limited, waiting {wait}s (attempt {attempt+1}/{MAX_RETRIES})")
                time.sleep(wait)
            else:
                raise
    return public_get(path, params=params)  # final attempt, let it raise


def _parse_ts(s):
    if not s:
        return None
    try:
        return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
    except Exception:
        return None


def fetch_settled_markets(series_ticker):
    """Paginate through all settled markets for a series."""
    markets = []
    cursor = None
    while True:
        params = {
            "series_ticker": series_ticker,
            "status": "settled",
            "limit": 1000,
        }
        if cursor:
            params["cursor"] = cursor
        try:
            data = _api_call("/markets", params=params)
        except Exception as e:
            P(f"  [{series_ticker}] fetch error: {e}")
            break
        batch = data.get("markets", [])
        markets.extend(batch)
        cursor = data.get("cursor")
        if not cursor or not batch:
            break
        time.sleep(RATE_LIMIT_SLEEP)
    return markets


def _candle_volume(c):
    try:
        return float(c.get("volume_fp", 0) or 0)
    except Exception:
        return 0


def _candle_close_dollars(c):
    price = c.get("price") or {}
    val = price.get("close_dollars") or price.get("mean_dollars")
    if val is None:
        return None
    try:
        return float(val)
    except Exception:
        return None


def get_pregame_yes_price(series_ticker, market_ticker, open_ts, close_ts):
    """Return yes close price (dollars, 0.0–1.0) right before the game starts.

    Heuristic: find the first candle where volume spikes >3× the running
    median of all prior candles. That candle is game-start; the one before
    it is the last pre-game snapshot.

    Kalshi's candlesticks endpoint caps the window so we fetch in chunks.
    """
    if not open_ts or not close_ts or close_ts <= open_ts:
        return None

    all_candles = []
    CHUNK = 7 * 24 * 3600
    cur = open_ts
    while cur < close_ts:
        chunk_end = min(cur + CHUNK, close_ts)
        try:
            data = _api_call(
                f"/series/{series_ticker}/markets/{market_ticker}/candlesticks",
                params={
                    "start_ts": cur,
                    "end_ts": chunk_end,
                    "period_interval": 60,
                },
            )
        except Exception:
            break
        all_candles.extend(data.get("candlesticks", []))
        cur = chunk_end
        time.sleep(RATE_LIMIT_SLEEP)

    if not all_candles:
        return None

    # Find first volume spike (game start):
    #   volume > 3× median of prior candles AND volume > 5000 (absolute floor)
    # The absolute floor prevents false positives on tiny early-market fluctuations.
    MIN_SPIKE_VOL = 5000
    vols = [_candle_volume(c) for c in all_candles]
    spike_idx = None
    for i in range(2, len(vols)):
        prior = sorted(vols[:i])
        median_prior = prior[len(prior) // 2]
        if vols[i] >= MIN_SPIKE_VOL and median_prior > 0 and vols[i] > 3 * median_prior:
            spike_idx = i
            break

    if spike_idx is not None and spike_idx > 0:
        pick = all_candles[spike_idx - 1]
    else:
        # Fallback: use the candle before the peak volume candle
        max_idx = max(range(len(all_candles)), key=lambda i: vols[i])
        pick = all_candles[max_idx - 1] if max_idx > 0 else all_candles[0]

    return _candle_close_dollars(pick)


def load_existing(path):
    if not os.path.exists(path):
        return []
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return []


def save(path, records):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(records, f, default=str)
    os.replace(tmp, path)


def fetch_all(out_file=OUT_FILE, resume=True):
    records = load_existing(out_file) if resume else []
    done = {r["ticker"] for r in records}
    P(f"  [HISTORY] Starting fetch. {len(done)} records already cached.")

    for series in SPORTS_SERIES:
        P(f"  [HISTORY] {series}: fetching settled markets...")
        markets = fetch_settled_markets(series)
        P(f"  [HISTORY] {series}: {len(markets)} settled markets")
        if not markets:
            continue

        new_in_series = 0
        for i, m in enumerate(markets):
            ticker = m.get("ticker", "")
            if not ticker or ticker in done:
                continue
            result = m.get("result", "")
            if result not in ("yes", "no"):
                continue

            open_ts = _parse_ts(m.get("open_time", ""))
            close_ts = _parse_ts(m.get("close_time", ""))

            try:
                pregame_yes = get_pregame_yes_price(series, ticker, open_ts, close_ts)
            except Exception as e:
                P(f"  [HISTORY] Candlestick error for {ticker}: {e}")
                pregame_yes = None

            records.append({
                "ticker": ticker,
                "event_ticker": m.get("event_ticker", ""),
                "series": series,
                "title": m.get("title", ""),
                "yes_sub_title": m.get("yes_sub_title", ""),
                "no_sub_title": m.get("no_sub_title", ""),
                "open_time": m.get("open_time", ""),
                "close_time": m.get("close_time", ""),
                "result": result,
                "pregame_yes_dollars": pregame_yes,
            })
            done.add(ticker)
            new_in_series += 1

            if new_in_series % 20 == 0:
                save(out_file, records)
                P(f"  [HISTORY] {series}: {new_in_series}/{len(markets)} processed, {len(records)} total")

            time.sleep(RATE_LIMIT_SLEEP)

        save(out_file, records)
        P(f"  [HISTORY] {series}: done ({new_in_series} new)")

    save(out_file, records)
    P(f"  [HISTORY] Complete. Total records: {len(records)}")
    return records


if __name__ == "__main__":
    fetch_all()
