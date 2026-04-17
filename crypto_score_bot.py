"""
Kalshi 15-Minute Crypto Score Bot
==================================
Trades all 7 cryptos on Kalshi's 15-min prediction markets using a
CoinGecko-powered scoring engine.

Strategy: "Score-Based Late Entry"
  - In the last 4 minutes of each 15-min window (minute 11+)
  - Check which side is >75¢ (dominant side)
  - Compute score using RSI, stochastic, momentum, volatility, BTC correlation
  - Only bet if score >= 0

Usage:
    python3 crypto_score_bot.py              # Dry run
    python3 crypto_score_bot.py --live       # Place real bets
"""

import os
import sys
import json
import time
import math
import urllib.request
from datetime import datetime, timezone, timedelta

from crypto_15m_bot import (
    auth_get, auth_post, public_get,
    get_balance, get_existing_positions, get_open_orders,
    place_order, get_current_window, minutes_until_strike,
    find_current_market, get_dominant_side, P,
)

# ── Config ──────────────────────────────────────────────────────────────
BET_AMOUNT = float(os.environ.get("SCORE_BET_AMOUNT", "0.10"))
CONTRACT_COUNT = int(os.environ.get("SCORE_CONTRACT_COUNT", "1"))
ENTRY_AFTER_MINUTES = int(os.environ.get("SCORE_ENTRY_MINUTES", "11"))
POLL_INTERVAL = int(os.environ.get("SCORE_POLL_INTERVAL", "5"))
MIN_PRICE = 0.75
MAX_PRICE = 0.99
MIN_SCORE = int(os.environ.get("SCORE_MIN_SCORE", "-2"))

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
    "XRP": "ripple", "DOGE": "dogecoin", "BNB": "binancecoin",
    "HYPE": "hyperliquid",
}

COINGECKO = "https://api.coingecko.com/api/v3"

# ── CoinGecko data fetching ────────────────────────────────────────────
def fetch_coingecko(url, retries=2):
    for attempt in range(retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
            resp = urllib.request.urlopen(req, timeout=12)
            return json.loads(resp.read().decode())
        except Exception:
            if attempt < retries:
                time.sleep(1.5)
            else:
                return None


def fetch_crypto_prices():
    """Fetch 24h price data from CoinGecko for all cryptos."""
    crypto_data = {}
    for sym, cid in COIN_IDS.items():
        url = f"{COINGECKO}/coins/{cid}/market_chart?vs_currency=usd&days=1"
        data = fetch_coingecko(url)
        if data and "prices" in data:
            prices = [p[1] for p in sorted(data["prices"], key=lambda x: x[0])]
            if len(prices) >= 24:
                crypto_data[sym] = prices
        time.sleep(0.4)
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
    """Compute RSI, stochastic, momentum, volatility for all cryptos."""
    indicators = {}
    for sym in CRYPTOS:
        if sym not in crypto_data:
            continue
        pr = crypto_data[sym]
        n12 = min(12, len(pr) - 1)
        n36 = min(36, len(pr) - 1)
        ret_1h = (pr[-1] - pr[-n12]) / pr[-n12] * 100 if pr[-n12] else 0
        ret_3h = (pr[-1] - pr[-n36]) / pr[-n36] * 100 if pr[-n36] else 0

        hourly_rets = []
        for i in range(12, min(72, len(pr)), 12):
            idx = len(pr) - 1 - i
            idx_prev = len(pr) - 1 - i - 12
            if idx_prev >= 0 and pr[idx_prev]:
                hourly_rets.append(abs((pr[idx] - pr[idx_prev]) / pr[idx_prev] * 100))

        vol_6h = sum(hourly_rets) / len(hourly_rets) if hourly_rets else 0.5
        rsi = calc_rsi(pr[-min(100, len(pr)):], 14)
        stoch = calc_stoch(pr[-min(70, len(pr)):], 14)
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


# ── Scoring engine ──────────────────────────────────────────────────────
def compute_score(sym, side, price, indicators):
    """Score a potential trade. Returns (score, reasons_list)."""
    if sym not in indicators:
        return None, []
    ind = indicators[sym]
    now = datetime.now(timezone(timedelta(hours=-5)))
    s = 0
    reasons = []

    # Price penalty
    if price >= 0.97:
        s -= 2; reasons.append(("Price", f"{price:.0%}", "-2"))
    elif price >= 0.95:
        s -= 2; reasons.append(("Price", f"{price:.0%}", "-2"))
    elif price >= 0.93:
        s -= 2; reasons.append(("Price", f"{price:.0%}", "-2"))
    elif price < 0.70:
        s -= 1; reasons.append(("Price", f"{price:.0%}", "-1"))

    # Crypto bonus/penalty
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

    # Momentum vs side
    ret = ind["ret_1h"]
    if ret > 0.6 and side == "no":
        s -= 2; reasons.append(("Momentum", f"UP {ret:+.2f}% vs NO", "-2"))
    elif ret < -0.6 and side == "yes":
        s -= 2; reasons.append(("Momentum", f"DOWN {ret:+.2f}% vs YES", "-2"))

    # BTC correlation
    btc_ret = indicators.get("BTC", {}).get("ret_1h", 0)
    if sym != "BTC":
        if btc_ret > 0.3 and side == "no":
            s -= 3; reasons.append(("BTC", "UP vs NO", "-3"))
        elif btc_ret < -0.3 and side == "yes":
            s -= 3; reasons.append(("BTC", "DOWN vs YES", "-3"))

    # Volatility
    vol = ind["vol_6h"]
    if vol > 1.0:
        s -= 1; reasons.append(("Vol", "high", "-1"))
    elif vol < 0.3 and side == "no":
        pass  # vol_calm_no: removed penalty (was -1)
    elif vol < 0.3 and side == "yes":
        s -= 1; reasons.append(("Vol", "calm+YES", "-1"))

    # RSI
    rsi = ind["rsi"]
    if rsi > 65 and side == "yes":
        s -= 3; reasons.append(("RSI", f"{rsi:.0f}+YES", "-3"))
    elif rsi < 25 and side == "no":
        s -= 3; reasons.append(("RSI", f"{rsi:.0f}+NO", "-3"))

    # Stochastic
    stoch = ind["stoch"]
    if stoch > 80 and side == "yes":
        s -= 3; reasons.append(("Stoch", f"{stoch:.0f}+YES", "-3"))
    elif stoch < 10 and side == "no":
        s -= 3; reasons.append(("Stoch", f"{stoch:.0f}+NO", "-3"))

    # Pack agreement
    pa = ind["pack_agreement"]
    if pa > 0.8:
        if ind["ret_1h"] > 0 and side == "no":
            s -= 1; reasons.append(("Pack", "up vs NO", "-1"))
        elif ind["ret_1h"] < 0 and side == "yes":
            s -= 1; reasons.append(("Pack", "down vs YES", "-1"))

    # 3h big move
    if abs(ind["ret_3h"]) > 1.5:
        pass  # ext_3h: tightened threshold but removed penalty (was -1)

    return s, reasons


# ── Load/save bets ──────────────────────────────────────────────────────
def load_bets():
    if os.path.exists(BETS_FILE):
        with open(BETS_FILE) as f:
            return json.load(f)
    return []


def save_bets(bets):
    with open(BETS_FILE, "w") as f:
        json.dump(bets, f, indent=2, default=str)


def save_status(status):
    with open(STATUS_FILE, "w") as f:
        json.dump(status, f, indent=2, default=str)


# ── Main loop ───────────────────────────────────────────────────────────
def run(live=False):
    P("=" * 65)
    P("  CRYPTO SCORE BOT — CoinGecko Scoring Engine")
    P(f"  Mode: {'LIVE' if live else 'DRY RUN'}")
    P(f"  Entry: minute {ENTRY_AFTER_MINUTES}+ | Min score: {MIN_SCORE}")
    P(f"  Price range: {MIN_PRICE*100:.0f}-{MAX_PRICE*100:.0f}c | {CONTRACT_COUNT} contracts")
    P(f"  Cryptos: {', '.join(CRYPTOS.keys())}")
    P("=" * 65)

    if live:
        bal = get_balance()
        if bal:
            P(f"  Balance: ${bal['balance']:.2f} | Portfolio: ${bal['portfolio_value']:.2f}")

    bets = load_bets()
    total_new = 0
    last_window_end = None
    placed_this_window = set()
    skip_tickers = set()

    # Indicators cache
    indicators = {}
    checked_positions = False
    fetched_indicators = False
    PREFETCH_MINUTE = ENTRY_AFTER_MINUTES - 1  # Fetch CoinGecko 1 min before entry

    P(f"\n  Running continuously — polling every {POLL_INTERVAL}s...")

    while True:
        try:
            window_start, window_end = get_current_window()
            mins_left = minutes_until_strike()
            mins_in = 15 - mins_left

            # New window? Reset
            if window_end != last_window_end:
                last_window_end = window_end
                placed_this_window = set()
                checked_positions = False
                fetched_indicators = False
                P(f"\n  -- Window {window_start.strftime('%H:%M')}-{window_end.strftime('%H:%M')} UTC --")

            # Too early — sleep until prefetch time
            if mins_in < PREFETCH_MINUTE:
                time.sleep(POLL_INTERVAL)
                continue

            # Fetch CoinGecko 1 min before entry (minute 10)
            if not fetched_indicators:
                P("  Fetching CoinGecko data...")
                crypto_data = fetch_crypto_prices()
                if crypto_data:
                    indicators = compute_indicators(crypto_data)
                    P(f"  Got indicators for {len(indicators)} cryptos")

                    # Save status for dashboard
                    status = {
                        "last_update": datetime.now(timezone.utc).isoformat(),
                        "indicators": {},
                    }
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
                    save_status(status)
                else:
                    P("  WARNING: CoinGecko fetch failed")
                fetched_indicators = True

            # Wait until entry time (minute 11)
            if mins_in < ENTRY_AFTER_MINUTES:
                time.sleep(POLL_INTERVAL)
                continue

            # Fetch positions/orders once per window, right before trading
            if not checked_positions:
                open_order_tickers = get_open_orders()
                time.sleep(1)
                existing_positions = get_existing_positions()
                skip_tickers = open_order_tickers | existing_positions
                if skip_tickers:
                    P(f"  Skipping {len(skip_tickers)} tickers with open orders/positions")
                checked_positions = True
                time.sleep(1)

            if not indicators:
                time.sleep(POLL_INTERVAL)
                continue

            # Check each crypto — 1.5s between each to avoid 429
            for crypto, cfg in CRYPTOS.items():
                if crypto in placed_this_window:
                    continue

                time.sleep(1.5)
                market, event = find_current_market(cfg["series"])
                if not market:
                    continue
                ticker = market["ticker"]

                if ticker in skip_tickers:
                    placed_this_window.add(crypto)
                    continue
                if any(b.get("ticker") == ticker for b in bets):
                    placed_this_window.add(crypto)
                    continue

                time.sleep(1.5)
                side, price = get_dominant_side(ticker)
                if not side or not price:
                    continue

                # Only trade dominant side >75c
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
                    score_breakdown[factor] = {"detail": detail, "points": int(pts)}

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
                }

                if score < MIN_SCORE:
                    P(f"    {crypto}: SKIP (score {score:+d} < {MIN_SCORE})")
                    bet_record["action"] = "skip"
                    bets.append(bet_record)
                    save_bets(bets)
                    placed_this_window.add(crypto)
                    continue

                bet_record["action"] = "trade"

                if live:
                    result = place_order(ticker, side, price, BET_AMOUNT, count=CONTRACT_COUNT)
                    if result:
                        order = result.get("order", {})
                        bet_record["order_id"] = order.get("order_id", "")
                        bet_record["status"] = order.get("status", "")
                        bet_record["fill_price"] = order.get("avg_price", price)
                        bets.append(bet_record)
                        save_bets(bets)
                        total_new += 1
                        placed_this_window.add(crypto)
                        P(f"    {crypto}: BET PLACED | {side.upper()} @ {price:.2f} | Score {score:+d}")
                    else:
                        P(f"    {crypto}: Order failed")
                else:
                    P(f"    {crypto}: [DRY RUN] {side.upper()} @ {price:.2f} | Score {score:+d}")
                    bet_record["status"] = "dry_run"
                    bets.append(bet_record)
                    save_bets(bets)
                    total_new += 1
                    placed_this_window.add(crypto)

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
