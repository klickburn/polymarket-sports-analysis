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
    auth_get, auth_post, public_get,
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
MIN_SCORE = int(os.environ.get("SCORE_MIN_SCORE", "0"))
TAKE_PROFIT_PRICE = float(os.environ.get("SCORE_TAKE_PROFIT", "0.95"))
SCORE_VERSION = os.environ.get("SCORE_VERSION", "v4")

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
    """v5 — Stoch + consensus filter. Score=0→GO, <0→SKIP.
    Filters:
      1. Stoch < 30 (oversold confirmation)
      2. All cryptos in window agree on direction (same side)
    """
    if sym not in indicators:
        return None, []
    ind = indicators[sym]
    s = 0
    reasons = []

    # Filter 1: Stoch must be < 30
    stoch = ind["stoch"]
    if stoch >= 30:
        s -= 1; reasons.append(("Stoch High", f"{stoch:.1f} ≥30", "-1"))
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

    return s, reasons


# ── Version dispatcher ─────────────────────────────────────────────────
SCORE_VERSIONS = {"v1": compute_score_v1, "v2": compute_score_v2,
                  "v3": compute_score_v3, "v4": compute_score_v4,
                  "v5": compute_score_v5}

def compute_score(sym, side, price, indicators):
    fn = SCORE_VERSIONS.get(SCORE_VERSION, compute_score_v4)
    return fn(sym, side, price, indicators)


# ── Take profit ────────────────────────────────────────────────────────
def place_take_profit(ticker, side, count):
    """Place a limit sell order at TAKE_PROFIT_PRICE to lock in gains."""
    if TAKE_PROFIT_PRICE <= 0:
        return None
    tp_cents = int(round(TAKE_PROFIT_PRICE * 100))
    order = {
        "ticker": ticker,
        "action": "sell",
        "side": side,
        "type": "limit",
        "count": count,
        "client_order_id": str(uuid.uuid4()),
    }
    if side == "yes":
        order["yes_price"] = tp_cents
    else:
        order["no_price"] = tp_cents
    try:
        P(f"    Take-profit: SELL {count} @ {tp_cents}c ({side.upper()})")
        result = auth_post("/portfolio/orders", data=order)
        order_data = result.get("order", {})
        status = order_data.get("status", "unknown")
        P(f"    TP order status: {status}")
        return result
    except Exception as e:
        P(f"    TP ORDER FAILED: {e}")
        return None


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


# ── Git backup ─────────────────────────────────────────────────────────
GIT_BETS_FILE = os.path.join(os.path.dirname(__file__) or ".", "crypto_score_bets.json")
_last_git_backup = 0
GIT_BACKUP_INTERVAL = 900  # 15 min

def git_backup_bets(bets):
    """Save bets to repo and push to git for optimizer/strategy use."""
    global _last_git_backup
    now = time.time()
    if now - _last_git_backup < GIT_BACKUP_INTERVAL:
        return
    _last_git_backup = now
    try:
        import subprocess
        repo_dir = os.path.dirname(__file__) or "."
        # Write bets to repo file
        with open(GIT_BETS_FILE, "w") as f:
            json.dump(bets, f, indent=2, default=str)
        # Git add, commit, push
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        subprocess.run(["git", "add", "crypto_score_bets.json"], cwd=repo_dir,
                       capture_output=True, timeout=30)
        result = subprocess.run(
            ["git", "commit", "-m", f"data: score bot bets {ts}"],
            cwd=repo_dir, capture_output=True, timeout=30)
        if result.returncode == 0:
            # Pull rebase then push (handle concurrent bot commits)
            subprocess.run(["git", "pull", "--rebase"], cwd=repo_dir,
                           capture_output=True, timeout=60)
            subprocess.run(["git", "push"], cwd=repo_dir,
                           capture_output=True, timeout=60)
            P(f"  [GIT] Backed up {len(bets)} bets to repo")
    except Exception as e:
        P(f"  [GIT] Backup failed: {e}")


# ── Main loop ───────────────────────────────────────────────────────────
def run(live=False):
    P("=" * 65)
    P(f"  CRYPTO SCORE BOT — {SCORE_VERSION.upper()} Scoring Engine")
    P(f"  Mode: {'LIVE' if live else 'DRY RUN'}")
    P(f"  Strategy: {SCORE_VERSION} | Entry: minute {ENTRY_AFTER_MINUTES}+ | Min score: {MIN_SCORE}")
    tp_str = f"{TAKE_PROFIT_PRICE*100:.0f}c" if TAKE_PROFIT_PRICE > 0 else "OFF"
    P(f"  Price range: {MIN_PRICE*100:.0f}-{MAX_PRICE*100:.0f}c | {CONTRACT_COUNT} contracts | TP: {tp_str}")
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
    PREFETCH_MINUTE = ENTRY_AFTER_MINUTES  # Fetch CoinGecko right at entry time

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
                locked_side = None  # Lock direction after first trade
                checked_positions = False
                fetched_indicators = False
                P(f"\n  -- Window {window_start.strftime('%H:%M')}-{window_end.strftime('%H:%M')} UTC --")

            # Too early — sleep until prefetch time
            if mins_in < PREFETCH_MINUTE:
                time.sleep(POLL_INTERVAL)
                continue

            # Fetch CoinGecko right before trading
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

            # Single pass: collect sides for consensus + evaluate trades
            CONSENSUS_EXCLUDE = {"BNB", "HYPE"}
            crypto_snapshots = {}
            for c, cfg2 in CRYPTOS.items():
                if c in placed_this_window:
                    continue
                time.sleep(2)
                mkt2, ev2 = find_current_market(cfg2["series"])
                if not mkt2:
                    continue
                time.sleep(2)
                s2, p2 = get_dominant_side(mkt2["ticker"])
                crypto_snapshots[c] = {"market": mkt2, "event": ev2, "side": s2, "price": p2}

            # Build consensus from snapshots (excluding BNB/HYPE)
            if SCORE_VERSION == "v5":
                window_sides = {}
                for c, snap in crypto_snapshots.items():
                    if c in CONSENSUS_EXCLUDE:
                        continue
                    if snap["side"] and snap["price"] and snap["price"] >= 0.60:
                        window_sides[c] = snap["side"]
                indicators["_window_sides"] = window_sides
                P(f"  Window sides: {window_sides}")

            # Now evaluate each crypto using the collected data
            for crypto, snap in crypto_snapshots.items():
                if crypto in placed_this_window:
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

                # Lock direction after first trade — skip opposite side
                if locked_side and side != locked_side:
                    P(f"    {crypto}: SKIP (side {side.upper()} != locked {locked_side.upper()})")
                    continue

                # Re-fetch BTC from CoinGecko for fresh ret_1h before each trade
                btc_url = f"{COINGECKO}/coins/bitcoin/market_chart?vs_currency=usd&days=1"
                btc_data = fetch_coingecko(btc_url)
                if btc_data and "prices" in btc_data:
                    btc_prices = [p[1] for p in sorted(btc_data["prices"], key=lambda x: x[0])]
                    if len(btc_prices) >= 24:
                        old_btc = indicators.get("BTC", {})
                        fresh_ind = compute_indicators({"BTC": btc_prices})
                        if "BTC" in fresh_ind:
                            indicators["BTC"] = fresh_ind["BTC"]

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
                    score_breakdown[factor] = {"detail": detail, "points": int(pts) if pts not in ("pass",) else 0}

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
                        # Place take-profit sell order
                        if TAKE_PROFIT_PRICE > 0 and price < TAKE_PROFIT_PRICE:
                            time.sleep(1)
                            tp_result = place_take_profit(ticker, side, CONTRACT_COUNT)
                            if tp_result:
                                tp_order = tp_result.get("order", {})
                                bet_record["tp_order_id"] = tp_order.get("order_id", "")
                                bet_record["tp_price"] = TAKE_PROFIT_PRICE
                        bets.append(bet_record)
                        save_bets(bets)
                        total_new += 1
                        placed_this_window.add(crypto)
                        locked_side = side  # Lock direction for rest of window
                        tp_str = f" | TP @ {TAKE_PROFIT_PRICE*100:.0f}c" if TAKE_PROFIT_PRICE > 0 else ""
                        P(f"    {crypto}: BET PLACED | {side.upper()} @ {price:.2f} | Score {score:+d}{tp_str}")
                    else:
                        P(f"    {crypto}: Order failed")
                else:
                    P(f"    {crypto}: [DRY RUN] {side.upper()} @ {price:.2f} | Score {score:+d}")
                    bet_record["status"] = "dry_run"
                    bets.append(bet_record)
                    save_bets(bets)
                    total_new += 1
                    placed_this_window.add(crypto)

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
