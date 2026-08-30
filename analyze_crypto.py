"""Quick analysis of crypto 15m bot performance from Kalshi API fills."""
import os, sys, json, time, base64, requests
from datetime import datetime, timezone

API_BASE = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_KEY_ID = os.environ.get("KALSHI_KEY_ID", "")
KALSHI_PRIVATE_KEY = os.environ.get("KALSHI_PRIVATE_KEY", "")

session = requests.Session()
_private_key = None

CRYPTO_SERIES = {
    "KXBTC15M": "BTC",
    "KXETH15M": "ETH",
    "KXSOL15M": "SOL",
    "KXXRP15M": "XRP",
    "KXDOGE15M": "DOGE",
}

def get_private_key():
    global _private_key
    if _private_key:
        return _private_key
    from cryptography.hazmat.primitives import serialization
    key_pem = KALSHI_PRIVATE_KEY.strip().replace("\\n", "\n")
    _private_key = serialization.load_pem_private_key(key_pem.encode(), password=None)
    return _private_key

def sign_request(method, path, timestamp_ms):
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import padding
    pk = get_private_key()
    message = f"{timestamp_ms}{method}{path}".encode("utf-8")
    signature = pk.sign(message, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH), hashes.SHA256())
    return base64.b64encode(signature).decode("utf-8")

def auth_get(path, params=None):
    url = f"{API_BASE}{path}"
    full_path = f"/trade-api/v2{path}"
    ts = str(int(time.time() * 1000))
    sig = sign_request("GET", full_path, ts)
    headers = {"KALSHI-ACCESS-KEY": KALSHI_KEY_ID, "KALSHI-ACCESS-SIGNATURE": sig, "KALSHI-ACCESS-TIMESTAMP": ts}
    r = session.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def public_get(path, params=None):
    r = session.get(f"{API_BASE}{path}", params=params, timeout=30)
    r.raise_for_status()
    return r.json()

# Fetch all fills
print("Fetching fills...")
all_fills = []
cursor = None
while True:
    params = {"limit": 200}
    if cursor:
        params["cursor"] = cursor
    time.sleep(0.3)
    data = auth_get("/portfolio/fills", params=params)
    fills = data.get("fills", [])
    all_fills.extend(fills)
    cursor = data.get("cursor")
    if not cursor or not fills:
        break

print(f"Total fills: {len(all_fills)}")

# Group by ticker
by_ticker = {}
for fill in all_fills:
    ticker = fill.get("ticker", "")
    matched = None
    for prefix, name in CRYPTO_SERIES.items():
        if ticker.startswith(prefix):
            matched = name
            break
    if not matched:
        continue
    if ticker not in by_ticker:
        by_ticker[ticker] = {"crypto": matched, "side": fill["side"], "count": 0, "cost": 0}
    entry = by_ticker[ticker]
    count = int(float(fill.get("count_fp", fill.get("count", 0))))
    if entry["side"] == "yes":
        price = float(fill.get("yes_price_dollars", 0))
    else:
        price = float(fill.get("no_price_dollars", 0))
    entry["count"] += count
    entry["cost"] += count * price
    entry["avg_price"] = entry["cost"] / entry["count"] if entry["count"] else 0

# Check outcomes
print("Checking market outcomes...")
results = []
for ticker, entry in sorted(by_ticker.items()):
    try:
        time.sleep(0.15)
        mkt = public_get(f"/markets/{ticker}")
        market = mkt.get("market", {})
        status = market.get("status", "")
        result_val = market.get("result", "")
        avg_p = entry["avg_price"]
        if status in ("settled", "finalized") and result_val:
            won = (result_val == "yes" and entry["side"] == "yes") or \
                  (result_val == "no" and entry["side"] == "no")
            if won:
                pnl = round(entry["count"] * (1.0 - avg_p), 4)
                result = "WIN"
            else:
                pnl = round(-entry["count"] * avg_p, 4)
                result = "LOSS"
        elif status == "open":
            result = "OPEN"
            pnl = 0
        else:
            result = "PENDING"
            pnl = 0
        results.append({"ticker": ticker, "crypto": entry["crypto"], "side": entry["side"],
                        "contracts": entry["count"], "avg_price": round(avg_p, 4),
                        "result": result, "pnl": pnl})
    except Exception as e:
        print(f"  Error checking {ticker}: {e}")

losses = [r for r in results if r["result"] == "LOSS"]
wins = [r for r in results if r["result"] == "WIN"]
opens = [r for r in results if r["result"] == "OPEN"]

print(f"\n{'='*70}")
print(f"LOSSES ({len(losses)})")
print(f"{'='*70}")
losses.sort(key=lambda x: x["pnl"])
for r in losses:
    print(f"  {r['crypto']:5s} {r['ticker']:42s} {r['side']:3s} @ {r['avg_price']:.2f}  x{r['contracts']}  P&L: ${r['pnl']:+.4f}")

print(f"\n{'='*70}")
print(f"WINS ({len(wins)})")
print(f"{'='*70}")
for r in sorted(wins, key=lambda x: x["avg_price"]):
    print(f"  {r['crypto']:5s} {r['ticker']:42s} {r['side']:3s} @ {r['avg_price']:.2f}  x{r['contracts']}  P&L: ${r['pnl']:+.4f}")

if opens:
    print(f"\n{'='*70}")
    print(f"OPEN ({len(opens)})")
    print(f"{'='*70}")
    for r in opens:
        print(f"  {r['crypto']:5s} {r['ticker']:42s} {r['side']:3s} @ {r['avg_price']:.2f}  x{r['contracts']}")

print(f"\n{'='*70}")
print(f"SUMMARY")
print(f"{'='*70}")
total_pnl = sum(r["pnl"] for r in results if r["result"] in ("WIN", "LOSS"))
resolved = len(wins) + len(losses)
print(f"Wins: {len(wins)}, Losses: {len(losses)}, Open: {len(opens)}")
if resolved:
    print(f"Win rate: {len(wins)/resolved*100:.1f}%")
if wins:
    print(f"Avg win:  ${sum(r['pnl'] for r in wins)/len(wins):+.4f}")
if losses:
    print(f"Avg loss: ${sum(r['pnl'] for r in losses)/len(losses):+.4f}")
print(f"Total P&L: ${total_pnl:+.4f}")

print(f"\n{'='*70}")
print(f"P&L BY ENTRY PRICE BUCKET")
print(f"{'='*70}")
buckets = {}
for r in results:
    if r["result"] in ("WIN", "LOSS"):
        bucket = int(r["avg_price"] * 10) * 10
        label = f"{bucket}-{bucket+9}c"
        if label not in buckets:
            buckets[label] = {"wins": 0, "losses": 0, "pnl": 0, "loss_pnl": 0, "win_pnl": 0}
        buckets[label]["pnl"] += r["pnl"]
        if r["result"] == "WIN":
            buckets[label]["wins"] += 1
            buckets[label]["win_pnl"] += r["pnl"]
        else:
            buckets[label]["losses"] += 1
            buckets[label]["loss_pnl"] += r["pnl"]

for label in sorted(buckets.keys()):
    b = buckets[label]
    total = b["wins"] + b["losses"]
    wr = b["wins"]/total*100 if total else 0
    print(f"  {label}: {b['wins']}W/{b['losses']}L ({wr:.0f}% WR)  WinP&L: ${b['win_pnl']:+.2f}  LossP&L: ${b['loss_pnl']:+.2f}  Net: ${b['pnl']:+.2f}")

print(f"\n{'='*70}")
print(f"P&L BY CRYPTO")
print(f"{'='*70}")
by_crypto = {}
for r in results:
    if r["result"] in ("WIN", "LOSS"):
        c = r["crypto"]
        if c not in by_crypto:
            by_crypto[c] = {"wins": 0, "losses": 0, "pnl": 0}
        by_crypto[c]["pnl"] += r["pnl"]
        if r["result"] == "WIN":
            by_crypto[c]["wins"] += 1
        else:
            by_crypto[c]["losses"] += 1

for c in sorted(by_crypto.keys()):
    b = by_crypto[c]
    total = b["wins"] + b["losses"]
    wr = b["wins"]/total*100 if total else 0
    print(f"  {c:5s}: {b['wins']}W/{b['losses']}L ({wr:.0f}% WR)  P&L: ${b['pnl']:+.4f}")
