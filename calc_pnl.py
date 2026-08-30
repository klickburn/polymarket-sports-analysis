"""
Calculate accurate P&L from Kalshi API directly.
Uses /portfolio/fills + /markets/{ticker} for settlement outcomes.
Includes Kalshi fee: 0.07 * P * (1 - P) per contract.
"""
import os
import sys
import json
import time
import base64
import requests
from datetime import datetime, timezone
from collections import defaultdict

API_BASE = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_KEY_ID = os.environ.get("KALSHI_KEY_ID", "")
KALSHI_PRIVATE_KEY = os.environ.get("KALSHI_PRIVATE_KEY", "")

if not KALSHI_KEY_ID or not KALSHI_PRIVATE_KEY:
    print("ERROR: KALSHI_KEY_ID and KALSHI_PRIVATE_KEY env vars required")
    sys.exit(1)

session = requests.Session()
_private_key = None

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
    timestamp_ms = str(int(time.time() * 1000))
    sig = sign_request("GET", full_path, timestamp_ms)
    headers = {"KALSHI-ACCESS-KEY": KALSHI_KEY_ID, "KALSHI-ACCESS-SIGNATURE": sig, "KALSHI-ACCESS-TIMESTAMP": timestamp_ms, "Content-Type": "application/json"}
    r = session.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def public_get(path, params=None):
    url = f"{API_BASE}{path}"
    r = session.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def kalshi_fee(price):
    return 0.07 * price * (1 - price)

CRYPTO_SERIES = {
    "KXBTC15M": "BTC", "KXETH15M": "ETH", "KXSOL15M": "SOL",
    "KXXRP15M": "XRP", "KXDOGE15M": "DOGE", "KXHYPE15M": "HYPE", "KXBNB15M": "BNB",
}

def get_crypto_name(ticker):
    for prefix, name in CRYPTO_SERIES.items():
        if ticker.startswith(prefix):
            return name
    return None

def is_sports(ticker):
    for p in ["KXNBA", "KXMLB", "KXNHL", "KXNCAA", "KXEPL", "KXUCL", "KXCS2", "KXLOL", "KXEUR", "KXDOTA"]:
        if ticker.startswith(p):
            return True
    return False

# ── Fetch all fills ─────────────────────────────────────────────────────
print("Fetching all fills from Kalshi API...")
all_fills = []
cursor = None
while True:
    params = {"limit": 200}
    if cursor:
        params["cursor"] = cursor
    data = auth_get("/portfolio/fills", params=params)
    fills = data.get("fills", [])
    all_fills.extend(fills)
    cursor = data.get("cursor")
    if not cursor or not fills:
        break
    time.sleep(0.15)

print(f"Fetched {len(all_fills)} total fills\n")

# ── Group fills by ticker ───────────────────────────────────────────────
fills_by_ticker = defaultdict(list)
for fill in all_fills:
    fills_by_ticker[fill["ticker"]].append(fill)

# Separate crypto vs sports tickers
crypto_tickers = {}
sports_tickers = {}
other_tickers = {}

for ticker, fills in fills_by_ticker.items():
    crypto_name = get_crypto_name(ticker)
    if crypto_name:
        crypto_tickers[ticker] = {"fills": fills, "crypto": crypto_name}
    elif is_sports(ticker):
        sports_tickers[ticker] = {"fills": fills}
    else:
        other_tickers[ticker] = {"fills": fills}

print(f"Crypto tickers: {len(crypto_tickers)} ({sum(len(v['fills']) for v in crypto_tickers.values())} fills)")
print(f"Sports tickers: {len(sports_tickers)} ({sum(len(v['fills']) for v in sports_tickers.values())} fills)")
print(f"Other tickers:  {len(other_tickers)} ({sum(len(v['fills']) for v in other_tickers.values())} fills)")

# ── Check market outcomes (batch) ───────────────────────────────────────
def compute_pnl_for_tickers(ticker_dict, label, show_detail=True):
    """For each ticker, compute P&L from fills + market outcome."""
    total_pnl_no_fee = 0
    total_pnl_with_fee = 0
    total_fees = 0
    total_wagered = 0
    wins = 0
    losses = 0
    open_count = 0
    pending_count = 0
    total_contracts = 0

    results = []

    for ticker, info in sorted(ticker_dict.items()):
        fills = info["fills"]

        # Aggregate fills for this ticker
        side = fills[0].get("side", "")
        total_count = 0
        total_cost = 0

        for fill in fills:
            count = int(float(fill.get("count_fp", 0)))
            if side == "yes":
                price = float(fill.get("yes_price_dollars", 0))
            else:
                price = float(fill.get("no_price_dollars", 0))
            total_count += count
            total_cost += count * price

        avg_price = total_cost / total_count if total_count else 0
        total_contracts += total_count

        # Entry fee (per contract)
        entry_fee = sum(kalshi_fee(
            float(f.get("yes_price_dollars" if side == "yes" else "no_price_dollars", 0))
        ) * int(float(f.get("count_fp", 0))) for f in fills)

        # Check market outcome
        try:
            time.sleep(0.05)
            mkt = public_get(f"/markets/{ticker}")
            market = mkt.get("market", {})
            status = market.get("status", "")
            result_val = market.get("result", "")

            if status in ("settled", "finalized") and result_val:
                won = (result_val == "yes" and side == "yes") or \
                      (result_val == "no" and side == "no")

                # Settlement at $1 or $0 has zero fee
                settlement_fee = 0

                if won:
                    pnl_no_fee = total_count * (1.0 - avg_price)
                    pnl_with_fee = pnl_no_fee - entry_fee - settlement_fee
                    wins += 1
                    result = "WIN"
                else:
                    pnl_no_fee = -(total_count * avg_price)
                    pnl_with_fee = pnl_no_fee - entry_fee - settlement_fee
                    losses += 1
                    result = "LOSS"

                total_pnl_no_fee += pnl_no_fee
                total_pnl_with_fee += pnl_with_fee
                total_fees += entry_fee + settlement_fee
                total_wagered += total_cost

                results.append({
                    "ticker": ticker, "side": side, "contracts": total_count,
                    "avg_price": avg_price, "cost": total_cost,
                    "result": result, "pnl_no_fee": pnl_no_fee,
                    "fee": entry_fee, "pnl_with_fee": pnl_with_fee,
                    "crypto": info.get("crypto", ""),
                })

            elif status == "open":
                open_count += 1
                total_wagered += total_cost
            else:
                pending_count += 1
        except Exception as e:
            pending_count += 1

    # Print results
    print(f"\n{'=' * 70}")
    print(f"{label} P&L REPORT")
    print(f"{'=' * 70}")

    if show_detail and results:
        print(f"\n{'Ticker':<42} {'Side':<4} {'#':<4} {'AvgP':<6} {'Result':<5} {'PnL':<9} {'Fee':<7} {'Net PnL'}")
        print("-" * 90)
        for r in results:
            t = r["ticker"][:41]
            crypto_label = f"[{r['crypto']}] " if r.get("crypto") else ""
            print(f"{crypto_label}{t:<42} {r['side']:<4} {r['contracts']:<4} {r['avg_price']:<6.3f} {r['result']:<5} ${r['pnl_no_fee']:<+8.2f} ${r['fee']:<6.4f} ${r['pnl_with_fee']:<+.2f}")
        print("-" * 90)

    resolved = wins + losses
    print(f"\n{label} Summary:")
    print(f"  Total tickers: {len(ticker_dict)}")
    print(f"  Resolved:      {resolved} ({wins}W / {losses}L)")
    print(f"  Open:          {open_count}")
    print(f"  Pending:       {pending_count}")
    if resolved > 0:
        print(f"  Win Rate:      {wins/resolved*100:.1f}%")
    print(f"  Total Contracts: {total_contracts}")
    print(f"  Total Wagered:   ${total_wagered:.2f}")
    print(f"  P&L (no fees):   ${total_pnl_no_fee:+.2f}")
    print(f"  Total Fees:       ${total_fees:.4f}")
    print(f"  P&L (with fees): ${total_pnl_with_fee:+.2f}")

    return total_pnl_no_fee, total_pnl_with_fee, total_fees

# ── Run for each category ──────────────────────────────────────────────
print("\nChecking market outcomes (this may take a minute)...")

crypto_no, crypto_with, crypto_fees = compute_pnl_for_tickers(crypto_tickers, "CRYPTO", show_detail=True)
sports_no, sports_with, sports_fees = compute_pnl_for_tickers(sports_tickers, "SPORTS", show_detail=True)

if other_tickers:
    other_no, other_with, other_fees = compute_pnl_for_tickers(other_tickers, "OTHER", show_detail=True)
else:
    other_no, other_with, other_fees = 0, 0, 0

# ── Account totals ─────────────────────────────────────────────────────
bal_data = auth_get("/portfolio/balance")
balance = bal_data.get("balance", 0) / 100
portfolio_value = bal_data.get("portfolio_value", 0) / 100

print(f"\n{'=' * 70}")
print(f"ACCOUNT TOTAL")
print(f"{'=' * 70}")
print(f"  Balance:            ${balance:.2f}")
print(f"  Portfolio Value:    ${portfolio_value:.2f}")
print(f"  Total (bal+port):   ${balance + portfolio_value:.2f}")
print()
print(f"  {'Category':<12} {'No Fees':<12} {'Fees':<10} {'With Fees'}")
print(f"  {'-'*44}")
print(f"  {'Crypto':<12} ${crypto_no:<+11.2f} ${crypto_fees:<9.4f} ${crypto_with:<+.2f}")
print(f"  {'Sports':<12} ${sports_no:<+11.2f} ${sports_fees:<9.4f} ${sports_with:<+.2f}")
if other_no:
    print(f"  {'Other':<12} ${other_no:<+11.2f} ${other_fees:<9.4f} ${other_with:<+.2f}")
print(f"  {'-'*44}")
total_no = crypto_no + sports_no + other_no
total_with = crypto_with + sports_with + other_with
total_fees = crypto_fees + sports_fees + other_fees
print(f"  {'TOTAL':<12} ${total_no:<+11.2f} ${total_fees:<9.4f} ${total_with:<+.2f}")
