"""
S&P 500 Options Strategy Backtester
====================================
Uses free Yahoo Finance data (SPY + VIX) with Black-Scholes pricing
to simulate and compare multiple options strategies.

Strategies tested:
  1. Weekly Cash-Secured Put (sell ATM-5% puts)
  2. Weekly Covered Call / BuyWrite (buy SPY + sell ATM+2% calls)
  3. Weekly Iron Condor (sell call+put spreads 3% OTM, $5 wide)
  4. VIX-Adaptive Put Selling (adjust delta based on VIX level)
  5. Mean Reversion Straddle (buy straddle after big down days)
  6. Weekly Strangle Selling (sell 5% OTM puts + 3% OTM calls)
"""

import math
import json
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
from collections import defaultdict

# ── Black-Scholes ─────────────────────────────────────────────────────

def norm_cdf(x):
    """Standard normal CDF approximation (Abramowitz & Stegun)."""
    a1, a2, a3, a4, a5 = 0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429
    p = 0.3275911
    sign = 1 if x >= 0 else -1
    x = abs(x)
    t = 1.0 / (1.0 + p * x)
    y = 1.0 - (((((a5*t + a4)*t) + a3)*t + a2)*t + a1)*t * math.exp(-x*x/2.0)
    return 0.5 * (1.0 + sign * y)

def bs_price(S, K, T, r, sigma, option_type="put"):
    """Black-Scholes option price. T in years."""
    if T <= 0 or sigma <= 0:
        if option_type == "call":
            return max(S - K, 0)
        return max(K - S, 0)
    d1 = (math.log(S/K) + (r + sigma**2/2)*T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if option_type == "call":
        return S * norm_cdf(d1) - K * math.exp(-r*T) * norm_cdf(d2)
    return K * math.exp(-r*T) * norm_cdf(-d2) - S * norm_cdf(-d1)

def bs_delta(S, K, T, r, sigma, option_type="put"):
    """Black-Scholes delta."""
    if T <= 0 or sigma <= 0:
        if option_type == "call":
            return 1.0 if S > K else 0.0
        return -1.0 if S < K else 0.0
    d1 = (math.log(S/K) + (r + sigma**2/2)*T) / (sigma * math.sqrt(T))
    if option_type == "call":
        return norm_cdf(d1)
    return norm_cdf(d1) - 1

# ── Data Fetching ─────────────────────────────────────────────────────

def fetch_yahoo(symbol, period1, period2):
    """Fetch daily OHLCV from Yahoo Finance v8 API."""
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {
        "period1": str(int(period1.timestamp())),
        "period2": str(int(period2.timestamp())),
        "interval": "1d",
    }
    url += "?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode())
    result = data["chart"]["result"][0]
    timestamps = result["timestamp"]
    quote = result["indicators"]["quote"][0]
    rows = []
    for i in range(len(timestamps)):
        if quote["close"][i] is None:
            continue
        rows.append({
            "date": datetime.utcfromtimestamp(timestamps[i]).strftime("%Y-%m-%d"),
            "open": quote["open"][i],
            "high": quote["high"][i],
            "low": quote["low"][i],
            "close": quote["close"][i],
            "volume": quote["volume"][i],
        })
    return rows

def load_data(years=3):
    """Load SPY and VIX data."""
    end = datetime.utcnow()
    start = end - timedelta(days=years*365)
    print(f"Fetching SPY data ({start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')})...")
    spy = fetch_yahoo("SPY", start, end)
    print(f"  Got {len(spy)} SPY days")
    print("Fetching VIX data...")
    vix = fetch_yahoo("^VIX", start, end)
    print(f"  Got {len(vix)} VIX days")
    # Align by date
    vix_map = {v["date"]: v["close"] for v in vix}
    for row in spy:
        row["vix"] = vix_map.get(row["date"], 20.0)
        row["iv"] = row["vix"] / 100.0  # VIX as annualized IV proxy
    return spy

# ── Strategy Helpers ──────────────────────────────────────────────────

def find_fridays(data):
    """Group data by trading weeks (Mon-Fri), return (entry_idx, exit_idx) pairs."""
    weeks = []
    i = 0
    while i < len(data) - 1:
        entry = i
        entry_date = datetime.strptime(data[i]["date"], "%Y-%m-%d")
        entry_weekday = entry_date.weekday()
        # Find end of this week (Friday or last trading day of week)
        j = i + 1
        while j < len(data):
            next_date = datetime.strptime(data[j]["date"], "%Y-%m-%d")
            # If we've crossed into a new week
            if next_date.weekday() <= entry_weekday and (next_date - entry_date).days > 1:
                break
            entry_weekday = next_date.weekday()
            entry_date = next_date
            j += 1
        exit_idx = j - 1
        if exit_idx > entry:
            weeks.append((entry, exit_idx))
        i = j
    return weeks

# ── Strategies ────────────────────────────────────────────────────────

def strategy_cash_secured_put(data, otm_pct=0.05):
    """Sell weekly cash-secured puts, OTM by otm_pct."""
    weeks = find_fridays(data)
    trades = []
    for entry_i, exit_i in weeks:
        S_entry = data[entry_i]["close"]
        S_exit = data[exit_i]["close"]
        iv = data[entry_i]["iv"]
        K = S_entry * (1 - otm_pct)  # 5% OTM put
        T = (exit_i - entry_i) / 252.0
        premium = bs_price(S_entry, K, T, 0.05, iv, "put")
        # At expiry
        intrinsic = max(K - S_exit, 0)
        pnl = premium - intrinsic  # Sold put: keep premium, pay intrinsic
        capital = K  # Cash secured
        trades.append({
            "entry_date": data[entry_i]["date"],
            "exit_date": data[exit_i]["date"],
            "spy_entry": S_entry,
            "spy_exit": S_exit,
            "strike": K,
            "premium": premium,
            "pnl": pnl,
            "return_pct": pnl / capital * 100,
            "vix": data[entry_i]["vix"],
        })
    return trades

def strategy_covered_call(data, otm_pct=0.02):
    """Buy SPY + sell weekly OTM call."""
    weeks = find_fridays(data)
    trades = []
    for entry_i, exit_i in weeks:
        S_entry = data[entry_i]["close"]
        S_exit = data[exit_i]["close"]
        iv = data[entry_i]["iv"]
        K = S_entry * (1 + otm_pct)  # 2% OTM call
        T = (exit_i - entry_i) / 252.0
        premium = bs_price(S_entry, K, T, 0.05, iv, "call")
        # Stock P&L + short call P&L
        stock_pnl = S_exit - S_entry
        call_intrinsic = max(S_exit - K, 0)
        pnl = stock_pnl + premium - call_intrinsic
        trades.append({
            "entry_date": data[entry_i]["date"],
            "exit_date": data[exit_i]["date"],
            "spy_entry": S_entry,
            "spy_exit": S_exit,
            "strike": K,
            "premium": premium,
            "pnl": pnl,
            "return_pct": pnl / S_entry * 100,
            "vix": data[entry_i]["vix"],
        })
    return trades

def strategy_iron_condor(data, put_otm=0.03, call_otm=0.03, width=5):
    """Weekly iron condor: sell put+call spreads OTM, $5 wide."""
    weeks = find_fridays(data)
    trades = []
    for entry_i, exit_i in weeks:
        S = data[entry_i]["close"]
        S_exit = data[exit_i]["close"]
        iv = data[entry_i]["iv"]
        T = (exit_i - entry_i) / 252.0
        # Short put spread
        K_put_short = S * (1 - put_otm)
        K_put_long = K_put_short - width
        put_short_prem = bs_price(S, K_put_short, T, 0.05, iv, "put")
        put_long_prem = bs_price(S, K_put_long, T, 0.05, iv, "put")
        put_credit = put_short_prem - put_long_prem
        # Short call spread
        K_call_short = S * (1 + call_otm)
        K_call_long = K_call_short + width
        call_short_prem = bs_price(S, K_call_short, T, 0.05, iv, "call")
        call_long_prem = bs_price(S, K_call_long, T, 0.05, iv, "call")
        call_credit = call_short_prem - call_long_prem
        total_credit = put_credit + call_credit
        # At expiry
        put_short_val = max(K_put_short - S_exit, 0)
        put_long_val = max(K_put_long - S_exit, 0)
        call_short_val = max(S_exit - K_call_short, 0)
        call_long_val = max(S_exit - K_call_long, 0)
        put_spread_loss = put_short_val - put_long_val
        call_spread_loss = call_short_val - call_long_val
        pnl = total_credit - put_spread_loss - call_spread_loss
        max_risk = width - total_credit
        trades.append({
            "entry_date": data[entry_i]["date"],
            "exit_date": data[exit_i]["date"],
            "spy_entry": S,
            "spy_exit": S_exit,
            "credit": total_credit,
            "pnl": pnl,
            "return_pct": pnl / max(max_risk, 0.01) * 100,
            "vix": data[entry_i]["vix"],
        })
    return trades

def strategy_vix_adaptive_put(data):
    """Sell puts with strike adjusted by VIX level.
    Low VIX (<15): sell 2% OTM (aggressive)
    Med VIX (15-25): sell 5% OTM (normal)
    High VIX (>25): sell 8% OTM (conservative) but bigger premium
    """
    weeks = find_fridays(data)
    trades = []
    for entry_i, exit_i in weeks:
        S = data[entry_i]["close"]
        S_exit = data[exit_i]["close"]
        vix = data[entry_i]["vix"]
        iv = data[entry_i]["iv"]
        T = (exit_i - entry_i) / 252.0
        if vix < 15:
            otm = 0.02
        elif vix < 25:
            otm = 0.05
        else:
            otm = 0.08
        K = S * (1 - otm)
        premium = bs_price(S, K, T, 0.05, iv, "put")
        intrinsic = max(K - S_exit, 0)
        pnl = premium - intrinsic
        trades.append({
            "entry_date": data[entry_i]["date"],
            "exit_date": data[exit_i]["date"],
            "spy_entry": S,
            "spy_exit": S_exit,
            "strike": K,
            "otm_pct": otm,
            "premium": premium,
            "pnl": pnl,
            "return_pct": pnl / K * 100,
            "vix": vix,
        })
    return trades

def strategy_mean_reversion_straddle(data, threshold=-0.015):
    """Buy straddle after SPY drops >1.5% in a day, hold 3 days.
    Idea: big drops cause elevated vol, often followed by bounce or continued move."""
    trades = []
    i = 0
    while i < len(data) - 4:
        daily_ret = (data[i]["close"] - data[i-1]["close"]) / data[i-1]["close"] if i > 0 else 0
        if daily_ret < threshold:
            S = data[i]["close"]
            iv = data[i]["iv"]
            # Buy ATM straddle, hold 3 trading days
            exit_i = min(i + 3, len(data) - 1)
            S_exit = data[exit_i]["close"]
            T_entry = 5 / 252.0  # ~1 week expiry
            T_exit = max((5 - 3) / 252.0, 0.001)
            K = S
            # Entry cost
            call_entry = bs_price(S, K, T_entry, 0.05, iv, "call")
            put_entry = bs_price(S, K, T_entry, 0.05, iv, "put")
            straddle_cost = call_entry + put_entry
            # Exit value (still has ~2 days left, use same IV as approximation)
            iv_exit = data[exit_i]["iv"]
            call_exit = bs_price(S_exit, K, T_exit, 0.05, iv_exit, "call")
            put_exit = bs_price(S_exit, K, T_exit, 0.05, iv_exit, "put")
            straddle_exit = call_exit + put_exit
            pnl = straddle_exit - straddle_cost
            trades.append({
                "entry_date": data[i]["date"],
                "exit_date": data[exit_i]["date"],
                "spy_entry": S,
                "spy_exit": S_exit,
                "move_pct": (S_exit - S) / S * 100,
                "cost": straddle_cost,
                "pnl": pnl,
                "return_pct": pnl / straddle_cost * 100,
                "trigger_drop": daily_ret * 100,
                "vix": data[i]["vix"],
            })
            i = exit_i + 1  # Skip holding period
        else:
            i += 1
    return trades

def strategy_strangle_sell(data, put_otm=0.05, call_otm=0.03):
    """Sell weekly strangle: naked OTM put + naked OTM call."""
    weeks = find_fridays(data)
    trades = []
    for entry_i, exit_i in weeks:
        S = data[entry_i]["close"]
        S_exit = data[exit_i]["close"]
        iv = data[entry_i]["iv"]
        T = (exit_i - entry_i) / 252.0
        K_put = S * (1 - put_otm)
        K_call = S * (1 + call_otm)
        put_prem = bs_price(S, K_put, T, 0.05, iv, "put")
        call_prem = bs_price(S, K_call, T, 0.05, iv, "call")
        total_prem = put_prem + call_prem
        put_loss = max(K_put - S_exit, 0)
        call_loss = max(S_exit - K_call, 0)
        pnl = total_prem - put_loss - call_loss
        # Margin ~= max(put notional, call notional) * 20%
        margin = S * 0.20
        trades.append({
            "entry_date": data[entry_i]["date"],
            "exit_date": data[exit_i]["date"],
            "spy_entry": S,
            "spy_exit": S_exit,
            "premium": total_prem,
            "pnl": pnl,
            "return_pct": pnl / margin * 100,
            "vix": data[entry_i]["vix"],
        })
    return trades

# ── Reporting ─────────────────────────────────────────────────────────

def analyze(name, trades, capital=10000):
    """Compute performance metrics for a strategy."""
    if not trades:
        return {"name": name, "trades": 0}
    pnls = [t["pnl"] for t in trades]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]
    total_pnl = sum(pnls)
    # Per-contract P&L (1 SPY option = 100 shares)
    total_pnl_100 = total_pnl * 100
    cum = 0
    peak = 0
    max_dd = 0
    for p in pnls:
        cum += p * 100
        peak = max(peak, cum)
        dd = peak - cum
        max_dd = max(max_dd, dd)
    # Annualize
    days = (datetime.strptime(trades[-1]["exit_date"], "%Y-%m-%d") -
            datetime.strptime(trades[0]["entry_date"], "%Y-%m-%d")).days
    years = max(days / 365.25, 0.1)
    annual_return = total_pnl_100 / years
    avg_pnl = total_pnl / len(trades)
    std_pnl = (sum((p - avg_pnl)**2 for p in pnls) / len(pnls)) ** 0.5 if len(pnls) > 1 else 0
    sharpe = (avg_pnl / std_pnl * math.sqrt(52)) if std_pnl > 0 else 0  # Weekly trades → annualize
    # Win streaks and loss streaks
    max_consec_loss = 0
    cur_loss = 0
    for p in pnls:
        if p <= 0:
            cur_loss += 1
            max_consec_loss = max(max_consec_loss, cur_loss)
        else:
            cur_loss = 0

    return {
        "name": name,
        "trades": len(trades),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": len(wins) / len(trades) * 100,
        "total_pnl": total_pnl_100,
        "avg_pnl": avg_pnl * 100,
        "best_trade": max(pnls) * 100,
        "worst_trade": min(pnls) * 100,
        "max_drawdown": max_dd,
        "sharpe": sharpe,
        "annual_return": annual_return,
        "max_consec_losses": max_consec_loss,
        "years": years,
        "avg_vix": sum(t["vix"] for t in trades) / len(trades),
    }

def print_report(results):
    """Print comparison table."""
    # Sort by total P&L
    results.sort(key=lambda r: r.get("total_pnl", 0), reverse=True)
    print("\n" + "="*120)
    print(f"{'STRATEGY COMPARISON':^120}")
    print("="*120)
    print(f"{'Strategy':<32} {'Trades':>6} {'Win%':>6} {'Total P&L':>12} {'Avg/Trade':>10} {'Best':>10} {'Worst':>10} {'MaxDD':>10} {'Sharpe':>7} {'Annual':>12}")
    print("-"*120)
    for r in results:
        if r["trades"] == 0:
            print(f"{r['name']:<32} {'N/A':>6}")
            continue
        print(f"{r['name']:<32} {r['trades']:>6} {r['win_rate']:>5.1f}% "
              f"${r['total_pnl']:>+10,.0f} ${r['avg_pnl']:>+8.0f} "
              f"${r['best_trade']:>+8.0f} ${r['worst_trade']:>+8.0f} "
              f"${r['max_drawdown']:>8,.0f} {r['sharpe']:>6.2f} "
              f"${r['annual_return']:>+10,.0f}/yr")
    print("="*120)
    print("Note: P&L is per 1 contract (100 shares). Multiply by # contracts for portfolio sizing.")
    print()

def print_yearly(name, trades):
    """Print year-by-year breakdown."""
    by_year = defaultdict(list)
    for t in trades:
        yr = t["entry_date"][:4]
        by_year[yr].append(t)
    print(f"\n  {name} — Yearly Breakdown:")
    print(f"  {'Year':<6} {'Trades':>6} {'Win%':>6} {'P&L':>10} {'Avg':>8} {'Worst':>8} {'Avg VIX':>8}")
    print(f"  {'-'*56}")
    for yr in sorted(by_year.keys()):
        tt = by_year[yr]
        pnls = [t["pnl"] * 100 for t in tt]
        w = sum(1 for p in pnls if p > 0)
        print(f"  {yr:<6} {len(tt):>6} {w/len(tt)*100:>5.1f}% ${sum(pnls):>+8,.0f} "
              f"${sum(pnls)/len(pnls):>+6.0f} ${min(pnls):>+6.0f} "
              f"{sum(t['vix'] for t in tt)/len(tt):>7.1f}")

# ── Main ──────────────────────────────────────────────────────────────

def main():
    print("S&P 500 Options Strategy Backtester")
    print("="*50)

    data = load_data(years=3)
    if len(data) < 100:
        print("ERROR: Not enough data")
        return

    print(f"\nData range: {data[0]['date']} to {data[-1]['date']}")
    print(f"SPY: ${data[0]['close']:.2f} → ${data[-1]['close']:.2f} "
          f"({(data[-1]['close']/data[0]['close']-1)*100:+.1f}%)")
    print(f"VIX range: {min(d['vix'] for d in data):.1f} - {max(d['vix'] for d in data):.1f}")

    strategies = [
        ("1. Cash-Secured Put (5% OTM)", lambda: strategy_cash_secured_put(data, otm_pct=0.05)),
        ("2. Cash-Secured Put (3% OTM)", lambda: strategy_cash_secured_put(data, otm_pct=0.03)),
        ("3. Covered Call (2% OTM)", lambda: strategy_covered_call(data, otm_pct=0.02)),
        ("4. Covered Call (1% OTM)", lambda: strategy_covered_call(data, otm_pct=0.01)),
        ("5. Iron Condor (3% OTM, $5w)", lambda: strategy_iron_condor(data, 0.03, 0.03, 5)),
        ("6. Iron Condor (2% OTM, $5w)", lambda: strategy_iron_condor(data, 0.02, 0.02, 5)),
        ("7. Iron Condor (3% OTM, $10w)", lambda: strategy_iron_condor(data, 0.03, 0.03, 10)),
        ("8. VIX-Adaptive Put", lambda: strategy_vix_adaptive_put(data)),
        ("9. Mean Rev Straddle (-1.5%)", lambda: strategy_mean_reversion_straddle(data, -0.015)),
        ("10. Mean Rev Straddle (-2.0%)", lambda: strategy_mean_reversion_straddle(data, -0.02)),
        ("11. Strangle Sell (5p/3c)", lambda: strategy_strangle_sell(data, 0.05, 0.03)),
        ("12. Strangle Sell (3p/2c)", lambda: strategy_strangle_sell(data, 0.03, 0.02)),
    ]

    all_results = []
    all_trades = {}
    for name, fn in strategies:
        print(f"\nRunning {name}...")
        trades = fn()
        result = analyze(name, trades)
        all_results.append(result)
        all_trades[name] = trades

    print_report(all_results)

    # Print yearly breakdown for top 3
    top3 = sorted(all_results, key=lambda r: r.get("total_pnl", 0), reverse=True)[:3]
    print("\n" + "="*60)
    print("TOP 3 STRATEGIES — YEARLY BREAKDOWN")
    print("="*60)
    for r in top3:
        if r["trades"] > 0:
            print_yearly(r["name"], all_trades[r["name"]])

    # Print monthly breakdown for #1
    if top3[0]["trades"] > 0:
        best_name = top3[0]["name"]
        best_trades = all_trades[best_name]
        by_month = defaultdict(list)
        for t in best_trades:
            mo = t["entry_date"][:7]
            by_month[mo].append(t)
        print(f"\n  {best_name} — Monthly P&L:")
        print(f"  {'Month':<8} {'Trades':>6} {'P&L':>10}")
        print(f"  {'-'*26}")
        cum = 0
        for mo in sorted(by_month.keys()):
            tt = by_month[mo]
            mo_pnl = sum(t["pnl"] * 100 for t in tt)
            cum += mo_pnl
            bar = "+" * int(max(mo_pnl / 50, 0)) if mo_pnl > 0 else "-" * int(max(-mo_pnl / 50, 0))
            print(f"  {mo:<8} {len(tt):>6} ${mo_pnl:>+8,.0f}  {bar}")

    # Risk analysis
    print(f"\n{'='*60}")
    print("RISK ANALYSIS")
    print(f"{'='*60}")
    for r in all_results:
        if r["trades"] == 0:
            continue
        risk_score = 0
        if r["max_drawdown"] > 5000: risk_score += 2
        elif r["max_drawdown"] > 2000: risk_score += 1
        if r["win_rate"] < 50: risk_score += 2
        elif r["win_rate"] < 65: risk_score += 1
        if r["max_consec_losses"] > 5: risk_score += 1
        if r["sharpe"] < 0.5: risk_score += 1
        risk_label = ["LOW", "LOW-MED", "MEDIUM", "MED-HIGH", "HIGH", "VERY HIGH"][min(risk_score, 5)]
        print(f"  {r['name']:<32} Risk: {risk_label:<10} MaxDD: ${r['max_drawdown']:>8,.0f}  "
              f"MaxConsecLoss: {r['max_consec_losses']}  Sharpe: {r['sharpe']:.2f}")

if __name__ == "__main__":
    main()
