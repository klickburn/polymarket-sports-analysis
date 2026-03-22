"""
Polymarket P&L Tracker
======================
Correctly calculates per-bet and total P&L from Polymarket US activity data.

Key insight: afterPosition.realized.value in resolution events contains the
TOTAL lifetime P&L for that market (trading round-trips + resolution payout).

Usage:
    python3 pnl_tracker.py              # Show P&L summary from saved activities
    python3 pnl_tracker.py --refresh    # Fetch fresh activities from API first
"""

import os
import sys
import json
import time
import base64
import requests
from datetime import datetime, timezone

# ── Config ──────────────────────────────────────────────────────────────
API_BASE = "https://api.polymarket.us"
API_KEY = os.environ.get("PM_API_KEY", "")
API_SECRET = os.environ.get("PM_API_SECRET", "")

ACTIVITIES_FILE = "my_activities.json"
PNL_FILE = "pnl_report.json"

session = requests.Session()


def P(msg=""):
    print(msg, flush=True)


# ── Auth (reused from trading_bot) ──────────────────────────────────────
def sign_request(method, path, timestamp):
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    message = f"{timestamp}{method.upper()}{path}"
    secret_bytes = base64.b64decode(API_SECRET)
    private_key = Ed25519PrivateKey.from_private_bytes(secret_bytes[:32])
    signature = private_key.sign(message.encode())
    return base64.b64encode(signature).decode()


def auth_headers(method, path):
    timestamp = str(int(time.time() * 1000))
    signature = sign_request(method, path, timestamp)
    return {
        "X-PM-Access-Key": API_KEY,
        "X-PM-Timestamp": timestamp,
        "X-PM-Signature": signature,
        "Content-Type": "application/json",
    }


def auth_get(path, params=None):
    url = f"{API_BASE}{path}"
    headers = auth_headers("GET", path)
    r = session.get(url, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


# ── Fetch Activities ────────────────────────────────────────────────────
def fetch_all_activities():
    """Fetch all activities from Polymarket US API, paginating through all."""
    all_activities = []
    cursor = None

    while True:
        params = {"limit": 100}
        if cursor:
            params["cursor"] = cursor

        data = auth_get("/v1/portfolio/activities", params=params)
        activities = data.get("activities", [])
        all_activities.extend(activities)

        cursor = data.get("nextCursor")
        if not cursor or not activities:
            break
        time.sleep(0.1)

    with open(ACTIVITIES_FILE, "w") as f:
        json.dump(all_activities, f, indent=2)

    P(f"  Fetched {len(all_activities)} activities → {ACTIVITIES_FILE}")
    return all_activities


# ── Positions ──────────────────────────────────────────────────────────
def fetch_open_positions():
    """Fetch actually open positions from the API (net position != 0)."""
    try:
        data = auth_get("/v1/portfolio/positions", params={"limit": 200})
        positions = data.get("positions", {})
        open_slugs = set()
        for slug, p in positions.items():
            net = float(p.get("netPosition", 0))
            if net != 0:
                open_slugs.add(slug)
        return open_slugs
    except Exception as e:
        P(f"  WARNING: Could not fetch positions: {e}")
        return None  # None = couldn't check, fall back to old behavior


# ── P&L Calculation ────────────────────────────────────────────────────
def calculate_pnl(activities):
    """Calculate per-market and total P&L from activity data."""

    trades = [a for a in activities if a["type"] == "ACTIVITY_TYPE_TRADE"]
    resolutions = [a for a in activities if a["type"] == "ACTIVITY_TYPE_POSITION_RESOLUTION"]
    deposits = [a for a in activities
                if "DEPOSIT" in a["type"]
                and a.get("accountBalanceChange", {}).get("status") == "ACCOUNT_BALANCE_CHANGE_STATUS_COMPLETED"]

    total_deposited = sum(float(d["accountBalanceChange"]["amount"]["value"]) for d in deposits)

    # Build trade summary per market
    trade_summary = {}
    for t_act in trades:
        t = t_act["trade"]
        slug = t["marketSlug"]
        cost = float(t["cost"]["value"])
        qty = float(t["qty"])
        price = float(t["price"]["value"])
        create_time = t.get("createTime", "")

        order = t.get("aggressor") or t.get("passive") or {}
        intent = order.get("intent", "UNKNOWN")
        outcome = order.get("marketMetadata", {}).get("outcome", "Unknown")
        title = order.get("marketMetadata", {}).get("title", slug)

        if slug not in trade_summary:
            trade_summary[slug] = {
                "title": title,
                "outcome_bet": outcome,
                "trades": 0,
                "total_cost": 0,
                "total_shares": 0,
                "intents": set(),
                "first_trade": create_time,
                "last_trade": create_time,
            }

        trade_summary[slug]["trades"] += 1
        trade_summary[slug]["total_cost"] += cost
        trade_summary[slug]["total_shares"] += qty
        trade_summary[slug]["intents"].add(intent)
        if create_time > trade_summary[slug]["last_trade"]:
            trade_summary[slug]["last_trade"] = create_time

    # Process resolutions — afterPosition.realized.value = total lifetime P&L
    resolved_markets = []
    resolved_slugs = set()
    total_resolved_pnl = 0

    for res_act in resolutions:
        r = res_act["positionResolution"]
        bp = r["beforePosition"]
        ap = r["afterPosition"]
        slug = r["marketSlug"]
        resolved_slugs.add(slug)

        net_position = float(bp["netPosition"])
        cost_basis = float(bp["cost"]["value"])
        total_pnl = float(ap["realized"]["value"])  # Total lifetime P&L for this market
        pre_resolution_pnl = float(bp["realized"]["value"])  # P&L from trading before resolution
        resolution_pnl = total_pnl - pre_resolution_pnl  # P&L from the resolution event itself

        outcome = bp.get("marketMetadata", {}).get("outcome", "Unknown")
        title = bp.get("marketMetadata", {}).get("title", slug)
        event_slug = bp.get("marketMetadata", {}).get("eventSlug", "")

        # Determine win/loss from resolution P&L
        won = resolution_pnl > 0

        ts = trade_summary.get(slug, {})

        resolved_markets.append({
            "market_slug": slug,
            "title": title,
            "outcome_bet": outcome,
            "net_position": net_position,
            "cost_basis": cost_basis,
            "total_pnl": round(total_pnl, 2),
            "trading_pnl": round(pre_resolution_pnl, 2),
            "resolution_pnl": round(resolution_pnl, 2),
            "won": won,
            "num_trades": ts.get("trades", 0),
            "total_traded": round(ts.get("total_cost", 0), 2),
            "first_trade": ts.get("first_trade", ""),
        })

        total_resolved_pnl += total_pnl

    # Open positions (not yet resolved)
    # Cross-reference with API to exclude positions user has manually closed
    actually_open = fetch_open_positions() if API_KEY and API_SECRET else None

    open_markets = []
    total_open_cost = 0
    for slug, ts in trade_summary.items():
        if slug not in resolved_slugs:
            # If we got API data, only include positions that are actually open
            if actually_open is not None and slug not in actually_open:
                continue
            open_markets.append({
                "market_slug": slug,
                "title": ts["title"],
                "outcome_bet": ts["outcome_bet"],
                "trades": ts["trades"],
                "total_cost": round(ts["total_cost"], 2),
                "total_shares": round(ts["total_shares"], 2),
                "intents": list(ts["intents"]),
            })
            total_open_cost += ts["total_cost"]

    # Sort resolved by P&L
    resolved_markets.sort(key=lambda x: x["total_pnl"], reverse=True)

    wins = sum(1 for m in resolved_markets if m["won"])
    losses = len(resolved_markets) - wins

    return {
        "total_deposited": total_deposited,
        "total_resolved_pnl": round(total_resolved_pnl, 2),
        "resolved_count": len(resolved_markets),
        "wins": wins,
        "losses": losses,
        "win_rate": round(wins / len(resolved_markets) * 100, 1) if resolved_markets else 0,
        "open_count": len(open_markets),
        "open_cost": round(total_open_cost, 2),
        "estimated_balance": round(total_deposited + total_resolved_pnl, 2),
        "resolved_markets": resolved_markets,
        "open_markets": open_markets,
        "total_trades": len(trades),
    }


# ── Display ─────────────────────────────────────────────────────────────
def display_report(report):
    P("=" * 70)
    P("  POLYMARKET P&L REPORT")
    P("=" * 70)
    P()

    P(f"  Total Deposited:     ${report['total_deposited']:>10,.2f}")
    P(f"  Resolved P&L:        ${report['total_resolved_pnl']:>+10,.2f}")
    P(f"  Estimated Balance:   ${report['estimated_balance']:>10,.2f}  (excl. open position market value)")
    P()

    P(f"  Resolved Markets:    {report['resolved_count']}")
    P(f"  Record:              {report['wins']}W - {report['losses']}L ({report['win_rate']:.1f}%)")
    P(f"  Open Positions:      {report['open_count']} (cost: ${report['open_cost']:,.2f})")
    P(f"  Total Trades:        {report['total_trades']}")
    P()

    # Top winners
    P("  TOP WINNERS:")
    P("  " + "-" * 65)
    for m in report["resolved_markets"][:10]:
        if m["total_pnl"] <= 0:
            break
        P(f"    ${m['total_pnl']:>+7.2f}  {m['title'][:40]:<42} ({m['outcome_bet'][:12]})")

    P()

    # Top losers
    P("  TOP LOSERS:")
    P("  " + "-" * 65)
    for m in reversed(report["resolved_markets"][-10:]):
        if m["total_pnl"] >= 0:
            break
        P(f"    ${m['total_pnl']:>+7.2f}  {m['title'][:40]:<42} ({m['outcome_bet'][:12]})")

    P()

    # Per-market detail
    P("  ALL RESOLVED MARKETS:")
    P("  " + "-" * 90)
    P(f"  {'Market':<40} {'Bet On':<14} {'Pos':>5} {'Trades':>6} {'P&L':>9} {'Result':>6}")
    P("  " + "-" * 90)

    for m in sorted(report["resolved_markets"], key=lambda x: x["first_trade"]):
        result = "WIN" if m["won"] else "LOSS"
        P(f"  {m['title'][:40]:<40} {m['outcome_bet'][:14]:<14} {m['net_position']:>5.0f} {m['num_trades']:>6} ${m['total_pnl']:>+8.2f} {result:>6}")

    P()

    # Open positions
    if report["open_markets"]:
        P("  OPEN POSITIONS:")
        P("  " + "-" * 70)
        for m in report["open_markets"]:
            P(f"    {m['title'][:40]:<42} {m['outcome_bet'][:14]:<14} cost=${m['total_cost']:>6.2f}")

    P()

    # Save report
    with open(PNL_FILE, "w") as f:
        json.dump(report, f, indent=2, default=str)
    P(f"  Full report saved to {PNL_FILE}")


# ── Entry Point ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    refresh = "--refresh" in sys.argv

    if refresh:
        if not API_KEY or not API_SECRET:
            P("Set PM_API_KEY and PM_API_SECRET to refresh activities")
            sys.exit(1)
        P("  Fetching fresh activities from API...")
        activities = fetch_all_activities()
    else:
        if not os.path.exists(ACTIVITIES_FILE):
            P(f"  No {ACTIVITIES_FILE} found. Run with --refresh to fetch from API.")
            sys.exit(1)
        with open(ACTIVITIES_FILE) as f:
            activities = json.load(f)
        P(f"  Loaded {len(activities)} activities from {ACTIVITIES_FILE}")

    report = calculate_pnl(activities)
    display_report(report)
