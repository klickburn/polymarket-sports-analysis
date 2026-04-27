"""
Polymarket Trading Bot — Hybrid 5-Tier Strategy (Kalshi-validated)
=======================================================
Scans NBA, CBB, NHL markets.

Strategy (validated against 1500+ Kalshi games):
  T1: Team B fav @ 75-90%             → 1.5% sizing (Polymarket-specific edge, unconfirmed on Kalshi)
  T2: Coin flip (fav 40-60%)          → Bet FAVORITE for NBA, UNDERDOG for CBB/NHL
  T3: CBB Team B underdog (fav 50-55%) → 65.8% WR, +34.9% ROI (CBB) — confirmed on Kalshi
  T4: NBA exact 50/50 (fav <50.6%)    → 61.3% WR, +22.8% ROI (NBA) — confirmed on Kalshi

Sizing: 3% proven, 2% new leagues, 1.5% T1 (reduced confidence)
Uses Polymarket US API (api.polymarket.us / gateway.polymarket.us)

Usage:
    python3 trading_bot.py              # Scan and show qualifying bets
    python3 trading_bot.py --live       # Actually place bets
    python3 trading_bot.py --monitor    # Continuous monitoring loop
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
GATEWAY_BASE = "https://gateway.polymarket.us"

API_KEY = os.environ.get("PM_API_KEY", "")
API_SECRET = os.environ.get("PM_API_SECRET", "")

BANKROLL_PCT = 0.03          # 3% of bankroll per bet (proven leagues)
BANKROLL_PCT_NEW = 0.02      # 2% of bankroll per bet (new/unproven leagues)
BANKROLL_PCT_T1 = 0.015      # 1.5% for T1 (Polymarket-specific, unconfirmed on Kalshi)
MIN_BET = 1.00               # Minimum bet size
MAX_BET = 500.00             # Safety cap per bet

LEAGUES = ["nba", "cbb", "nhl"]
NEW_LEAGUES = {"nhl"}  # Leagues without backtest data — use reduced sizing
BET_WINDOW_HOURS = 2  # Only bet within this many hours before game start (all leagues)
# In T2 coin flips: these leagues bet UNDERDOG (Kalshi-validated), rest bet FAVORITE
T2_UNDERDOG_LEAGUES = {"cbb", "nhl"}
SCAN_INTERVAL = 300           # 5 minutes between scans in monitor mode

TIER_NAMES = {
    1: "T1: B fav 75-90%",
    2: "T2: Coin flip fav",
    6: "T2: Coin flip dog",
    3: "T3: CBB B dog 50-55%",
    4: "T4: NBA 50/50",
}

LOG_FILE = "trading_bot.log"
BETS_FILE = "trading_bot_bets.json"

session = requests.Session()

# ── Logging ─────────────────────────────────────────────────────────────
_log = open(LOG_FILE, "a")

def P(msg=""):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    _log.write(line + "\n")
    _log.flush()


# ── Authentication ──────────────────────────────────────────────────────
def sign_request(method, path, timestamp):
    """ED25519 signature for Polymarket US API."""
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


def auth_post(path, body):
    url = f"{API_BASE}{path}"
    headers = auth_headers("POST", path)
    r = session.post(url, headers=headers, json=body, timeout=30)
    r.raise_for_status()
    return r.json()


def public_get(path, params=None):
    url = f"{GATEWAY_BASE}{path}"
    r = session.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


# ── Account ─────────────────────────────────────────────────────────────
def get_balance():
    try:
        data = auth_get("/v1/account/balances")
        balances = data.get("balances", [])
        if balances:
            bp = float(balances[0].get("buyingPower", 0))
            cb = float(balances[0].get("currentBalance", 0))
            return {"buying_power": bp, "current_balance": cb}
        return {"buying_power": 0, "current_balance": 0}
    except Exception as e:
        P(f"  ERROR getting balance: {e}")
        return None


def get_existing_positions():
    """Fetch all current positions from the API to avoid duplicate bets."""
    try:
        data = auth_get("/v1/positions", params={"limit": 200})
        positions = data.get("positions", [])
        slugs = set()
        for pos in positions:
            net = float(pos.get("netPosition", 0))
            if net != 0:  # Only count non-zero positions
                slug = pos.get("marketSlug", "")
                if slug:
                    slugs.add(slug)
        P(f"  Active positions on account: {len(slugs)}")
        return slugs
    except Exception as e:
        P(f"  WARNING: Could not fetch positions: {e}")
        return set()


def get_open_orders():
    """Get slugs with open/live orders on Polymarket to prevent duplicates."""
    try:
        slugs = set()
        data = auth_get("/v1/orders", params={"status": "live", "limit": 200})
        for order in data.get("orders", []):
            slug = order.get("marketSlug", "")
            if slug:
                slugs.add(slug)
        if slugs:
            P(f"  Open orders on {len(slugs)} markets")
        return slugs
    except Exception as e:
        P(f"  WARNING: Could not fetch open orders: {e}")
        return set()


# ── Market Discovery ────────────────────────────────────────────────────
def get_league_events(league_slug, limit=50):
    """Get active events for a league. Markets are embedded in events."""
    try:
        data = public_get(f"/v2/leagues/{league_slug}/events", params={
            "limit": limit,
            "type": "sport",
        })
        return data.get("events", []) if isinstance(data, dict) else []
    except Exception as e:
        P(f"  ERROR fetching {league_slug} events: {e}")
        return []


def get_market_price(market_id):
    """Get fresh prices from the single-market endpoint (more accurate)."""
    try:
        data = public_get(f"/v1/market/id/{market_id}")
        mkt = data.get("market", data) if isinstance(data, dict) else data
        if not isinstance(mkt, dict):
            return None, None, None, None

        outcomes_raw = mkt.get("outcomes", "[]")
        prices_raw = mkt.get("outcomePrices", "[]")

        # These come as JSON strings from the league endpoint but as lists from market endpoint
        if isinstance(outcomes_raw, str):
            outcomes = json.loads(outcomes_raw)
        else:
            outcomes = outcomes_raw
        if isinstance(prices_raw, str):
            prices = json.loads(prices_raw)
        else:
            prices = prices_raw

        if len(outcomes) != 2 or len(prices) != 2:
            return None, None, None, None

        return outcomes[0], outcomes[1], float(prices[0]), float(prices[1])
    except Exception as e:
        P(f"    ERROR fetching market {market_id} price: {e}")
        return None, None, None, None


def is_draw_market(market):
    """Check if this is an EPL draw market."""
    slug = market.get("slug", "")
    question = market.get("question", "")
    return "-draw" in slug or "- Draw" in question


def parse_market_from_event(market, league=None):
    """Parse a market from the league events endpoint. Only pre-game markets."""
    market_type = market.get("sportsMarketType", market.get("marketType", ""))
    # Allow moneyline for all leagues, plus drawable_outcome for draw markets
    if market_type == "moneyline":
        pass  # Always allowed
    elif market_type == "drawable_outcome" and is_draw_market(market):
        pass  # Draw markets allowed
    else:
        return None
    if not market.get("active", False) or market.get("closed", False):
        return None

    # Skip live/in-progress games AND only bet within 2h of game start
    game_start = market.get("gameStartTime") or market.get("endDate")
    if game_start:
        try:
            start_dt = datetime.fromisoformat(game_start.replace("Z", "+00:00"))
            now = datetime.now(timezone.utc)
            if now >= start_dt:
                return None  # Game already started
            hours_until = (start_dt - now).total_seconds() / 3600
            if hours_until > BET_WINDOW_HOURS:
                return None  # Too far from game start
        except (ValueError, TypeError):
            pass

    outcomes_raw = market.get("outcomes", "[]")
    prices_raw = market.get("outcomePrices", "[]")

    if isinstance(outcomes_raw, str):
        outcomes = json.loads(outcomes_raw)
    else:
        outcomes = outcomes_raw
    if isinstance(prices_raw, str):
        prices = json.loads(prices_raw)
    else:
        prices = prices_raw

    if len(outcomes) != 2 or len(prices) != 2:
        return None

    price_a = float(prices[0])
    price_b = float(prices[1])

    return {
        "market_id": market.get("id"),
        "slug": market.get("slug", ""),
        "question": market.get("question", ""),
        "team_a": outcomes[0],
        "team_b": outcomes[1],
        "price_a": price_a,
        "price_b": price_b,
        "is_draw": is_draw_market(market),
    }


# ── Tier Assignment ────────────────────────────────────────────────────
def assign_tier(parsed, league):
    """
    Assign a market to the highest qualifying tier.
    Returns (tier_number, tier_name) or (0, None) if no tier matches.
    """
    price_a = parsed["price_a"]
    price_b = parsed["price_b"]
    fav_price = max(price_a, price_b)
    team_b_is_fav = price_b > price_a
    team_b_is_dog = price_b < price_a
    is_coin_flip = 0.40 <= fav_price < 0.60

    # Sanity check: both prices must be reasonable (sum roughly to 1.0)
    # Skip for draw markets — they're Yes/No on a single outcome, not two-sided
    if not parsed.get("is_draw"):
        price_sum = price_a + price_b
        if price_sum < 0.70 or price_sum > 1.30:
            return 0, None  # Bad data — skip

    # Tier 1: Team B is favorite at 75-90% (any sport)
    if team_b_is_fav and 0.75 <= price_b < 0.90:
        return 1, TIER_NAMES[1]

    # Tier 3: CBB Team B underdog, fav at 50-55%
    if league == "cbb" and team_b_is_dog and 0.50 <= fav_price < 0.55:
        return 3, TIER_NAMES[3]

    # Tier 4: NBA exact 50/50 — both teams must be in 40-60% range
    if league == "nba" and fav_price < 0.506 and min(price_a, price_b) >= 0.40:
        return 4, TIER_NAMES[4]

    # Tier 2/6: Coin flips — both teams in reasonable range
    # T2 = bet favorite (NBA/NFL/MLB/ATP/WTA), T6 = bet underdog (CBB/NHL) — Kalshi-validated
    if league in ("cbb", "nba", "nhl") and is_coin_flip and min(price_a, price_b) >= 0.30:
        if league in T2_UNDERDOG_LEAGUES:
            return 6, TIER_NAMES[6]  # Bet underdog
        return 2, TIER_NAMES[2]  # Bet favorite

    return 0, None


# ── Bet Tracking ────────────────────────────────────────────────────────
def load_placed_bets():
    if os.path.exists(BETS_FILE):
        with open(BETS_FILE) as f:
            return json.load(f)
    return []


def save_bet(bet_info):
    bets = load_placed_bets()
    bets.append(bet_info)
    with open(BETS_FILE, "w") as f:
        json.dump(bets, f, indent=2, default=str)


# ── Order Placement ─────────────────────────────────────────────────────
def place_bet(market_slug, bet_price, bet_amount, buy_yes=False):
    """
    Place a bet on a market.
    - buy_yes=True: BUY_LONG (Team A bets, Draw bets). price.value = bet_price.
    - buy_yes=False: BUY_SHORT (Team B bets). price.value = 1 - bet_price.
    """
    quantity = max(1, int(bet_amount / bet_price))

    if buy_yes:
        # Team A or Draw: buy YES directly
        long_side_price = round(bet_price, 2)
        intent = "ORDER_INTENT_BUY_LONG"
    else:
        # Team B: buy NO, price.value = 1 - team_b_price
        long_side_price = round(1.0 - bet_price, 2)
        intent = "ORDER_INTENT_BUY_SHORT"

    order_request = {
        "marketSlug": market_slug,
        "type": "ORDER_TYPE_LIMIT",
        "price": {"value": str(long_side_price), "currency": "USD"},
        "quantity": quantity,
        "tif": "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL",
        "intent": intent,
        "manualOrderIndicator": "MANUAL_ORDER_INDICATOR_AUTOMATIC",
    }

    try:
        # Preview first (API requires body wrapped in "request")
        P(f"    Previewing order: {quantity} shares @ ${bet_price:.2f} = ${bet_amount:.2f} ({'YES' if buy_yes else 'NO'})")
        preview = auth_post("/v1/order/preview", {"request": order_request})
        P(f"    Preview OK: {json.dumps(preview, default=str)[:200]}")

        # Place the order
        result = auth_post("/v1/orders", order_request)
        P(f"    Order result: {json.dumps(result, default=str)[:200]}")
        return result
    except requests.exceptions.HTTPError as e:
        P(f"    ORDER ERROR: {e}")
        try:
            P(f"    Response: {e.response.text[:300]}")
        except Exception:
            pass
        return None
    except Exception as e:
        P(f"    ORDER ERROR: {e}")
        return None


# ── Main Scanner ────────────────────────────────────────────────────────
def scan_markets(live=False):
    P("=" * 65)
    P("  SCANNING — HYBRID 5-TIER STRATEGY")
    P("=" * 65)

    # Get balance
    balance_info = get_balance()
    if balance_info:
        bankroll = balance_info["buying_power"]
        P(f"  Balance: ${balance_info['current_balance']:.2f} | Buying Power: ${bankroll:.2f}")
    else:
        bankroll = 0
        P("  Could not fetch balance — running in scan-only mode")

    bet_size = max(MIN_BET, min(bankroll * BANKROLL_PCT, MAX_BET)) if bankroll > 0 else 0
    bet_size_new = max(MIN_BET, min(bankroll * BANKROLL_PCT_NEW, MAX_BET)) if bankroll > 0 else 0
    P(f"  Bet size: ${bet_size:.2f} (3% proven) / ${bet_size_new:.2f} (2% new leagues)")
    P()

    placed_bets = load_placed_bets()
    placed_slugs = {b["market_slug"] for b in placed_bets}  # Bot's own history
    # Track events to prevent betting both sides of same game
    placed_events = {b.get("event_title", "") for b in placed_bets if b.get("event_title")}

    # Also check API for existing positions and open orders (catches duplicates if bets file was stale)
    active_positions = get_existing_positions()
    open_orders = get_open_orders()
    placed_slugs = placed_slugs | active_positions | open_orders  # Merge all sets

    qualifying = []

    for league in LEAGUES:
        P(f"  Scanning {league.upper()}...")
        events = get_league_events(league)
        P(f"    Found {len(events)} events")

        for event in events:
            title = event.get("title", "Unknown")

            # Markets are embedded directly in events from v2 endpoint
            for market in event.get("markets", []):
                parsed = parse_market_from_event(market, league=league)
                if not parsed:
                    continue

                # Assign tier — skip if no tier matches
                tier, tier_name = assign_tier(parsed, league)
                if tier == 0:
                    continue

                slug = parsed["slug"]
                already_bet = slug in placed_slugs or title in placed_events

                # For draw bets: the bet price is the Yes price (min of the two)
                is_draw = parsed.get("is_draw", False)
                if is_draw:
                    bet_price = min(parsed["price_a"], parsed["price_b"])
                    bet_label = "Draw"
                elif tier == 6:
                    # T6: Coin flip underdog (CBB/NHL) — bet the cheaper side
                    if parsed["price_a"] <= parsed["price_b"]:
                        bet_price = parsed["price_a"]
                        bet_label = parsed["team_a"]
                    else:
                        bet_price = parsed["price_b"]
                        bet_label = parsed["team_b"]
                elif tier in (2, 7):
                    # T2: Coin flip favorite / T7: 60%+ favorite — bet the pricier side
                    if parsed["price_a"] >= parsed["price_b"]:
                        bet_price = parsed["price_a"]
                        bet_label = parsed["team_a"]
                    else:
                        bet_price = parsed["price_b"]
                        bet_label = parsed["team_b"]
                else:
                    bet_price = parsed["price_b"]
                    bet_label = parsed["team_b"]

                qualifying.append({
                    **parsed,
                    "tier": tier,
                    "tier_name": tier_name,
                    "fav_price": max(parsed["price_a"], parsed["price_b"]),
                    "bet_price": bet_price,
                    "bet_label": bet_label,
                    "event_title": title,
                    "league": league,
                    "already_bet": already_bet,
                })

                tag = "[DONE]" if already_bet else "[SIGNAL]"
                P(f"    {tag} {tier_name:<20} | {parsed['question'][:35]:<35} | {bet_label:<15} @{bet_price:.1%}")

        time.sleep(0.1)

    P()
    P(f"  Total qualifying markets: {len(qualifying)}")
    for t in sorted(set(q["tier"] for q in qualifying)):
        count = sum(1 for q in qualifying if q["tier"] == t)
        P(f"    {TIER_NAMES[t]}: {count}")
    new_markets = [q for q in qualifying if not q["already_bet"]]
    P(f"  New (not yet bet): {len(new_markets)}")
    P()

    if not new_markets:
        P("  No new qualifying markets found.")
        return qualifying

    if live and bankroll > 0:
        P("  PLACING BETS:")
        P("  " + "-" * 65)
        for mkt in new_markets:
            # Sizing: T1=1.5%, new leagues=2%, proven=3%
            if mkt["tier"] == 1:
                mkt_bet_size = max(MIN_BET, min(bankroll * BANKROLL_PCT_T1, MAX_BET))
                sizing_label = "1.5%"
            elif mkt["league"] in NEW_LEAGUES:
                mkt_bet_size = bet_size_new
                sizing_label = "2%"
            else:
                mkt_bet_size = bet_size
                sizing_label = "3%"

            if mkt_bet_size < MIN_BET:
                P(f"    SKIP (bet too small): ${mkt_bet_size:.2f}")
                continue

            is_draw = mkt.get("is_draw", False)

            # Get fresh price from single-market endpoint
            _, _, fresh_a, fresh_b = get_market_price(mkt["market_id"])
            if fresh_a is not None and fresh_b is not None:
                # Re-check tier with fresh prices
                fresh_parsed = {**mkt, "price_a": fresh_a, "price_b": fresh_b}
                fresh_tier, _ = assign_tier(fresh_parsed, mkt["league"])
                if fresh_tier == 0:
                    P(f"    SKIP {mkt['bet_label']} — price moved out of tier range")
                    continue
                # Determine bet price based on tier
                if is_draw:
                    bet_price = min(fresh_a, fresh_b)
                elif fresh_tier == 6:
                    bet_price = min(fresh_a, fresh_b)  # underdog = cheaper side
                elif fresh_tier in (2, 7):
                    bet_price = max(fresh_a, fresh_b)  # favorite = pricier side
                else:
                    bet_price = fresh_b
            else:
                bet_price = mkt["bet_price"]

            # Determine buy direction
            # T6 underdog / T2 fav on Team A: need buy YES (long) on Team A
            # T1/T3/default Team B: buy NO (short) on Team A
            if is_draw:
                buy_yes = True
            elif mkt["tier"] in (2, 6, 7) and mkt["bet_label"] == mkt["team_a"]:
                buy_yes = True  # Betting on Team A = buy YES
            else:
                buy_yes = False  # Betting on Team B = buy NO

            P(f"    [{mkt['tier_name']}] Betting ${mkt_bet_size:.2f} ({sizing_label}) on {mkt['bet_label']} @ {bet_price:.1%} | {mkt['league'].upper()} {mkt['question'][:30]}")
            result = place_bet(mkt["slug"], bet_price, mkt_bet_size, buy_yes=buy_yes)

            if result:
                save_bet({
                    "market_slug": mkt["slug"],
                    "question": mkt["question"],
                    "team_a": mkt["team_a"],
                    "team_b": mkt["bet_label"],
                    "price_b_at_bet": bet_price,
                    "bet_amount": mkt_bet_size,
                    "tier": mkt["tier"],
                    "tier_name": mkt["tier_name"],
                    "league": mkt["league"],
                    "is_draw": is_draw,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "order_result": result,
                })
                P(f"    BET PLACED on {mkt['bet_label']}")
                # Track event to prevent betting both sides of same game
                if mkt.get("event_title"):
                    placed_events.add(mkt["event_title"])
            else:
                P(f"    BET FAILED for {mkt['bet_label']}")

            time.sleep(0.5)

        # Refresh balance after betting
        new_bal = get_balance()
        if new_bal:
            P(f"\n  Updated Balance: ${new_bal['current_balance']:.2f} | Buying Power: ${new_bal['buying_power']:.2f}")
    elif live:
        P("  Cannot place bets — no buying power available")
    else:
        P("  DRY RUN — use --live to actually place bets")
        P("  " + "-" * 65)
        for mkt in new_markets:
            if mkt["tier"] == 1:
                mkt_bet_size = max(MIN_BET, min(bankroll * BANKROLL_PCT_T1, MAX_BET)) if bankroll > 0 else 0
                sizing_label = "1.5%"
            elif mkt["league"] in NEW_LEAGUES:
                mkt_bet_size = bet_size_new
                sizing_label = "2%"
            else:
                mkt_bet_size = bet_size
                sizing_label = "3%"
            P(f"    [{mkt['tier_name']}] WOULD BET ${mkt_bet_size:.2f} ({sizing_label}) on {mkt['bet_label']:<15} @{mkt['bet_price']:.1%} | {mkt['league'].upper()} {mkt['question'][:30]}")

    return qualifying


def monitor_loop(live=False):
    """Continuous monitoring loop."""
    P("Starting monitor loop...")
    P(f"  Scan interval: {SCAN_INTERVAL}s ({SCAN_INTERVAL//60}m)")
    P(f"  Live trading: {'YES' if live else 'NO (dry run)'}")
    P()

    while True:
        try:
            scan_markets(live=live)
        except Exception as e:
            P(f"  ERROR in scan: {e}")
            import traceback
            traceback.print_exc()
        P(f"\n  Next scan in {SCAN_INTERVAL//60}m...\n")
        time.sleep(SCAN_INTERVAL)


# ── Entry Point ─────────────────────────────────────────────────────────
if __name__ == "__main__":
    if not API_KEY or not API_SECRET:
        P("Set PM_API_KEY and PM_API_SECRET environment variables")
        P("  export PM_API_KEY='your-key-id'")
        P("  export PM_API_SECRET='your-secret-key'")
        sys.exit(1)

    live = "--live" in sys.argv
    monitor = "--monitor" in sys.argv

    P("=" * 65)
    P("  POLYMARKET — MULTI-TIER BOT")
    P(f"  Mode: {'LIVE' if live else 'DRY RUN'} | Sizing: {BANKROLL_PCT*100:.0f}% of bankroll")
    P(f"  Leagues: {', '.join(l.upper() for l in LEAGUES)}")
    P(f"  Bet window: {BET_WINDOW_HOURS}h before game start")
    P(f"  Sizing: 3% (NBA/CBB) | 2% (NHL/MLB/EPL/DOTA2)")
    P(f"  Tiers: T1=B fav 75-90% | T2=Coin flip fav | T6=Coin flip dog")
    P(f"         T3=CBB B dog 50-55% | T4=NBA 50/50")
    P(f"         T7=60%+ fav (DOTA2/EPL/MLB)")
    P("=" * 65)
    P()

    if monitor:
        monitor_loop(live=live)
    else:
        qualifying = scan_markets(live=live)

        # Save bot status for dashboard
        status = {
            "last_run": datetime.now(timezone.utc).isoformat(),
            "mode": "LIVE" if live else "DRY RUN",
            "markets_scanned": len(qualifying),
            "bets_placed": len(load_placed_bets()),
        }
        with open("bot_status.json", "w") as f:
            json.dump(status, f, indent=2)

    _log.close()
