"""Polymarket 15-minute BTC/ETH bot — core (phantom), split dips, core dips.

Standalone service. Shares no file and no data with the Kalshi bot; the two
must never be able to affect each other's state. In particular the sizing
history lives in this service's own bets file, so Polymarket results can never
leak into Kalshi's cool-off / WR ladder / scale groups (or vice versa).

Three strategies, mirroring the Kalshi deployment:

  CORE       scored entry on one side of the window. Runs PHANTOM by default:
             recorded in full, no order placed. POLY_PHANTOM_MODE=off to trade.
  SPLIT DIP  when BTC and ETH take OPPOSITE sides in the same window, rest a
             limit buy at 10c on each crypto's own side.
  CORE DIP   on non-split windows, rest the same 10c buy on our own side.

EVERY SCANNED WINDOW IS RECORDED, including windows that do not qualify, with
the observed prices and the reason. That is deliberate. Polymarket's contracts
price against the window's own opening price rather than a fixed strike, and a
360-window sample put the favourite's peak at a median 0.51 — touching the
deployed 0.60-0.85 entry band once. If that holds, the scan log is what shows
it, with live numbers, instead of the bot simply looking idle.
"""
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

import poly_client as PC
from poly_client import P, window_close, get_window_markets
import poly_indicators as PI

# ── Config (POLY_* so it can never collide with the Kalshi service) ──────
DATA_DIR = os.environ.get("POLY_DATA_DIR", "/data")
if not os.path.isdir(DATA_DIR):
    DATA_DIR = os.path.dirname(__file__) or "."
BETS_FILE = os.path.join(DATA_DIR, "poly_bets.json")

MIN_PRICE = float(os.environ.get("POLY_SCORE_MIN_PRICE", "0.60"))
MAX_PRICE = float(os.environ.get("POLY_SCORE_MAX_PRICE", "0.85"))
MIN_SCORE = int(os.environ.get("POLY_SCORE_MIN_SCORE", "-3"))
MAX_SCORE = int(os.environ.get("POLY_SCORE_MAX_SCORE", "4"))
CORE_COUNT = int(os.environ.get("POLY_CORE_COUNT", "5"))
ENTRY_AFTER = int(os.environ.get("POLY_ENTRY_AFTER_SEC", "120"))   # ~2 min in

# "all" = never order core (default), "off" = trade it for real
PHANTOM_MODE = os.environ.get("POLY_PHANTOM_MODE", "all").strip().lower()

SPLIT_DIP_ENABLED = os.environ.get("POLY_SPLIT_DIP_ENABLED", "1") == "1"
SPLIT_DIP_COUNT = int(os.environ.get("POLY_SPLIT_DIP_COUNT", "100"))
SPLIT_DIP_PRICE = float(os.environ.get("POLY_SPLIT_DIP_PRICE", "0.10"))

CORE_DIP_ENABLED = os.environ.get("POLY_CORE_DIP_ENABLED", "1") == "1"
CORE_DIP_COUNT = int(os.environ.get("POLY_CORE_DIP_COUNT", "1"))
CORE_DIP_PRICE = float(os.environ.get("POLY_CORE_DIP_PRICE", "0.10"))
CORE_DIP_CRYPTOS = [c.strip().upper() for c in
                    os.environ.get("POLY_CORE_DIP_CRYPTOS", "BTC,ETH").split(",") if c.strip()]

POLL_SECONDS = int(os.environ.get("POLY_POLL_SECONDS", "20"))


# ── Bets file ───────────────────────────────────────────────────────────
def load_bets():
    try:
        with open(BETS_FILE) as f:
            return json.load(f)
    except FileNotFoundError:
        return []
    except Exception as e:
        P(f"  [BETS] load failed ({e}) — starting empty")
        return []


def save_bets(bets):
    tmp = BETS_FILE + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(bets, f)
        os.replace(tmp, BETS_FILE)
    except Exception as e:
        P(f"  [BETS] save failed: {e}")


def _now():
    return datetime.now(timezone.utc).isoformat()


# ── Sizing ──────────────────────────────────────────────────────────────
def _clamp_size(n, market, what):
    """Polymarket enforces a minimum order size (5 contracts today). The Kalshi
    core dip runs at 1, which is simply not placeable here, so it is raised and
    the change is recorded rather than silently applied."""
    lo = float(market.get("min_size") or 5)
    if n < lo:
        P(f"    [SIZE] {what}: {n} below venue minimum {lo:.0f} — raised")
        return lo, True
    return float(n), False


# ── Scan log: every window, qualifying or not ───────────────────────────
def record_scan(bets, close_ts, markets, evaluated):
    """One row per window per crypto, whether or not it produced a trade."""
    for crypto, ev in evaluated.items():
        bets.append({
            "venue": "polymarket",
            "kind": "scan",
            "crypto": crypto,
            "window_end": markets[crypto]["window_end"],
            "close_ts": close_ts,
            "slug": markets[crypto]["slug"],
            "yes_price": ev.get("yes_price"),
            "no_price": ev.get("no_price"),
            "best_side": ev.get("side"),
            "best_price": ev.get("price"),
            "score": ev.get("score"),
            "qualified": ev.get("qualified", False),
            "reason": ev.get("reason"),
            "timestamp": _now(),
        })


def evaluate(markets, indicators):
    """Score both sides of each crypto and pick the tradeable one, if any."""
    out = {}
    for crypto, m in markets.items():
        yes_tok = m["tokens"].get("yes")
        no_tok = m["tokens"].get("no")
        yes_p = PC.mid_price(yes_tok) if yes_tok else None
        no_p = PC.mid_price(no_tok) if no_tok else None
        ev = {"yes_price": yes_p, "no_price": no_p}
        if yes_p is None or no_p is None:
            ev["reason"] = "no_book"
            out[crypto] = ev
            continue
        # the favourite is the side the strategy would consider
        side, price = ("yes", yes_p) if yes_p >= no_p else ("no", no_p)
        ev["side"], ev["price"] = side, price
        if not (MIN_PRICE <= price <= MAX_PRICE):
            ev["reason"] = f"price {price:.2f} outside {MIN_PRICE}-{MAX_PRICE}"
            out[crypto] = ev
            continue
        try:
            score, reasons = PI.compute_score(crypto, side, price, indicators)
        except Exception as e:
            ev["reason"] = f"score error: {e}"
            out[crypto] = ev
            continue
        ev["score"] = score
        ev["reasons"] = reasons
        if not (MIN_SCORE <= score <= MAX_SCORE):
            ev["reason"] = f"score {score} outside {MIN_SCORE}-{MAX_SCORE}"
            out[crypto] = ev
            continue
        ev["qualified"] = True
        ev["reason"] = "qualified"
        out[crypto] = ev
    return out


# ── Core ────────────────────────────────────────────────────────────────
def place_core(bets, markets, evaluated, close_ts):
    """Record the core trade. Phantom by default: no order is placed."""
    placed = {}
    for crypto, ev in evaluated.items():
        if not ev.get("qualified"):
            continue
        m = markets[crypto]
        side = ev["side"]
        token = m["tokens"][side]
        rec = {
            "venue": "polymarket", "kind": "core", "crypto": crypto,
            "side": side, "token_id": token, "slug": m["slug"],
            "price": ev["price"], "score": ev.get("score"),
            "contracts": CORE_COUNT, "window_end": m["window_end"],
            "close_ts": close_ts, "result": "open", "timestamp": _now(),
        }
        if PHANTOM_MODE == "all" or not PC.is_live():
            rec.update({"phantom": True, "status": "phantom",
                        "order_id": None, "fill_price": ev["price"],
                        "filled_count": CORE_COUNT})
            P(f"    {crypto}: PHANTOM core {CORE_COUNT} {side.upper()} @ {ev['price']:.2f}")
        else:
            size, _ = _clamp_size(CORE_COUNT, m, f"{crypto} core")
            oid = PC.place_limit(token, "BUY", ev["price"], size)
            if not oid:
                rec.update({"result": "unfilled", "status": "failed"})
            else:
                rec.update({"order_id": oid, "status": "placed", "contracts": size})
                P(f"    {crypto}: core {size:.0f} {side.upper()} @ {ev['price']:.2f} -> {oid}")
        bets.append(rec)
        placed[crypto] = rec
    if placed:
        save_bets(bets)
    return placed


# ── Dips ────────────────────────────────────────────────────────────────
def _already_dipped(bets, close_ts, dip_type=None):
    for b in bets:
        if b.get("kind") == "dip" and b.get("close_ts") == close_ts:
            if dip_type is None or b.get("dip_type") == dip_type:
                return True
    return False


def _rest_dip(bets, market, side, count, dip_type, close_ts):
    token = market["tokens"].get(side)
    if not token:
        return False
    size, raised = _clamp_size(count, market, f"{market['crypto']} {dip_type} dip")
    rec = {
        "venue": "polymarket", "kind": "dip", "dip_type": dip_type,
        "crypto": market["crypto"], "side": side, "token_id": token,
        "slug": market["slug"], "price": CORE_DIP_PRICE if dip_type == "core" else SPLIT_DIP_PRICE,
        "contracts": size, "size_raised_to_min": raised,
        "window_end": market["window_end"], "close_ts": close_ts,
        "result": "dip_pending", "timestamp": _now(),
    }
    price = rec["price"]
    if not PC.is_live():
        rec.update({"result": "unfilled", "status": "read_only", "order_id": None})
        P(f"    [DIP] {dip_type} {market['crypto']} {side} {size:.0f} @ {price*100:.0f}c "
          f"— read-only, not placed")
        bets.append(rec)
        return False
    oid = PC.place_limit(token, "BUY", price, size)
    if not oid:
        rec.update({"result": "unfilled", "status": "failed"})
        bets.append(rec)
        return False
    rec.update({"order_id": oid, "status": "resting"})
    bets.append(rec)
    P(f"    [DIP] {dip_type} {market['crypto']} {side} rested {size:.0f} @ {price*100:.0f}c")
    return True


def place_split_dips(bets, markets, core_recs, close_ts):
    """Both cryptos, opposite sides, same window."""
    if not SPLIT_DIP_ENABLED or _already_dipped(bets, close_ts, "split"):
        return False
    if len(core_recs) < 2:
        return False
    sides = {c: r["side"] for c, r in core_recs.items()}
    if len(set(sides.values())) < 2:
        return False        # same side -> not a split
    P(f"  [DIP] split window ({'/'.join(f'{c}:{s}' for c, s in sides.items())})")
    any_ok = False
    for crypto, rec in core_recs.items():
        any_ok |= _rest_dip(bets, markets[crypto], rec["side"],
                            SPLIT_DIP_COUNT, "split", close_ts)
        time.sleep(0.25)
    save_bets(bets)
    return any_ok


def place_core_dips(bets, markets, core_recs, close_ts):
    """Non-split windows — the ones the split rule never covers."""
    if not CORE_DIP_ENABLED or _already_dipped(bets, close_ts, "core"):
        return False
    if not core_recs:
        return False
    sides = {c: r["side"] for c, r in core_recs.items()}
    if len(core_recs) > 1 and len(set(sides.values())) > 1:
        return False        # that's a split; the other rule owns it
    any_ok = False
    for crypto, rec in core_recs.items():
        if crypto not in CORE_DIP_CRYPTOS:
            continue
        any_ok |= _rest_dip(bets, markets[crypto], rec["side"],
                            CORE_DIP_COUNT, "core", close_ts)
        time.sleep(0.25)
    save_bets(bets)
    return any_ok


# ── Resolution ──────────────────────────────────────────────────────────
def resolve(bets):
    now = int(time.time())
    changed = 0
    for b in bets:
        if b.get("kind") not in ("core", "dip"):
            continue
        if b.get("result") in ("win", "loss", "unfilled"):
            continue
        cts = b.get("close_ts") or 0
        if cts + 60 > now:
            continue
        won = PC.settled_outcome(b.get("crypto"), cts)
        if won is None:
            continue
        hit = (won == b.get("side"))
        entry = float(b.get("fill_price") or b.get("price") or 0)
        n = float(b.get("contracts") or 0)
        b["result"] = "win" if hit else "loss"
        b["pnl"] = round(n * (1 - entry), 2) if hit else round(-n * entry, 2)
        b["resolved_at"] = _now()
        changed += 1
    if changed:
        save_bets(bets)
        P(f"  [RESOLVE] settled {changed}")
    return changed


# ── Main loop ───────────────────────────────────────────────────────────
def run_once(bets, state):
    close_ts = window_close()
    if state.get("window") != close_ts:
        state.update({"window": close_ts, "scanned": False, "cored": False})
        P(f"\n  -- Window closing {datetime.fromtimestamp(close_ts, timezone.utc).strftime('%H:%M')}Z "
          f"({len(bets)} records) --")
    markets = get_window_markets(close_ts)
    if not markets:
        return
    opened = close_ts - PC.WINDOW_SECONDS
    if time.time() - opened < ENTRY_AFTER:
        return                      # let the window develop, like Kalshi does
    if state.get("cored"):
        return
    try:
        prices = PI.fetch_crypto_prices()
        indicators = PI.compute_indicators(prices)
    except Exception as e:
        P(f"  [INDICATORS] {e}")
        return
    evaluated = evaluate(markets, indicators)
    if not state.get("scanned"):
        record_scan(bets, close_ts, markets, evaluated)
        save_bets(bets)
        state["scanned"] = True
        for c, ev in evaluated.items():
            P(f"    {c}: {ev.get('side')} @ {ev.get('price')} "
              f"score={ev.get('score')} -> {ev.get('reason')}")
    core_recs = place_core(bets, markets, evaluated, close_ts)
    if core_recs:
        if not place_split_dips(bets, markets, core_recs, close_ts):
            place_core_dips(bets, markets, core_recs, close_ts)
    state["cored"] = True


def main():
    P("=" * 62)
    P("  POLYMARKET 15m BOT — core(phantom) / split dip / core dip")
    P(f"  entry band {MIN_PRICE}-{MAX_PRICE}   score {MIN_SCORE}..{MAX_SCORE}")
    P(f"  phantom={PHANTOM_MODE}  split_dip={SPLIT_DIP_ENABLED}({SPLIT_DIP_COUNT})"
      f"  core_dip={CORE_DIP_ENABLED}({CORE_DIP_COUNT})")
    P(f"  bets file {BETS_FILE}")
    P("=" * 62)
    bal = PC.get_balance()
    P(f"  mode: {'LIVE' if PC.is_live() else 'READ-ONLY'}"
      + (f"   USDC {bal:,.2f}" if bal is not None else ""))
    bets = load_bets()
    state = {}
    while True:
        try:
            run_once(bets, state)
            resolve(bets)
        except Exception as e:
            P(f"  [LOOP] {e}")
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
