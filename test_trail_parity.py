"""Parity test for the trailing-stop / streak-hold logic.

The bot decides each trade's size by replaying the day's *already-recorded* trades
through `_trail_replay` (incremental use). This test proves that incremental use
reproduces, bit-for-bit, the all-at-once reference policy that the backtest used to
produce the +$4,025 streak-hold result — for the plain latch (k=0) and the
streak-hold toggle (k>0). If this ever fails, the live stop has drifted from the
backtest and must not be trusted.

Run: python3 test_trail_parity.py
"""
import random
from crypto_score_bot import _trail_replay


def reference_policy(day, arm, give, k, floor=0.0, react_k=0):
    """All-at-once backtest policy: returns the list of realized pnl for a day.
    day: list of (full_scale_pnl, one_x_pnl, won)."""
    out = []; cum = 0.0; peak = 0.0; s = False; st = 0; fl = False
    for full, onex, won in day:
        if k > 0 and s and st >= k:
            s = False
        if fl and react_k > 0 and st >= react_k:
            fl = False
        r = onex if (s or fl) else full
        out.append(r); cum += r
        if cum > peak:
            peak = cum
        st = st + 1 if won else 0
        if not s and (k <= 0 or st < k) and peak >= arm and (peak - cum) >= give:
            s = True
        if floor > 0 and cum <= -floor:
            fl = True
    return out


def live_policy(day, arm, give, k, floor=0.0, react_k=0):
    """How the bot runs it: for each trade, replay the realized-so-far via
    _trail_replay to get the clip decision, then record the realized pnl."""
    out = []; realized = []  # (realized_pnl, won)
    for full, onex, won in day:
        clip, *_ = _trail_replay(realized, arm, give, k, floor, react_k)
        r = onex if clip else full
        out.append(r); realized.append((r, won))
    return out


def make_day(rng):
    n = rng.randint(1, 80)
    day = []
    for _ in range(n):
        won = rng.random() < 0.55
        price = rng.uniform(0.4, 0.85)
        scale = rng.choice([1, 30])
        onex = (1 - price) if won else -price
        full = scale * onex
        day.append((round(full, 2), round(onex, 2), won))
    return day


def main():
    rng = random.Random(20260716)
    # (arm, give, k, floor, react_k) — trailing only, floor only, and combined
    params = [(40, 20, 0, 0, 0), (30, 20, 0, 0, 0), (30, 20, 2, 0, 0),
              (40, 20, 3, 0, 0), (30, 20, 1, 0, 0), (50, 25, 2, 0, 0),
              (20, 18, 2, 35, 1), (20, 18, 2, 50, 2), (20, 18, 2, 60, 1),
              (0, 0, 0, 40, 1), (20, 18, 2, 35, 3)]
    mismatches = 0; checked = 0
    for _ in range(4000):
        day = make_day(rng)
        for arm, give, k, floor, react_k in params:
            ref = reference_policy(day, arm, give, k, floor, react_k)
            live = live_policy(day, arm, give, k, floor, react_k)
            checked += len(ref)
            if any(abs(a - b) > 1e-9 for a, b in zip(ref, live)):
                mismatches += 1
    print(f"checked {checked} trade decisions across 4000 random days x {len(params)} configs")
    assert mismatches == 0, f"PARITY FAILED: {mismatches} day mismatches"
    print("PARITY OK: bot's _trail_replay reproduces the backtest policy exactly")


if __name__ == "__main__":
    main()
