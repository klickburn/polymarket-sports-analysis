"""Rule tracker: what each sizing and exit rule changed, from the bot's own records.

Every number is "compared with not having the rule", at real size (positive =
the rule helped):

  size cuts      an order sent smaller than its full size because of ONE rule. If the
                 capped order filled, a full-size order is assumed to have filled the
                 same share of its size: won -> the cut gave up (full - sent) x share x
                 (1 - price); lost -> it avoided (full - sent) x share x price. Orders
                 cut by two or more rules at once are shown as "Several rules at once",
                 because removing one of them alone would not have changed the size.
  take-profit    P&L with the sale minus P&L held to settlement. Per contract sold:
                 + the sale price if the side then lost, sale price - 1 if it won.
  size raise     what the contracts above 1 made (Kalshi ETH core dips).
  late cancels   late_cancels.py, checked against public prices.

The same file runs in the Kalshi and the Polymarket bot; `venue` picks the record shape.
"""
from datetime import datetime, timezone, timedelta

WINDOW = 900
CUT_TAGS = ("momentum_cut", "weekday_cut", "expiry_week", "frozen", "reduced_hours")


def _ts(v):
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return None


def _norm(b, venue):
    if venue == "kalshi":
        if not b.get("dip_add"):
            return None
        end = _ts(b.get("window_end"))
    else:
        if b.get("kind") != "dip" or b.get("phantom"):
            return None
        end = float(b.get("close_ts") or 0) or _ts(b.get("window_end"))
    if not end:
        return None
    sent = float(b.get("contracts") or 0)
    return {"type": b.get("dip_type") or "split", "crypto": (b.get("crypto") or "").upper(),
            "price": float(b.get("fill_price") or b.get("price") or 0), "sent": sent,
            "full": float(b.get("full_count") or sent), "filled": float(b.get("filled_count") or 0),
            "result": b.get("result"), "pnl": b.get("pnl"), "start": end - WINDOW,
            "tags": [t for t in CUT_TAGS if b.get(t)], "tp": bool(b.get("tp_orders")),
            "tp_filled": b.get("tp_filled"), "tp_proceeds": float(b.get("tp_proceeds") or 0),
            "tp_fee": float(b.get("tp_fee") or 0)}


def _cut_rule(r, monday_hours):
    """Which rule cut this order, "several", or None if it went out at full size."""
    if not r["tags"] or r["full"] <= r["sent"] + 1e-9:
        return None
    keys = set()
    for t in r["tags"]:
        if t == "frozen":
            st = datetime.fromtimestamp(r["start"], timezone.utc)
            keys.add("freeze_monday" if (st.weekday(), st.hour) in monday_hours else "freeze_oneoff")
        elif t == "reduced_hours":
            keys.add("block_split" if r["type"] == "split" else "overnight_core")
        else:
            keys.add(t)
    return keys.pop() if len(keys) == 1 else "several"


def next_expiry_week(now):
    """(Monday, Thursday) dates of the next quarterly-expiry week not yet over."""
    d = now.date()
    for i in range(0, 15):
        y, m = d.year + (d.month - 1 + i) // 12, (d.month - 1 + i) % 12 + 1
        if m not in (3, 6, 9, 12):
            continue
        nxt = datetime(y + (m == 12), m % 12 + 1, 1).date()
        last = nxt - timedelta(days=1)
        fri = last - timedelta(days=(last.weekday() - 4) % 7)
        if fri - timedelta(days=1) >= d:
            return fri - timedelta(days=4), fri - timedelta(days=1)
    return None, None


def _row(key, label, note):
    return {"key": key, "label": label, "note": note, "since": None, "applied": 0, "filled": 0,
            "won": 0, "lost": 0, "waiting": 0, "net": 0.0, "today": 0.0}


def build(bets, venue, central_date, monday_hours, late=None, raises=(), now=None):
    """Rows for the dashboard's rule tracker. `late` is late_cancels.report();
    `raises` lists (key, label, note, crypto, type, since_epoch) size raises."""
    now = now or datetime.now(timezone.utc)
    today = central_date(now.timestamp())
    to = "1" if venue == "kalshi" else "5 (the minimum)"
    mon, thu = next_expiry_week(now)
    nxt = ("next " + mon.strftime("%b %-d") + "–" + thu.strftime("%-d")) if mon else ""
    defs = [
        ("momentum_cut", "Momentum cut", "core dip to %s when the last hour already moved 0.10%%+ our way" % to),
        ("weekday_cut", "BTC Wednesday cut", "BTC core dip to %s on Wednesdays (Central)" % to),
        ("tp", "90c take-profit", "a filled core dip gets a sell order at 90c"),
        ("expiry_week", "Quarterly-expiry freeze", "split dips to %s Mon–Thu before each quarterly expiry; %s" % (to, nxt)),
        ("freeze_monday", "Monday 10am–4pm freeze", "every dip to %s; split dips there also fall under the 10–4 cap (see Several)" % to),
        ("freeze_oneoff", "One-off freezes", "rest-of-day cuts asked for on the day"),
        ("block_split", "10am–4pm split cap", "split dips to %s from 10am to 4pm CDT" % to),
        ("overnight_core", "BTC overnight core cap", "BTC core dip to %s from 7pm to 1am CDT" % to),
        ("several", "Several rules at once", "cut by two or more of the rules above"),
    ]
    rows = {k: _row(k, lab, note) for k, lab, note in defs}
    for key, label, note, *_ in raises:
        rows[key] = _row(key, label, note)

    def hit(row, r, effect, outcome):
        row["applied"] += 1
        row["since"] = min(row["since"] or r["start"], r["start"])
        if outcome:
            row["filled"] += 1
            row[outcome] += 1
            row["net"] += effect
            if central_date(r["start"]) == today:
                row["today"] += effect

    for b in bets:
        r = _norm(b, venue)
        if not r:
            continue
        settled = r["result"] in ("win", "loss") and r["filled"] > 0
        outcome = ("won" if r["result"] == "win" else "lost") if settled else None
        # size cuts
        key = _cut_rule(r, monday_hours)
        if key:
            eff = 0.0
            if outcome and r["sent"] > 0:
                extra = (r["full"] - r["sent"]) * min(1.0, r["filled"] / r["sent"])
                eff = -extra * (1 - r["price"]) if outcome == "won" else extra * r["price"]
            hit(rows[key], r, eff, outcome)
        # take-profit
        if r["tp"] and settled:
            row = rows["tp"]
            if r["tp_filled"] is None:
                row["applied"] += 1
                row["waiting"] += 1
            else:
                tq = float(r["tp_filled"] or 0)
                eff = r["tp_proceeds"] - (tq if outcome == "won" else 0.0) - r["tp_fee"]
                # outcome counts only orders the take-profit actually sold
                hit(row, r, eff if tq > 0 else 0.0, outcome if tq > 0 else None)
        # size raises: what the contracts above 1 made
        for key, label, note, crypto, dtype, since in raises:
            if (r["crypto"] == crypto and r["type"] == dtype and r["start"] >= since
                    and r["sent"] > 1 and settled and r["pnl"] is not None):
                hit(rows[key], r, float(r["pnl"]) * (r["sent"] - 1) / r["sent"], outcome)

    out = []
    for k in [d[0] for d in defs] + [x[0] for x in raises]:
        row = rows[k]
        row["net"], row["today"] = round(row["net"], 2), round(row["today"], 2)
        row["since"] = row["since"] and datetime.fromtimestamp(row["since"], timezone.utc).isoformat()
        out.append(row)
    if late and late.get("rows"):
        lr = late["rows"]
        row = _row("late", "Late cancels", "unfilled dips cancelled near the end of the window (detail above)")
        row.update(since=late.get("since"), applied=sum(x["cancelled"] for x in lr),
                   filled=sum(x["fill_lost"] + x["fill_won"] for x in lr),
                   won=sum(x["fill_won"] for x in lr), lost=sum(x["fill_lost"] for x in lr),
                   waiting=sum(x["waiting"] for x in lr), net=late.get("saved", 0.0),
                   today=late.get("saved_today", 0.0))
        out.append(row)
    return {"rows": out, "generated": now.isoformat()}
