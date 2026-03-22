"""
Flask web server for Polymarket Dashboard.
Serves the dashboard with live P&L data from the API.
Auto-refreshes data every 10 minutes.
"""

import os
import json
import threading
import time
from datetime import datetime, timezone
from flask import Flask, Response

app = Flask(__name__)

# ── Shared state ─────────────────────────────────────────────────────────
_report = {}
_bot_bets = []
_last_refresh = None
REFRESH_INTERVAL = 600  # 10 minutes


def refresh_data():
    """Fetch fresh data from API and recalculate P&L."""
    global _report, _bot_bets, _last_refresh

    try:
        from pnl_tracker import fetch_all_activities, calculate_pnl

        API_KEY = os.environ.get("PM_API_KEY", "")
        API_SECRET = os.environ.get("PM_API_SECRET", "")

        if API_KEY and API_SECRET:
            activities = fetch_all_activities()
        elif os.path.exists("my_activities.json"):
            with open("my_activities.json") as f:
                activities = json.load(f)
        else:
            print("[web] No API keys or cached activities", flush=True)
            return

        _report = calculate_pnl(activities)
        _last_refresh = datetime.now(timezone.utc).isoformat()
        print(f"[web] Data refreshed: {_report.get('resolved_count', 0)} resolved, "
              f"P&L ${_report.get('total_resolved_pnl', 0):+.2f}", flush=True)

    except Exception as e:
        print(f"[web] Refresh error: {e}", flush=True)

    # Load bot bets
    try:
        if os.path.exists("trading_bot_bets.json"):
            with open("trading_bot_bets.json") as f:
                _bot_bets = json.load(f)
    except Exception as e:
        print(f"[web] Bot bets load error: {e}", flush=True)


def background_refresher():
    """Background thread that refreshes data periodically."""
    while True:
        refresh_data()
        time.sleep(REFRESH_INTERVAL)


# Start background refresh thread
_refresh_thread = threading.Thread(target=background_refresher, daemon=True)
_refresh_thread.start()


@app.route("/")
def dashboard():
    """Serve the dashboard with live data injected."""
    template_path = "dashboard_template.html"
    if not os.path.exists(template_path):
        return "Dashboard template not found", 404

    with open(template_path) as f:
        html = f.read()

    html = html.replace("REPORT_DATA_PLACEHOLDER", json.dumps(_report, default=str))
    html = html.replace("BOT_DATA_PLACEHOLDER", json.dumps(_bot_bets, default=str))

    return Response(html, mimetype="text/html")


@app.route("/api/report")
def api_report():
    """JSON endpoint for dashboard data."""
    return {
        "report": _report,
        "bot_bets": _bot_bets,
        "last_refresh": _last_refresh,
    }


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    """Manually trigger a data refresh."""
    refresh_data()
    return {"status": "refreshed", "last_refresh": _last_refresh}


@app.route("/health")
def health():
    return {"status": "ok", "last_refresh": _last_refresh}


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
