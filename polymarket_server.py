"""
Polymarket Server — Trading Bot + Dashboard on Railway
======================================================
Runs the Polymarket sports trading bot on a loop and serves a live dashboard.
Deploy as a separate Railway service in the same project.

Start command:
    uvicorn polymarket_server:app --host 0.0.0.0 --port ${PORT:-8081}

Required env vars:
    PM_API_KEY, PM_API_SECRET
"""

import os
import json
import time
import threading
from datetime import datetime, timezone
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

# ── Import from existing modules ──────────────────────────────────────
from pnl_tracker import fetch_all_activities, calculate_pnl, get_balance
from trading_bot import scan_markets, load_placed_bets, P as bot_P

# ── Config ─────────────────────────────────────────────────────────────
SCAN_INTERVAL = int(os.environ.get("PM_SCAN_INTERVAL", "300"))  # 5 min default
DATA_DIR = os.environ.get("PM_DATA_DIR", "/data")
if not os.path.isdir(DATA_DIR):
    DATA_DIR = "."

BETS_FILE = os.path.join(DATA_DIR, "trading_bot_bets.json")
STATUS_FILE = os.path.join(DATA_DIR, "pm_bot_status.json")

app = FastAPI(title="Polymarket Dashboard")

_cache = {"report": None, "bot_status": None, "refreshed_at": None}
_cache_lock = threading.Lock()


def P(msg=""):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# ── Background: Trading Bot Loop ──────────────────────────────────────
def bot_loop():
    """Run the trading bot on a loop (like --monitor --live)."""
    P("[BOT] Starting Polymarket trading bot loop")
    P(f"[BOT] Scan interval: {SCAN_INTERVAL}s ({SCAN_INTERVAL // 60}m)")

    # Wait for server to start
    time.sleep(5)

    while True:
        try:
            P("[BOT] Running market scan...")
            scan_markets(live=True)
            P("[BOT] Scan complete")
        except Exception as e:
            P(f"[BOT] ERROR in scan: {e}")
            import traceback
            traceback.print_exc()

        # Save bot status
        try:
            status = {
                "last_scan": datetime.now(timezone.utc).isoformat(),
                "scan_interval": SCAN_INTERVAL,
                "running": True,
            }
            with open(STATUS_FILE, "w") as f:
                json.dump(status, f)
        except Exception:
            pass

        P(f"[BOT] Next scan in {SCAN_INTERVAL // 60}m...")
        time.sleep(SCAN_INTERVAL)


# ── Background: Data Refresh Loop ─────────────────────────────────────
def data_refresh_loop():
    """Refresh P&L data every 5 minutes for the dashboard."""
    P("[DATA] Starting data refresh loop")
    time.sleep(10)  # Let bot run first

    while True:
        try:
            P("[DATA] Refreshing P&L data...")
            activities = fetch_all_activities()
            report = calculate_pnl(activities)

            # Load bot bets
            bot_bets = []
            # Check both possible locations
            for bf in [BETS_FILE, "trading_bot_bets.json"]:
                if os.path.exists(bf):
                    try:
                        with open(bf) as f:
                            bot_bets = json.load(f)
                        break
                    except Exception:
                        pass

            # Load bot status
            bot_status = {}
            for sf in [STATUS_FILE, "bot_status.json"]:
                if os.path.exists(sf):
                    try:
                        with open(sf) as f:
                            bot_status = json.load(f)
                        break
                    except Exception:
                        pass

            # Get balance
            balance_info = get_balance() or {}

            with _cache_lock:
                _cache["report"] = report
                _cache["bot_bets"] = bot_bets
                _cache["bot_status"] = bot_status
                _cache["balance"] = balance_info
                _cache["refreshed_at"] = datetime.now(timezone.utc).isoformat()

            P(f"[DATA] Refreshed: {report['resolved_count']} resolved, "
              f"{report['wins']}W-{report['losses']}L, "
              f"P&L: ${report['total_resolved_pnl']:+.2f}")

        except Exception as e:
            P(f"[DATA] ERROR refreshing: {e}")
            import traceback
            traceback.print_exc()

        time.sleep(300)  # Refresh every 5 min


# ── API Endpoints ─────────────────────────────────────────────────────
@app.get("/api/data")
def get_data():
    with _cache_lock:
        if _cache["report"] is None:
            return JSONResponse({"status": "warming up"}, status_code=503)
        return {
            "report": _cache["report"],
            "bot_bets": _cache.get("bot_bets", []),
            "bot_status": _cache.get("bot_status", {}),
            "balance": _cache.get("balance", {}),
            "refreshed_at": _cache["refreshed_at"],
        }


@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}


@app.get("/", response_class=HTMLResponse)
def dashboard():
    """Serve the Polymarket dashboard."""
    template_file = os.path.join(os.path.dirname(__file__), "dashboard_template.html")
    if not os.path.exists(template_file):
        return HTMLResponse("<h1>Dashboard template not found</h1>", status_code=500)

    with open(template_file) as f:
        html = f.read()

    # Replace static placeholders with empty defaults — JS will fetch live data
    html = html.replace("REPORT_DATA_PLACEHOLDER", "{}")
    html = html.replace("BOT_DATA_PLACEHOLDER", "[]")
    html = html.replace("BOT_STATUS_PLACEHOLDER", "{}")
    html = html.replace("DATA_REFRESHED_PLACEHOLDER", "")

    # Inject live-data fetching script before </body>
    live_script = """
<script>
(async function() {
  // Fetch live data from API and re-render
  for (let i = 0; i < 20; i++) {
    try {
      const resp = await fetch('/api/data');
      if (resp.status === 503) {
        await new Promise(r => setTimeout(r, 3000));
        continue;
      }
      if (!resp.ok) throw new Error('API error');
      const data = await resp.json();
      // Update the baked data and re-render if the dashboard supports it
      if (window._REPORT) {
        Object.assign(window._REPORT, data.report);
      }
      if (window._BOT_BETS) {
        window._BOT_BETS.length = 0;
        window._BOT_BETS.push(...(data.bot_bets || []));
      }
      // Show refresh time
      const refreshEl = document.querySelector('.subtitle');
      if (refreshEl && data.refreshed_at) {
        refreshEl.textContent = 'Live — Last refreshed: ' + new Date(data.refreshed_at).toLocaleString();
      }
      console.log('Dashboard data loaded from API');
      break;
    } catch(e) {
      console.warn('Retrying...', e);
      await new Promise(r => setTimeout(r, 3000));
    }
  }
})();
</script>
"""
    html = html.replace("</body>", live_script + "</body>")
    return HTMLResponse(html)


# ── Startup ────────────────────────────────────────────────────────────
@app.on_event("startup")
def startup():
    P("=" * 60)
    P("  POLYMARKET SERVER")
    P(f"  Scan interval: {SCAN_INTERVAL}s")
    P(f"  Data dir: {DATA_DIR}")
    P("=" * 60)

    threading.Thread(target=bot_loop, daemon=True, name="pm-bot").start()
    threading.Thread(target=data_refresh_loop, daemon=True, name="pm-data").start()
