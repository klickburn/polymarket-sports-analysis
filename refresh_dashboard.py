"""
One-command dashboard refresh.
Fetches fresh activity data from API, recalculates P&L, rebuilds dashboard.

Usage:
    python3 refresh_dashboard.py
"""

import json
import os
import sys

# Step 1: Refresh activities from API
print("  [1/3] Fetching fresh activities from API...")
from pnl_tracker import fetch_all_activities, calculate_pnl

API_KEY = os.environ.get("PM_API_KEY", "")
API_SECRET = os.environ.get("PM_API_SECRET", "")

if API_KEY and API_SECRET:
    activities = fetch_all_activities()
else:
    print("  No API keys — using cached activities")
    with open("my_activities.json") as f:
        activities = json.load(f)

# Step 2: Calculate P&L
print("  [2/3] Calculating P&L...")
report = calculate_pnl(activities)

with open("pnl_report.json", "w") as f:
    json.dump(report, f, indent=2, default=str)

# Step 3: Rebuild dashboard
print("  [3/3] Building dashboard...")

bot_file = "trading_bot_bets.json"
bot_bets = []
if os.path.exists(bot_file):
    with open(bot_file) as f:
        bot_bets = json.load(f)

# Read the template (raw HTML with placeholders)
# We store a clean template separately
template_file = "dashboard_template.html"
if not os.path.exists(template_file):
    print("  No template found — reading current dashboard as template")
    # If no template, we can't rebuild. Just inform user.
    print("  Run this script after the dashboard has been set up.")
    sys.exit(1)

with open(template_file) as f:
    html = f.read()

html = html.replace("REPORT_DATA_PLACEHOLDER", json.dumps(report, default=str))
html = html.replace("BOT_DATA_PLACEHOLDER", json.dumps(bot_bets, default=str))

with open("dashboard.html", "w") as f:
    f.write(html)

print()
print(f"  Dashboard refreshed!")
print(f"  Resolved: {report['resolved_count']} ({report['wins']}W-{report['losses']}L)")
print(f"  P&L: ${report['total_resolved_pnl']:+.2f}")
print(f"  Open: {report['open_count']} positions")
print(f"  Bot bets: {len(bot_bets)}")
print(f"  → Open dashboard.html to view")
