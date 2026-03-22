"""
Background worker for Render deployment.
Runs the trading bot in continuous monitor mode.
"""

import subprocess
import sys
import time

print("[worker] Starting trading bot in live monitor mode...", flush=True)

while True:
    try:
        result = subprocess.run(
            [sys.executable, "trading_bot.py", "--live"],
            capture_output=False,
            timeout=300,  # 5 min timeout per scan
        )
        print(f"[worker] Scan complete (exit code: {result.returncode})", flush=True)
    except subprocess.TimeoutExpired:
        print("[worker] Scan timed out, continuing...", flush=True)
    except Exception as e:
        print(f"[worker] Error: {e}", flush=True)

    # Wait 30 minutes between scans
    print("[worker] Sleeping 30 minutes until next scan...", flush=True)
    time.sleep(1800)
