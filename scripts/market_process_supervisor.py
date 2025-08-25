
#!/usr/bin/env python3
"""
market_process_supervisor.py
 - Time-gated supervisor that runs BOTH TradingBot and sidecar during market hours.
 - Restarts on crash within the window; stops both at close.
 - Honors kill file.
Env:
  MARKET_TZ=America/New_York
  MARKET_OPEN=09:30
  MARKET_CLOSE=16:00
  RUN_PREMARKET=false
  RUN_POSTMARKET=false
  SUPERVISOR_POLL_SEC=15
  KILL_FILE=ml_sidecar/logs/kill.txt
  BOT_CMD=python -m apps.executor.TradingBot
  SIDECAR_CMD=python ml_sidecar/sidecar/logger_service.py
"""

import os, sys, time, shlex, subprocess, signal
from datetime import datetime, time as dtime, timezone

MARKET_TZ = os.getenv("MARKET_TZ", "America/New_York")
OPEN_STR  = os.getenv("MARKET_OPEN", "09:30")
CLOSE_STR = os.getenv("MARKET_CLOSE", "16:00")
RUN_PRE   = os.getenv("RUN_PREMARKET", "false").lower() == "true"
RUN_POST  = os.getenv("RUN_POSTMARKET", "false").lower() == "true"
POLL_SEC  = int(os.getenv("SUPERVISOR_POLL_SEC", "15"))
KILL_FILE = os.getenv("KILL_FILE", "ml_sidecar/logs/kill.txt")
BOT_CMD   = os.getenv("BOT_CMD", "python -m apps.executor.TradingBot")
SIDECAR_CMD = os.getenv("SIDECAR_CMD", "python ml_sidecar/sidecar/logger_service.py")

try:
    from zoneinfo import ZoneInfo
    TZ = ZoneInfo(MARKET_TZ)
except Exception:
    TZ = timezone.utc

def parse_hhmm(s: str) -> dtime:
    hh, mm = s.split(":")
    return dtime(int(hh), int(mm))

OPEN_T = parse_hhmm(OPEN_STR)
CLOSE_T = parse_hhmm(CLOSE_STR)

def now_local():
    return datetime.now(TZ)

def in_window(dt_local: datetime) -> bool:
    if dt_local.weekday() >= 5:
        return RUN_PRE or RUN_POST
    t = dt_local.timetz()
    start = dtime(0,0, tzinfo=TZ) if RUN_PRE else OPEN_T.replace(tzinfo=TZ)
    end   = dtime(23,59,59, tzinfo=TZ) if RUN_POST else CLOSE_T.replace(tzinfo=TZ)
    return start <= t <= end

def spawn(cmd: str) -> subprocess.Popen:
    args = cmd if os.name == "nt" else shlex.split(cmd)
    print(f"[SUP] launch: {cmd}")
    return subprocess.Popen(args)

def stop(p: subprocess.Popen, name: str, grace: float = 10.0):
    if p is None or p.poll() is not None:
        return
    print(f"[SUP] stopping {name} ...")
    try:
        if os.name == "nt":
            p.terminate()
        else:
            p.send_signal(signal.SIGTERM)
        waited = 0.0
        while p.poll() is None and waited < grace:
            time.sleep(0.5); waited += 0.5
        if p.poll() is None:
            print(f"[SUP] kill {name}")
            p.kill()
    except Exception as e:
        print(f"[SUP] stop {name} failed: {e}")

def main():
    print(f"[SUP] Market supervisor: TZ={MARKET_TZ}, open={OPEN_STR}, close={CLOSE_STR}, pre={RUN_PRE}, post={RUN_POST}")
    bot = None
    side = None
    try:
        while True:
            if os.path.exists(KILL_FILE):
                print("[SUP] Kill file detected; stopping children.")
                stop(bot, "bot"); stop(side, "sidecar")
                break

            now = now_local()
            allowed = in_window(now)

            if allowed:
                # start if needed
                if bot is None or bot.poll() is not None:
                    bot = spawn(BOT_CMD)
                if side is None or side.poll() is not None:
                    side = spawn(SIDECAR_CMD)
                # if any crashed, restart after small backoff
                if (bot and bot.poll() is not None) or (side and side.poll() is not None):
                    print("[SUP] child exited; restarting in 5s")
                    time.sleep(5)
                    continue
            else:
                # outside window: stop any running
                if bot:  stop(bot, "bot");  bot = None
                if side: stop(side, "sidecar"); side = None

            time.sleep(POLL_SEC)
    except KeyboardInterrupt:
        print("\n[SUP] Ctrl-C received. Stopping children.")
        stop(bot, "bot"); stop(side, "sidecar")

if __name__ == "__main__":
    main()
