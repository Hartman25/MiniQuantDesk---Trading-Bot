
#!/usr/bin/env python3
"""
market_supervisor.py
 - Time-gates running of your trading bot during market hours.
 - Starts the bot at open, restarts on crash during the window, and stops at close.
 - Honors an on-disk kill file to stop immediately.
 - No external dependencies.

Env (optional):
  MARKET_TZ=America/New_York
  MARKET_OPEN=09:30
  MARKET_CLOSE=16:00
  RUN_PREMARKET=false
  RUN_POSTMARKET=false
  SUPERVISOR_POLL_SEC=15
  KILL_FILE=ml_sidecar/logs/kill.txt
  BOT_CMD=python -m apps.executor.TradingBot
"""

import os, sys, time, shlex, subprocess, signal
from datetime import datetime, time as dtime, timedelta, timezone

# ----- Config -----
MARKET_TZ = os.getenv("MARKET_TZ", "America/New_York")
OPEN_STR  = os.getenv("MARKET_OPEN", "09:30")
CLOSE_STR = os.getenv("MARKET_CLOSE", "16:00")
RUN_PRE   = os.getenv("RUN_PREMARKET", "false").lower() == "true"
RUN_POST  = os.getenv("RUN_POSTMARKET", "false").lower() == "true"
POLL_SEC  = int(os.getenv("SUPERVISOR_POLL_SEC", "15"))
KILL_FILE = os.getenv("KILL_FILE", "ml_sidecar/logs/kill.txt")
BOT_CMD   = os.getenv("BOT_CMD", "python -m apps.executor.TradingBot")

# tz helper (no external deps)
try:
    from zoneinfo import ZoneInfo  # py>=3.9
    TZ = ZoneInfo(MARKET_TZ)
except Exception:
    # Fallback to UTC if zoneinfo not present
    TZ = timezone.utc

def parse_hhmm(s: str) -> dtime:
    hh, mm = s.split(":")
    return dtime(int(hh), int(mm))

OPEN_T = parse_hhmm(OPEN_STR)
CLOSE_T = parse_hhmm(CLOSE_STR)

def now_local():
    return datetime.now(TZ)

def in_window(dt_local: datetime) -> bool:
    # Weekdays only; extend with holiday logic later
    if dt_local.weekday() >= 5:  # 5=Sat,6=Sun
        return RUN_PRE or RUN_POST  # only run if explicitly allowed
    t = dt_local.timetz()
    start = dtime(0,0, tzinfo=TZ) if RUN_PRE else OPEN_T.replace(tzinfo=TZ)
    end   = dtime(23,59,59, tzinfo=TZ) if RUN_POST else CLOSE_T.replace(tzinfo=TZ)
    return start <= t <= end

def spawn(cmd: str) -> subprocess.Popen:
    args = cmd if os.name == "nt" else shlex.split(cmd)
    return subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

def stop(p: subprocess.Popen, grace: float = 10.0):
    if p.poll() is not None:
        return
    try:
        if os.name == "nt":
            p.terminate()
        else:
            p.send_signal(signal.SIGTERM)
        waited = 0.0
        while p.poll() is None and waited < grace:
            time.sleep(0.5); waited += 0.5
        if p.poll() is None:
            p.kill()
    except Exception:
        pass

def tail_pipe_once(p: subprocess.Popen):
    try:
        if p.stdout and not p.stdout.closed:
            lines = []
            while True:
                line = p.stdout.readline()
                if not line:
                    break
                lines.append(line.rstrip())
            if lines:
                print("[BOT]", lines[-1])
    except Exception:
        pass

def main():
    print(f"[SUP] Supervisor started. TZ={MARKET_TZ}, open={OPEN_STR}, close={CLOSE_STR}, pre={RUN_PRE}, post={RUN_POST}")
    bot: subprocess.Popen | None = None
    try:
        while True:
            if os.path.exists(KILL_FILE):
                print("[SUP] Kill file detected. Stopping.")
                if bot: stop(bot)
                break

            now = now_local()
            allowed = in_window(now)

            # Start if allowed and not running
            if allowed and (bot is None or bot.poll() is not None):
                print(f"[SUP] Launching bot: {BOT_CMD}")
                bot = spawn(BOT_CMD)

            # Stop if outside window and running
            if not allowed and bot is not None and bot.poll() is None:
                print("[SUP] Market closed window. Stopping bot.")
                stop(bot); bot = None

            # Restart on crash inside window
            if allowed and bot is not None and bot.poll() is not None:
                print("[SUP] Bot exited unexpectedly; restarting.")
                bot = spawn(BOT_CMD)

            # Light log pulse
            if bot is not None:
                tail_pipe_once(bot)

            time.sleep(POLL_SEC)
    except KeyboardInterrupt:
        print("
[SUP] Ctrl-C received. Stopping bot.")
        if bot: stop(bot)

if __name__ == "__main__":
    main()
