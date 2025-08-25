
#!/usr/bin/env python3
"""
process_supervisor.py
 - Runs TradingBot AND the sidecar logger together, with restart-on-crash.
 - Honors kill file; clean shutdown on Ctrl+C.
 - Cross-platform; no external deps.
Env (optional):
  KILL_FILE=ml_sidecar/logs/kill.txt
  SUP_RESTART_BACKOFF_SEC=5
  BOT_CMD=python -m apps.executor.TradingBot
  SIDECAR_CMD=python ml_sidecar/sidecar/logger_service.py
"""

import os, sys, time, shlex, subprocess, signal

KILL_FILE = os.getenv("KILL_FILE", "ml_sidecar/logs/kill.txt")
BACKOFF   = float(os.getenv("SUP_RESTART_BACKOFF_SEC", "5"))
BOT_CMD   = os.getenv("BOT_CMD", "python -m apps.executor.TradingBot")
SIDECAR_CMD = os.getenv("SIDECAR_CMD", "python ml_sidecar/sidecar/logger_service.py")

def spawn(cmd: str) -> subprocess.Popen:
    args = cmd if os.name == "nt" else shlex.split(cmd)
    print(f"[SUP] launching: {cmd}")
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
    bot = None
    side = None
    try:
        while True:
            if os.path.exists(KILL_FILE):
                print("[SUP] Kill file detected; stopping.")
                stop(bot, "bot"); stop(side, "sidecar")
                break

            # start if needed
            if bot is None or bot.poll() is not None:
                bot = spawn(BOT_CMD)
            if side is None or side.poll() is not None:
                side = spawn(SIDECAR_CMD)

            # if any crashed, restart after backoff
            if (bot and bot.poll() is not None) or (side and side.poll() is not None):
                print(f"[SUP] child exited; restarting after {BACKOFF}s")
                time.sleep(BACKOFF)
                continue

            time.sleep(2)
    except KeyboardInterrupt:
        print("
[SUP] Ctrl-C received. Stopping children.")
        stop(bot, "bot"); stop(side, "sidecar")

if __name__ == "__main__":
    main()
