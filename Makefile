# MiniQuantDesk Makefile
# Usage: make <target>

# Detect venv python path for Windows vs Unix without requiring activation
PY := $(shell if [ -x .venv/bin/python ]; then echo .venv/bin/python; else echo .venv/Scripts/python.exe; fi)
PIP := $(shell if [ -x .venv/bin/pip ]; then echo .venv/bin/pip; else echo .venv/Scripts/pip.exe; fi)

.PHONY: help venv install preflight backtest paper live logs clean

help:
\t@echo "Available commands:"
\t@echo "  make venv       - create virtual environment (.venv)"
\t@echo "  make install    - install dependencies into .venv"
\t@echo "  make preflight  - run environment checks (offline, no smoke)"
\t@echo "  make backtest   - run bot in BACKTEST mode"
\t@echo "  make paper      - run bot in PAPER mode (with sidecar)"
\t@echo "  make live       - run bot in LIVE mode (with sidecar)"
\t@echo "  make logs       - tail logs/ output (best effort)"
\t@echo "  make clean      - remove venv + logs + caches"

venv:
\tpython -m venv .venv

install: venv
\t$(PY) -m pip install --upgrade pip
\t$(PIP) install -r requirements.txt

preflight:
\t$(PY) scripts/preflight.py --offline --no-smoke

backtest:
\tMODE=BACKTEST $(PY) apps/executor/TradingBot.py

paper:
\t$(PY) scripts/run_all.py --with-bot --mode PAPER

live:
\t$(PY) scripts/run_all.py --with-bot --mode LIVE

logs:
\t- tail -f logs/*.out || tail -f ml_sidecar/logs/sidecar.log || type logs\\*.out || type ml_sidecar\\logs\\sidecar.log

clean:
\t- rm -rf .venv logs ml_sidecar/logs ml_sidecar/data __pycache__ */__pycache__ || true
\t- powershell -NoProfile -Command "Remove-Item -Recurse -Force .venv, logs, ml_sidecar\\logs, ml_sidecar\\data" 2> NUL || exit 0
