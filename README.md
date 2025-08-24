# MiniQuantDesk

A modular **mini quant trading desk** that supports backtesting, paper trading, and live execution with Alpaca.  
Includes logging, Discord integration, and an ML sidecar for telemetry + feature capture.

---

## 📂 Project Structure
```
MiniQuantDesk/
├─ apps/
│  └─ executor/        # Trading bot (backtest, paper, live)
│     └─ TradingBot.py
├─ scripts/
│  ├─ preflight.py     # environment + integration checks
│  └─ run_all.py       # supervisor for bot + sidecar
├─ ml_sidecar/
│  ├─ schemas.py       # event schemas
│  └─ sidecar/
│     ├─ event_bus.py  # ZeroMQ publisher
│     └─ logger_service.py # SUB logger -> parquet/logs
├─ docs/               # roadmap and planning docs
│  ├─ mini_quant_desk_roadmap.docx
│  └─ mini_quant_desk_plan.docx
├─ env                 # your real secrets (NOT committed)
├─ .env.example        # template for env config
├─ .gitignore
├─ requirements.txt
└─ README.md
```

---

## 🚀 Quick Start

### 1. Clone / Create Repo
```bash
git clone <your-repo-url>
cd MiniQuantDesk
```

### 2. Create Virtual Environment
- **PowerShell (Windows)**
  ```powershell
  python -m venv .venv
  .\.venv\Scripts\Activate.ps1
  ```
  If blocked, run:
  ```powershell
  Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
  .\.venv\Scripts\Activate.ps1
  ```

- **Mac/Linux**
  ```bash
  python3 -m venv .venv
  source .venv/bin/activate
  ```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure Environment
Copy `.env.example` to `env` (or `.env`) and fill in:
- Alpaca keys (paper & live)
- Discord bot token + webhooks
- User timezone
- Risk settings

⚠️ Never commit your real `env` file.

---

## 🧪 Preflight Checks
Run environment checks:
```bash
python scripts/preflight.py --offline --no-smoke
```
- `--offline` skips Alpaca/webhook checks.  
- Remove it once your `env` is configured.

---

## ▶️ Run the System

### Sidecar Only (logging/telemetry)
```bash
python scripts/run_all.py
```

### Bot + Sidecar (Paper mode)
```bash
python scripts/run_all.py --with-bot --mode PAPER
```

### Bot + Sidecar (Backtest)
```bash
set MODE=BACKTEST   # or export MODE=BACKTEST (Mac/Linux)
python apps/executor/TradingBot.py
```

---

## 📊 Features
- **Backtest**: multi-symbol, CSV reports, Discord digests.
- **Paper/Live**: Alpaca execution, Discord alerts, auto-risk guardrails.
- **Discord control**: `!status`, `!symbols`, `!add`, `!remove`, `!enable`, `!disable`, `!kill`.
- **Sidecar logging**: events → Parquet, heartbeat → logs.

---

## 🔒 Safety
- Real API keys live only in `env`, not in Git.  
- Daily/weekly loss limits auto-halt new entries.  
- Optional flatten-on-breach for full auto-protection.  
