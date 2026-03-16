# Rotations

Quantitative finance monorepo: signal generation pipeline + web dashboard.

## Repository Structure

```
rotations/
├── signals/              Signal generation pipeline
│   ├── rotations.py      Main pipeline (~8000 lines, 15 cells)
│   ├── rotations_old_outputs.py   Extracted Group B report cells
│   └── databento_test.py
├── app/
│   ├── backend/          FastAPI server
│   │   ├── main.py       REST endpoints + WebSocket streaming
│   │   └── signals_engine.py   Live signal calculation engine
│   └── frontend/         React 18 + TypeScript + Vite
│       └── src/
│           ├── App.tsx           Main orchestration
│           ├── components/
│           │   ├── TVChart.tsx        Multi-pane chart (lightweight-charts)
│           │   ├── BasketSummary.tsx  Signals/Correlation/Returns tabs
│           │   └── BacktestPanel.tsx  Backtesting UI
│           └── index.css         Solarized Light theme
├── .claude/              Agent reference files
│   ├── agents/           Agent definitions
│   ├── dependency-tree.md
│   ├── integration-map.md
│   ├── ideas-and-goals.md
│   └── logs/             Session logs
└── ecosystem.config.js   PM2 process manager config
```

## Shared Data

Both subsystems communicate through `~/Documents/Python_Outputs/Data_Storage/`:
- **Producer** (signals): writes parquet, JSON, pkl caches
- **Consumer** (app): reads those caches via FastAPI endpoints

Key shared files:
- `signals_cache_500.parquet` — Individual ticker signals
- `basket_equity_cache/{slug}_equity_ohlc.parquet` — Basket OHLC
- `basket_signals_cache/{slug}_basket_signals.parquet` — Basket signals
- `correlation_cache/within_osc_500.parquet` — Pre-computed correlations
- `top500stocks.json` — Quarterly universe
- `gics_mappings_500.json` — Sector/industry mappings
- `live_signals_500.parquet` — Intraday signals (Source='live')
- `live_basket_signals_500.parquet` — Intraday basket OHLC

## Commands

```bash
# Backend — activate venv then start server
cd app/backend
python -m venv venv                # first time only
source venv/bin/activate           # Linux/macOS
# .\venv\Scripts\Activate.ps1     # Windows (PowerShell)
pip install fastapi uvicorn pandas numpy databento python-dotenv pyarrow
python main.py                     # starts uvicorn on http://0.0.0.0:8000

# Frontend — install deps then start dev server
cd app/frontend
npm install                        # first time only
npm run dev                        # Dev server on http://localhost:5173
npm run build                      # tsc -b && vite build
npm run lint                       # eslint .

# Both via PM2
pm2 start ecosystem.config.js
```

## Environment Variables

The backend auto-loads `.env` from `app/backend/` first, falling back to the repo root `.env`.

- `DATABENTO_API_KEY` — Required for live/intraday data
- `DATABENTO_DATASET` — Default: `EQUS.MINI`
- `PYTHON_OUTPUTS_DIR` — Base path for cached data (default: `~/Documents/Python_Outputs`)

## Agent Workflow Rules

### MANDATORY: At session start
Run **session-logger** with "session start" to review carried-over goals and ideas from previous sessions. Present them to the user before beginning work.

### MANDATORY: Before ANY code edit
1. Read the **dependency tree** at `.claude/dependency-tree.md` for the target function(s)
2. Read the **integration map** at `.claude/integration-map.md` if the change could affect cross-repo data contracts
3. Present the dependency context and integration risks to the user BEFORE writing any code

### MANDATORY: After code changes are merged to main
Run these agents in the background:
1. **dependency-mapper** — Incrementally update the dependency tree and cell map for modified functions
2. **integration-tracker** — Incrementally update the cross-repo integration map
3. **session-logger** — Record what changed and why

## Agent Reference Files

These files are maintained by agents — read them for instant context:
- `.claude/dependency-tree.md` — Function dependency tree + cell map (all .py files)
- `.claude/integration-map.md` — Cross-repo data contracts
- `.claude/ideas-and-goals.md` — Persistent ideas backlog and active goals
- `.claude/logs/session_*.md` — Daily session logs

## Signal Logic Rules

- BTFD/STFR use PREVIOUS day's target, not current day's
- Entry prices must account for gap fills (open vs target)
- Weights/rankings must use data from the PRIOR period, never same period
- All cache-affecting changes require version constant bumps
- `Source` column: 'norgate' (daily rebuild) vs 'live' (intraday Databento)

## File Navigation

- `signals/rotations.py` is ~8000 lines. Check `.claude/dependency-tree.md` for cell map and line ranges FIRST. ALWAYS read the specific function before editing — never edit based on memory.
- When a function is modified, check both batch path (`signals/rotations.py`) AND live path (`app/backend/signals_engine.py` / `app/backend/main.py`) for parallel implementations.

## Performance Baselines

Baseline targets:
- correlation: <18s thematic, <16s sector, <8s industry
- breadth: <8s thematic, <8s sector, <7s industry
- TOTAL: <31s thematic, <24s sector, <14s industry

Any change exceeding baseline by >20% per step or >10% total = REGRESSION.
