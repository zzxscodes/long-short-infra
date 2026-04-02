#!/usr/bin/env bash
# 独立脚本：父进程 argv 不含 prepare_backtest_history_data.py，避免 pkill -f 误杀调用方 shell。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PID_FILE="$ROOT/logs/prepare_backtest_history_2022-2026.pid"
LOG="$ROOT/logs/prepare_backtest_history_2022-2026.log"
HIST="/mnt/sda/Simulation/LongShort/backtest_pull_data/history"
COMPARE_PULL="/mnt/sda/Simulation/LongShort/compare/backtest_pull"

if [[ -f "$PID_FILE" ]]; then
  old="$(tr -d '\n' < "$PID_FILE" || true)"
  if [[ -n "$old" ]] && kill -0 "$old" 2>/dev/null; then
    kill -9 "$old" 2>/dev/null || true
  fi
fi
# 仍可能存在的 worker
pkill -9 -f '[p]ython3.*scripts/prepare_backtest_history_data.py' 2>/dev/null || true
sleep 2

rm -rf "$HIST/funding_rates" "$HIST/klines" "$HIST/premium_index" "$HIST/universe" "$HIST/trades"
mkdir -p "$HIST/funding_rates" "$HIST/klines" "$HIST/premium_index" "$HIST/universe" "$HIST/trades"
find "$COMPARE_PULL" -maxdepth 1 \( -name '*.history.json' -o -name '*.history_range_summary.json' \) -delete 2>/dev/null || true
: > "$LOG"

cd "$ROOT"
nohup env PYTHONUNBUFFERED=1 ./quant/bin/python3 -u scripts/prepare_backtest_history_data.py \
  --start-day 2022-12-31 --end-day 2026-02-28 \
  --infer-universe-from-fetch --max-concurrent 20 \
  >> "$LOG" 2>&1 &
echo $! | tee "$PID_FILE" | tee "$ROOT/logs/prepare_backtest_history.pid"
sleep 6
head -18 "$LOG"
