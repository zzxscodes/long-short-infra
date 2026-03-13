#!/bin/bash
#
# 回测机数据拉取对比启动器（Linux/macOS）
# 从实盘机器拉取数据，与官方数据对比，不一致则自动修复
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT" || exit 1
mkdir -p logs

if [ -x "$PROJECT_ROOT/quant/bin/python3" ]; then
    PYTHON="$PROJECT_ROOT/quant/bin/python3"
elif [ -x "$PROJECT_ROOT/quant/bin/python" ]; then
    PYTHON="$PROJECT_ROOT/quant/bin/python"
else
    PYTHON="python3"
fi

"$PYTHON" scripts/backtest_pull_compare.py --print-fail >> logs/backtest_pull_compare.log 2>&1
