#!/usr/bin/env python3
"""
端到端回测冒烟（Mock K 线 → 本地存储 → DataReplayEngine → BacktestExecutor）

不依赖实盘行情；用于验证 quant 环境与整套回测链路。

用法（在项目根目录）:
  ./quant/bin/python scripts/backtest_mock_smoke.py
  ./quant/bin/python scripts/backtest_mock_smoke.py --days 3 --symbols BTCUSDT ETHUSDT
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.strategy.calculator import AlphaCalculatorBase, AlphaDataView


class _SmokeMomentumCalculator(AlphaCalculatorBase):
    """与 backtest_example.DemoFactor 类似，继承 AlphaCalculatorBase 以走实盘同一接口。"""

    name = "smoke_momentum"

    def run(self, view: AlphaDataView) -> dict:
        weights: dict = {}
        for symbol in view.iter_symbols():
            bar = view.get_bar(symbol, tail=20)
            if bar is None or bar.empty or len(bar) < 5:
                weights[symbol] = 0.0
                continue
            ret = (float(bar["close"].iloc[-1]) - float(bar["close"].iloc[-5])) / float(
                bar["close"].iloc[-5]
            )
            weights[symbol] = ret * 3.0
        return weights


def main() -> int:
    parser = argparse.ArgumentParser(description="Mock 数据端到端回测冒烟")
    parser.add_argument("--days", type=int, default=5, help="生成最近 N 天模拟 K 线（默认 5）")
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=["BTCUSDT", "ETHUSDT"],
        help="交易对列表",
    )
    args = parser.parse_args()

    end = datetime.now(timezone.utc).replace(hour=23, minute=59, second=59, microsecond=0)
    start = (end - timedelta(days=max(1, args.days))).replace(hour=0, minute=0, second=0, microsecond=0)

    from src.backtest.mock_data import MockDataManager
    from src.backtest.executor import run_backtest as run_bt_executor

    print(f"[mock] Generating klines: {start.date()} ~ {end.date()} symbols={args.symbols}")
    mgr = MockDataManager()
    mgr.generate_and_save_mock_data(
        symbols=args.symbols,
        start_date=start,
        end_date=end,
        seed=42,
        interval_minutes=5,
    )

    calc = _SmokeMomentumCalculator()
    print("[mock] Running BacktestExecutor + DataReplayEngine ...")
    result = run_bt_executor(
        calculator=calc,
        start_date=start,
        end_date=end,
        symbols=args.symbols,
        initial_balance=10_000.0,
        long_count=min(2, len(args.symbols)),
        short_count=min(2, len(args.symbols)),
        verbose=False,
    )

    print("[mock] OK")
    print(f"  total_return={result.total_return:.4f}% trades={result.total_trades}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
