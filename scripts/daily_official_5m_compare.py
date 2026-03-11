#!/usr/bin/env python3
"""
每日官方 5m K 线对比与同步

5. 下载 Binance 官方 5m K 线，与自聚合 K 线对比
6. 通过则同步到回测服务器；不通过则重新拉 aggTrade、重聚合、再同步

注意：脚本对比的是「过去某一天」的历史数据，默认取昨天（UTC），确保该日已结束且 data.binance.vision 通常已发布。
若本地尚未有该日的自聚合 K 线（未回填），会出现大量 missing_local，属正常；可先回填或对已有数据的日期用 --day 指定再对比。

建议 cron：严格 UTC+0 10:30（30 10 * * *，配合 CRON_TZ=UTC），data.binance.vision 当日文件已可用
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.config import config
from src.common.utils import ensure_directory, format_symbol
from src.data.daily_official_compare import (
    DailyCompareConfig,
    compare_official_vs_local_5m,
    run_daily_compare_and_sync,
)


def _parse_day(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _default_day_utc() -> date:
    now = datetime.now(timezone.utc)
    return (now - timedelta(days=1)).date()


def _load_universe_symbols() -> list:
    import csv
    universe_dir = Path(config.get("data.universe_directory", "data/universe"))
    # 1) universe.json（兼容旧配置）
    candidate_json = universe_dir / "universe.json"
    if candidate_json.exists():
        try:
            data = json.loads(candidate_json.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                syms = data.get("symbols") or data.get("universe") or []
            else:
                syms = data
            if isinstance(syms, list):
                return [format_symbol(s) for s in syms if s]
        except Exception:
            pass
    # 2) universe.csv 在目录根
    candidate_csv = universe_dir / "universe.csv"
    if candidate_csv.exists():
        try:
            with open(candidate_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                syms = [format_symbol(row.get("symbol", "")) for row in reader if row.get("symbol")]
            return [s for s in syms if s]
        except Exception:
            pass
    # 3) 按日期目录查找最新 universe.csv：data/universe/YYYY-MM-DD/v1/universe.csv
    try:
        date_dirs = sorted(
            [d for d in universe_dir.iterdir() if d.is_dir() and len(d.name) == 10 and d.name[4] == "-" and d.name[7] == "-"],
            reverse=True,
        )
        for date_dir in date_dirs:
            csv_path = date_dir / "v1" / "universe.csv"
            if csv_path.exists():
                with open(csv_path, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    syms = [format_symbol(row.get("symbol", "")) for row in reader if row.get("symbol")]
                out = [s for s in syms if s]
                if out:
                    return out
    except Exception:
        pass
    return []


async def _main_async(args: argparse.Namespace) -> int:
    day = _parse_day(args.day) if args.day else _default_day_utc()

    if args.symbols:
        symbols = [format_symbol(s.strip()) for s in args.symbols.split(",") if s.strip()]
    else:
        symbols = _load_universe_symbols()

    if not symbols:
        print("No symbols provided and universe (universe.json / universe.csv) not found/empty.", file=sys.stderr)
        return 1

    cfg = DailyCompareConfig(
        api_base=args.api_base or str(config.get("execution.live.api_base", "https://fapi.binance.com")),
        max_concurrent=int(args.max_concurrent),
        use_data_vision=not getattr(args, "use_rest", False),
    )

    if args.compare_only:
        results = await compare_official_vs_local_5m(day=day, symbols=symbols, cfg=cfg)
        ok = sum(1 for r in results if r.ok)
        fail = len(results) - ok
        out_dir = Path(args.output_dir or "data/compare/official_5m")
        ensure_directory(str(out_dir))
        out_path = out_dir / f"{day.isoformat()}.json"
        from dataclasses import asdict
        payload = {
            "day": day.isoformat(),
            "api_base": cfg.api_base,
            "symbols": symbols,
            "summary": {"ok": ok, "fail": fail, "total": len(results)},
            "results": [asdict(r) for r in results],
        }
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"day={day.isoformat()} total={len(results)} ok={ok} fail={fail}")
        print(f"saved: {out_path}")
        if fail and args.print_fail:
            for r in results:
                if not r.ok:
                    print(f"  - {r.symbol}: missing_local={r.missing_local} missing_remote={r.missing_remote} mismatched={r.mismatched_rows}")
        if fail:
            missing_local_count = sum(1 for r in results if not r.ok and r.missing_local > 0)
            if missing_local_count >= fail * 0.8:
                print("Tip: Most failures have missing_local — ensure local klines exist for this day (backfill) or use --day for a day you have.", file=sys.stderr)
        return 0 if fail == 0 else 2

    # 完整流程：对比 + 失败重聚合 + 同步（含 klines + funding_rates + premium_index）
    all_ok, results = await run_daily_compare_and_sync(
        day=day,
        symbols=symbols,
        backtest_dest=args.backtest_dest or None,
        backtest_funding_rates_dest=args.backtest_funding_dest or None,
        backtest_premium_index_dest=args.backtest_premium_dest or None,
        cfg=cfg,
        output_dir=Path(args.output_dir or "data/compare/official_5m"),
    )
    ok = sum(1 for r in results if r.ok)
    fail = len(results) - ok
    print(f"day={day.isoformat()} total={len(results)} ok={ok} fail={fail} sync_dest={args.backtest_dest or 'data/backtest_klines'}")
    if fail and args.print_fail:
        for r in results:
            if not r.ok:
                print(f"  - {r.symbol}: missing_local={r.missing_local} missing_remote={r.missing_remote} mismatched={r.mismatched_rows}")
    if fail:
        missing_local_count = sum(1 for r in results if not r.ok and r.missing_local > 0)
        if missing_local_count >= fail * 0.8:
            print("Tip: Most failures have missing_local — ensure local klines exist for this day (backfill) or use --day for a day you have.", file=sys.stderr)
    return 0 if all_ok else 2


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Daily compare official 5m klines vs local, sync to backtest. Cron: UTC 10:30 (30 10 * * *)."
    )
    parser.add_argument("--day", help="UTC day YYYY-MM-DD to compare (historical). Default: yesterday. Use a day you have local klines for.")
    parser.add_argument("--symbols", help="Comma-separated symbols. If omitted, use universe (universe.json or universe.csv).")
    parser.add_argument("--api-base", dest="api_base", help="Binance futures REST base.")
    parser.add_argument("--max-concurrent", type=int, default=3)
    parser.add_argument("--output-dir", default="data/compare/official_5m", help="Report output dir.")
    parser.add_argument("--backtest-dest", default="", help="Backtest klines dest (local path or user@host:/path).")
    parser.add_argument("--backtest-funding-dest", default="", help="Backtest funding_rates dest (same layout as klines, no compare). Default from config.")
    parser.add_argument("--backtest-premium-dest", default="", help="Backtest premium_index dest (same layout as klines, no compare). Default from config.")
    parser.add_argument("--compare-only", action="store_true", help="Only compare, no re-aggregate or sync.")
    parser.add_argument("--print-fail", action="store_true", help="Print failed symbols and examples.")
    parser.add_argument("--use-rest", action="store_true", help="Use fapi REST (default: data.binance.vision to avoid rate limit conflict).")
    args = parser.parse_args()
    return asyncio.run(_main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
