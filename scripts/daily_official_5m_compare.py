#!/usr/bin/env python3

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import List

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.config import config
from src.common.utils import ensure_directory, format_symbol
from src.data.official_kline_compare import DailyCompareConfig, compare_official_vs_local_5m


def _parse_day(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _default_day_utc() -> date:
    # 默认对比“昨天”的完整 UTC 日（避免当天数据未完全落盘）
    now = datetime.now(timezone.utc)
    return (now - timedelta(days=1)).date()


def _load_universe_symbols() -> List[str]:
    universe_dir = Path(config.get("data.universe_directory", "data/universe"))
    candidate = universe_dir / "universe.json"
    if not candidate.exists():
        return []
    try:
        data = json.loads(candidate.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            syms = data.get("symbols") or data.get("universe") or []
        else:
            syms = data
        if not isinstance(syms, list):
            return []
        return [format_symbol(s) for s in syms if s]
    except Exception:
        return []


async def _main_async(args: argparse.Namespace) -> int:
    day = _parse_day(args.day) if args.day else _default_day_utc()

    symbols: List[str]
    if args.symbols:
        symbols = [format_symbol(s.strip()) for s in args.symbols.split(",") if s.strip()]
    else:
        symbols = _load_universe_symbols()

    if not symbols:
        raise SystemExit("No symbols provided and universe file not found/empty.")

    cfg = DailyCompareConfig(
        api_base=args.api_base or str(config.get("execution.live.api_base", "https://fapi.binance.com")),
        max_concurrent=int(args.max_concurrent),
    )

    results = await compare_official_vs_local_5m(day=day, symbols=symbols, cfg=cfg)
    ok = sum(1 for r in results if r.ok)
    fail = len(results) - ok

    out_dir = Path(args.output_dir or "data/compare/official_5m")
    ensure_directory(str(out_dir))
    out_path = out_dir / f"{day.isoformat()}.json"

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
    if fail and args.print_fail_examples:
        for r in results:
            if not r.ok:
                print(f"- {r.symbol}: missing_local={r.missing_local} missing_remote={r.missing_remote} mismatched={r.mismatched_rows}")
                for ex in (r.examples or [])[:2]:
                    print(f"  ex: {ex}")
    return 0 if fail == 0 else 2


def main() -> int:
    parser = argparse.ArgumentParser(description="Compare local 5m klines with Binance official 5m klines for a UTC day.")
    parser.add_argument("--day", help="UTC day to compare, format YYYY-MM-DD. Default: yesterday(UTC).")
    parser.add_argument("--symbols", help="Comma-separated symbols (e.g. BTCUSDT,ETHUSDT). If omitted, try universe.json.")
    parser.add_argument("--api-base", dest="api_base", help="Binance futures REST base (default from config).")
    parser.add_argument("--max-concurrent", type=int, default=5)
    parser.add_argument("--output-dir", default="data/compare/official_5m")
    parser.add_argument("--print-fail-examples", action="store_true")
    args = parser.parse_args()

    return asyncio.run(_main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())

