#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import os
import sys
from dataclasses import asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Sequence, Tuple

import aiohttp
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_PID_PATH = PROJECT_ROOT / "logs" / "prepare_backtest_history.pid"

from src.common.config import config
from src.common.utils import ensure_directory, format_symbol
from scripts.backtest_pull_compare import (
    DEFAULT_BACKTEST_COMPARE_OUTPUT_DIR,
    PullCompareConfig,
    SymbolCompareResult,
    _load_local_kline_df,
    _load_universe_symbols,
    _aggregate_agg_trades_to_klines,
    _compare_symbol_day,
    _fetch_agg_trades_from_vision,
    _fetch_funding_history,
    _fetch_official_5m_klines_from_vision,
    _fetch_premium_history,
    _official_to_df,
    _save_funding_history,
    _save_klines_parquet,
    _save_premium_history,
    pull_universe_from_live,
)


def _parse_day(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _iter_days(start_day: date, end_day: date) -> List[date]:
    if start_day > end_day:
        raise ValueError("start-day must be <= end-day")
    out: List[date] = []
    cur = start_day
    while cur <= end_day:
        out.append(cur)
        cur += timedelta(days=1)
    return out


async def _fetch_exchange_symbols(session: aiohttp.ClientSession, api_base: str) -> List[str]:
    url = f"{api_base}/fapi/v1/exchangeInfo"
    async with session.get(url, timeout=30) as resp:
        resp.raise_for_status()
        payload = await resp.json()
    out: List[str] = []
    for item in payload.get("symbols", []):
        if (
            item.get("status") == "TRADING"
            and item.get("quoteAsset") == "USDT"
            and item.get("contractType") == "PERPETUAL"
        ):
            out.append(format_symbol(item.get("symbol", "")))
    return sorted([s for s in out if s])


async def _fetch_exchange_symbols_with_retries(api_base: str, retries: int, retry_delay_s: float) -> List[str]:
    last_err: Exception | None = None
    for i in range(max(1, retries)):
        try:
            timeout = aiohttp.ClientTimeout(total=60)
            async with aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": "history-prepare/1.0"}) as session:
                return await _fetch_exchange_symbols(session, api_base)
        except Exception as e:  # noqa: BLE001
            last_err = e
            if i < retries - 1:
                await asyncio.sleep(retry_delay_s * (2 ** i))
    raise RuntimeError(f"fetch exchange symbols failed after retries: {last_err}") from last_err


def _save_universe_csv(universe_dir: Path, day: date, symbols: Sequence[str]) -> Path:
    p = universe_dir / day.isoformat() / "v1" / "universe.csv"
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["symbol"])
        writer.writeheader()
        for s in symbols:
            writer.writerow({"symbol": format_symbol(s)})
    return p


def _load_best_local_universe_symbols(universe_base: Path | None = None) -> List[str]:
    base = universe_base or Path(config.get("data.universe_directory", "data/universe"))
    best: List[str] = []
    if not base.exists():
        return best
    for p in base.glob("*/v1/universe.csv"):
        try:
            with p.open("r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                syms = sorted({format_symbol(row.get("symbol", "")) for row in reader if row.get("symbol")})
            syms = [s for s in syms if s]
            if len(syms) > len(best):
                best = syms
        except Exception:
            continue
    return best


def _save_trades_parquet(trades_dir: Path, symbol: str, day: date, trades: List[Dict]) -> Path:
    p = trades_dir / format_symbol(symbol) / f"{day.isoformat()}.parquet"
    p.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(trades).to_parquet(p, index=False)
    return p


async def _prepare_one_day(
    *,
    day: date,
    cfg: PullCompareConfig,
    report_dir: Path,
    candidate_symbols: Sequence[str],
    max_concurrent: int,
    persist_trades: bool,
) -> Tuple[Path, int, int]:
    sem = asyncio.Semaphore(max(1, max_concurrent))
    timeout = aiohttp.ClientTimeout(total=90)
    headers = {"User-Agent": "history-prepare/1.0"}

    universe_symbols: List[str] = []
    binance_kline_df_by_symbol: Dict[str, pd.DataFrame] = {}

    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        async def load_kline_for_universe(sym: str) -> None:
            async with sem:
                rows = await _fetch_official_5m_klines_from_vision(
                    session,
                    symbol=sym,
                    day=day,
                    timeout_s=60.0,
                    retries=max(2, int(cfg.retries)),
                    retry_delay_s=float(cfg.retry_delay_s),
                )
            df = _official_to_df(sym, rows)
            if not df.empty:
                universe_symbols.append(sym)
                binance_kline_df_by_symbol[sym] = df

        await asyncio.gather(*(load_kline_for_universe(s) for s in candidate_symbols))

        universe_symbols = sorted(set(universe_symbols))
        universe_csv = _save_universe_csv(Path(cfg.local_universe_dir), day, universe_symbols)
        print(f"[{day}] universe generated: {len(universe_symbols)} symbols -> {universe_csv}")

        # 关键：每次生成某一天的回测数据前，清掉本地同一天可能残留的文件。
        # 避免历史残留导致：即使本次网络下载失败，仍拿到旧数据参与对比/通过判断。
        local_klines_dir = Path(cfg.local_klines_dir)
        local_funding_dir = Path(cfg.local_funding_rates_dir)
        local_premium_dir = Path(cfg.local_premium_index_dir)
        for sym in universe_symbols:
            s = format_symbol(sym)
            (local_klines_dir / s / f"{day.isoformat()}.parquet").unlink(missing_ok=True)
            (local_funding_dir / s / f"{day.isoformat()}.parquet").unlink(missing_ok=True)
            (local_premium_dir / s / f"{day.isoformat()}.parquet").unlink(missing_ok=True)

        results = []

        async def prepare_symbol(sym: str) -> None:
            remote_df = binance_kline_df_by_symbol.get(sym, pd.DataFrame())
            try:
                async with sem:
                    funding_df = await _fetch_funding_history(
                        session,
                        sym,
                        day,
                        retries=max(2, int(cfg.retries)),
                        retry_delay_s=float(cfg.retry_delay_s),
                    )
                    premium_df = await _fetch_premium_history(
                        session,
                        sym,
                        day,
                        retries=max(2, int(cfg.retries)),
                        retry_delay_s=float(cfg.retry_delay_s),
                    )
                    trades = await _fetch_agg_trades_from_vision(
                        session,
                        symbol=sym,
                        day=day,
                        timeout_s=60.0,
                        retries=max(2, int(cfg.retries)),
                        retry_delay_s=float(cfg.retry_delay_s),
                    )

                if not funding_df.empty:
                    _save_funding_history(sym, day, funding_df, Path(cfg.local_funding_rates_dir))
                if not premium_df.empty:
                    _save_premium_history(sym, day, premium_df, Path(cfg.local_premium_index_dir))
                if trades:
                    if persist_trades:
                        _save_trades_parquet(Path(cfg.local_trades_dir), sym, day, trades)
                    own_df = _aggregate_agg_trades_to_klines(sym, trades)
                    if not own_df.empty:
                        _save_klines_parquet(sym, own_df, day, Path(cfg.local_klines_dir))

                # 与线上一致：用同一套 loader，保证含 open_time_ms 等列（空则返回带列名的空表）
                local_df = _load_local_kline_df(sym, day, Path(cfg.local_klines_dir))
                result = _compare_symbol_day(
                    symbol=sym,
                    day=day,
                    local_df=local_df,
                    remote_df=remote_df,
                    cfg=cfg,
                )
                results.append(result)
            except Exception as e:  # noqa: BLE001
                # 任何单 symbol 异常都记失败并继续，避免整日任务被中断。
                print(f"[{day}] {sym} failed: {e}", file=sys.stderr)
                binance_rows = int(len(remote_df))
                examples: List[Dict[str, Any]] = [{"error": str(e)}]
                results.append(
                    SymbolCompareResult(
                        symbol=format_symbol(sym),
                        day=str(day),
                        ok=False,
                        live_rows=0,
                        binance_history_rows=binance_rows,
                        missing_local=binance_rows,
                        missing_remote=0,
                        mismatched_rows=0,
                        mismatch_ratio=1.0 if binance_rows > 0 else 0.0,
                        max_ref_diff={},
                        examples=examples,
                    )
                )

        await asyncio.gather(*(prepare_symbol(s) for s in universe_symbols))

    results = sorted(results, key=lambda x: x.symbol)
    ok = sum(1 for r in results if r.ok)
    fail = len(results) - ok
    ensure_directory(str(report_dir))
    out_path = report_dir / f"{day.isoformat()}.history.json"
    payload = {
        "day": day.isoformat(),
        "mode": "history_prepare",
        "summary": {"klines": {"ok": ok, "fail": fail, "total": len(results)}},
        "klines_results": [asdict(r) for r in results],
    }
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return out_path, ok, fail


async def _main_async(args: argparse.Namespace) -> int:
    start_day = _parse_day(args.start_day)
    end_day = _parse_day(args.end_day)
    days = _iter_days(start_day, end_day)

    # 写入 PID（cron/nohup 都适用），便于排查/停止任务
    try:
        DEFAULT_PID_PATH.parent.mkdir(parents=True, exist_ok=True)
        DEFAULT_PID_PATH.write_text(f"{os.getpid()}\n", encoding="utf-8")
    except Exception:
        pass

    cfg = PullCompareConfig(
        live_host=str(config.get("data.backtest_pull_live_host", "")),
        live_user=str(config.get("data.backtest_pull_live_user", "")),
        live_universe_dir=str(config.get("data.backtest_pull_live_universe_dir", "")),
        local_klines_dir=args.local_klines_dir or str(config.get("data.klines_directory", "data/klines")),
        local_funding_rates_dir=args.local_funding_dir or str(config.get("data.funding_rates_directory", "data/funding_rates")),
        local_premium_index_dir=args.local_premium_dir or str(config.get("data.premium_index_directory", "data/premium_index")),
        local_universe_dir=args.local_universe_dir or str(config.get("data.universe_directory", "data/universe")),
        local_trades_dir=args.trades_dir or str(config.get("data.trades_directory", "data/trades")),
        max_concurrent=int(args.max_concurrent),
        retries=int(args.retries),
        retry_delay_s=float(args.retry_delay_s),
    )

    report_dir = Path(args.output_dir or DEFAULT_BACKTEST_COMPARE_OUTPUT_DIR)
    uroot = Path(cfg.local_universe_dir)

    symbols: List[str] = []
    if args.symbols:
        symbols = [format_symbol(s.strip()) for s in args.symbols.split(",") if s.strip()]
    if args.symbol_source in ("universe", "auto"):
        # 与 backtest_pull_compare 主流程一致：先本地 <= start_day，没有再 SSH 拉实盘 universe
        symbols = symbols or _load_universe_symbols(target_day=start_day, universe_dir=uroot)
        if not symbols and cfg.live_universe_dir:
            print(
                f"\n[Step 0] Local universe not found (<= {start_day}), pulling from live machine..."
            )
            try:
                if pull_universe_from_live(cfg, target_day=start_day):
                    symbols = _load_universe_symbols(target_day=start_day, universe_dir=uroot)
            except Exception:
                pass
        # 实盘若仅有较新日期目录，<= start_day 会拉不到；再尝试拉最新一日作候选池（与旧逻辑兼容）
        if len(symbols) < int(args.min_candidate_symbols):
            try:
                if pull_universe_from_live(cfg, target_day=None):
                    symbols = _load_universe_symbols(target_day=None, universe_dir=uroot)
            except Exception:
                pass
        if len(symbols) < int(args.min_candidate_symbols):
            best = _load_best_local_universe_symbols(uroot)
            if len(best) > len(symbols):
                symbols = best
    if (len(symbols) < int(args.min_candidate_symbols)) and args.symbol_source in ("exchange", "auto"):
        symbols = await _fetch_exchange_symbols_with_retries(
            str(config.get("execution.live.api_base", "https://fapi.binance.com")),
            retries=max(2, int(args.retries)),
            retry_delay_s=float(args.retry_delay_s),
        )
    if args.symbol_limit > 0:
        symbols = symbols[: args.symbol_limit]
    print(f"Candidate symbols: {len(symbols)}")

    for d in days:
        out_path, ok, fail = await _prepare_one_day(
            day=d,
            cfg=cfg,
            report_dir=report_dir,
            candidate_symbols=symbols,
            max_concurrent=int(args.max_concurrent),
            persist_trades=bool(args.persist_trades),
        )
        print(f"[{d}] history report: {out_path} | ok={ok} fail={fail}")

    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Prepare backtest history data by day range: universe/aggtrades/funding/premium/rebuilt klines/history report."
    )
    parser.add_argument("--start-day", required=True, help="Start day (UTC), YYYY-MM-DD.")
    parser.add_argument("--end-day", required=True, help="End day (UTC), YYYY-MM-DD.")
    parser.add_argument("--max-concurrent", type=int, default=8)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--retry-delay-s", type=float, default=0.8)
    parser.add_argument("--symbol-limit", type=int, default=0, help="For quick tests. 0 means no limit.")
    parser.add_argument("--symbols", default="", help="Optional comma-separated symbols to override symbol-source.")
    parser.add_argument(
        "--symbol-source",
        choices=["auto", "universe", "exchange"],
        default="auto",
        help="Candidate symbol source. auto=prefer local universe, fallback exchangeInfo.",
    )
    parser.add_argument("--min-candidate-symbols", type=int, default=50, help="If candidates below this threshold, fallback to other source.")
    parser.add_argument(
        "--output-dir",
        default=DEFAULT_BACKTEST_COMPARE_OUTPUT_DIR,
        help="Report output dir (same default as backtest_pull_compare.py).",
    )
    parser.add_argument("--local-klines-dir", default="")
    parser.add_argument("--local-funding-dir", default="")
    parser.add_argument("--local-premium-dir", default="")
    parser.add_argument("--local-universe-dir", default="")
    parser.add_argument(
        "--trades-dir",
        default="",
        help="AggTrade parquet root (default: data.trades_directory from config).",
    )
    parser.add_argument(
        "--persist-trades",
        action="store_true",
        help="Persist aggTrade parquet files for history run (default: false).",
    )
    args = parser.parse_args()
    return asyncio.run(_main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())

