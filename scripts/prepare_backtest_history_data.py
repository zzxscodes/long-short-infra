#!/usr/bin/env python3
"""长区间回测历史数据准备（无实盘 SSH）。

与 ``backtest_pull_compare.py`` 分工：
- **本脚本**：按 UTC 日批量从 data.binance.vision 拉取 funding / premium / aggTrade，
  用与实盘 ``data_layer`` / ``KlineAggregator`` 相同的 aggTrade→5m K 线聚合（经
  ``aggregate_vision_agg_trades_to_klines_dataframe``），写入本地目录并做官方 K 线校验。
  **候选交易对（默认 ``--symbol-source exchange``）**：**每个 UTC 日单独**用
  ``/fapi/v1/exchangeInfo`` + ``onboardDate`` 过滤出截至当日的池；若 fapi 不可达，则回退
  **data.binance.vision** 月度 5m K 线 ZIP HEAD + 种子列表（按年月缓存）。
  不再默认用「上一日落盘的 universe.csv」当候选池（``--symbol-source auto`` 才会那样），避免长区间里日复一日沿用旧列表。
- **compare 脚本**：检测「最近」实盘落盘与官方是否一致，并修复本地；可依赖 SSH。

二者共用 ``_compare_symbol_day``、Vision 拉取与 K 线聚合实现，保证口径一致。

**完整性**：默认任一日 K 线对比存在 ``fail>0`` 时进程退出码为 **2**，并写入
``{start}_{end}.history_range_summary.json``；仅排查时可加 ``--allow-partial``。

**断点续跑**：长区间任务中断后，可用 ``--resume``：若 ``{report_dir}/{YYYY-MM-DD}.history.json``
已存在且其中 ``summary.klines.fail == 0``，则跳过该日（不重删 parquet、不重拉）。
若某日曾有失败或未生成报告，仍会重新处理该日。
"""
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
from typing import Any, Dict, List, Optional, Sequence, Tuple

import aiohttp
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_PID_PATH = PROJECT_ROOT / "logs" / "prepare_backtest_history.pid"

from src.common.config import config
from src.common.utils import ensure_directory, format_symbol
from scripts.backtest_pull_compare import (
    DATA_VISION_BASE,
    PullCompareConfig,
    default_backtest_compare_output_dir,
    SymbolCompareResult,
    _load_local_kline_df,
    _load_universe_symbols,
    _month_str,
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


def _history_report_day_fully_ok(report_path: Path) -> bool:
    """
    True if per-day history JSON exists and K-line compare had zero failures for that day.
    Used by --resume to avoid re-deleting parquet and re-downloading completed days.
    """
    if not report_path.is_file():
        return False
    try:
        data = json.loads(report_path.read_text(encoding="utf-8"))
        summary = data.get("summary", {}).get("klines", {})
        if "fail" not in summary:
            return False
        return int(summary["fail"]) == 0
    except Exception:
        return False


# 进程内只拉一次 exchangeInfo，再按日历日过滤 onboardDate（history 按日换池）
_exchange_info_payload_cache: Dict[str, Any] | None = None
# fapi 不可达时改为 Vision 月度 K 线 ZIP 的 HEAD 探测 + 种子列表（按 UTC 年月缓存）
_CANDIDATE_POOL_MODE: str = "fapi"
_vision_month_candidates_cache: Dict[Tuple[int, int], List[str]] = {}


def _utc_day_end_ms(d: date) -> int:
    end = datetime(d.year, d.month, d.day, 23, 59, 59, 999000, tzinfo=timezone.utc)
    return int(end.timestamp() * 1000)


def _symbols_as_of_utc_day_from_exchange_info(symbols: List[Dict[str, Any]], as_of_day: date) -> List[str]:
    """
    用当前 exchangeInfo 快照近似「截至 as_of_day（UTC）日终」可交易的 USDT 永续池：
    onboardDate <= 该日日终，且 status==TRADING。已下架合约若不在当前 snapshot 中则无法包含（API 局限）。
    """
    cutoff = _utc_day_end_ms(as_of_day)
    out: List[str] = []
    for item in symbols:
        if item.get("contractType") != "PERPETUAL" or item.get("quoteAsset") != "USDT":
            continue
        if item.get("status") != "TRADING":
            continue
        od = item.get("onboardDate")
        if od is None:
            continue
        try:
            if int(od) > cutoff:
                continue
        except (TypeError, ValueError):
            continue
        s = format_symbol(item.get("symbol", ""))
        if s:
            out.append(s)
    return sorted(set(out))


async def _try_prime_fapi_exchange_info_quick(api_base: str, *, timeout_s: float = 12.0, attempts: int = 2) -> bool:
    """
    启动时快速探测 fapi 是否可达并缓存 exchangeInfo；失败则尽快切 Vision，避免长时间卡在长超时重试上。
    """
    global _exchange_info_payload_cache
    if _exchange_info_payload_cache is not None:
        return True
    url = f"{api_base.rstrip('/')}/fapi/v1/exchangeInfo"
    last_err: Exception | None = None
    for i in range(max(1, attempts)):
        try:
            t = aiohttp.ClientTimeout(total=timeout_s, connect=min(8.0, timeout_s * 0.6), sock_read=timeout_s)
            connector = aiohttp.TCPConnector(limit=4, ttl_dns_cache=300, enable_cleanup_closed=True)
            async with aiohttp.ClientSession(
                timeout=t,
                connector=connector,
                headers={"User-Agent": "history-prepare/1.0"},
            ) as session:
                async with session.get(url, timeout=t) as resp:
                    resp.raise_for_status()
                    _exchange_info_payload_cache = await resp.json()
                    return True
        except Exception as e:  # noqa: BLE001
            last_err = e
            if i < attempts - 1:
                await asyncio.sleep(0.4 * (i + 1))
    print(f"[history_prepare] fapi exchangeInfo quick probe failed ({last_err!r})", flush=True)
    return False


async def _fetch_exchange_info_payload_with_retries(api_base: str, retries: int, retry_delay_s: float) -> Dict[str, Any]:
    global _exchange_info_payload_cache
    if _exchange_info_payload_cache is not None:
        return _exchange_info_payload_cache
    last_err: Exception | None = None
    url = f"{api_base.rstrip('/')}/fapi/v1/exchangeInfo"
    req_timeout = aiohttp.ClientTimeout(total=180, connect=45, sock_read=150)
    for i in range(max(1, retries)):
        try:
            timeout = aiohttp.ClientTimeout(total=240, connect=45, sock_read=180)
            connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300, enable_cleanup_closed=True)
            async with aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={"User-Agent": "history-prepare/1.0"},
            ) as session:
                async with session.get(url, timeout=req_timeout) as resp:
                    resp.raise_for_status()
                    _exchange_info_payload_cache = await resp.json()
                    return _exchange_info_payload_cache
        except Exception as e:  # noqa: BLE001
            last_err = e
            if i < retries - 1:
                await asyncio.sleep(retry_delay_s * (2 ** i))
    raise RuntimeError(f"fetch exchangeInfo failed after retries: {last_err}") from last_err


def _load_candidate_seed_symbols(extra_path: str) -> List[str]:
    """
    若显式传入 ``--candidate-seed-file`` 且文件可读、非空，则**仅**使用该文件（便于缩小探测范围）。
    否则依次尝试：仓库 ``scripts/data/binance_um_futures_symbol_seed.txt``、``data/binance_um_futures_symbol_seed.txt``。
    """
    seen: set[str] = set()
    out: List[str] = []

    def _consume_file(p: Path) -> None:
        nonlocal out, seen
        text = p.read_text(encoding="utf-8")
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            s = format_symbol(line.split()[0])
            if s and s not in seen:
                seen.add(s)
                out.append(s)

    if extra_path.strip():
        p = Path(extra_path).expanduser()
        if p.is_file():
            try:
                _consume_file(p)
            except Exception:
                pass
            if out:
                return sorted(out)
        out = []
        seen = set()

    for p in (
        PROJECT_ROOT / "scripts" / "data" / "binance_um_futures_symbol_seed.txt",
        PROJECT_ROOT / "data" / "binance_um_futures_symbol_seed.txt",
    ):
        if not p.is_file():
            continue
        try:
            _consume_file(p)
        except Exception:
            continue
        if out:
            break

    if not out:
        raise RuntimeError(
            "候选种子为空：请配置 scripts/data/binance_um_futures_symbol_seed.txt 或使用 --candidate-seed-file"
        )
    return sorted(out)


async def _fetch_symbols_via_vision_monthly_heads(
    as_of_day: date,
    seed: Sequence[str],
    head_concurrent: int,
) -> List[str]:
    """
    当 fapi 不可用时：对种子列表做 HEAD
    ``.../monthly/klines/{SYM}/5m/{SYM}-5m-{YYYY-MM}.zip``。
    命中表示该月在 Vision 上存在官方 5m K 线归档（与后续拉取路径一致），按 UTC 年月缓存。
    """
    global _vision_month_candidates_cache
    key = (as_of_day.year, as_of_day.month)
    if key in _vision_month_candidates_cache:
        return _vision_month_candidates_cache[key]
    month_tag = _month_str(as_of_day)
    timeout = aiohttp.ClientTimeout(total=90, connect=25, sock_read=40)
    sem = asyncio.Semaphore(max(1, int(head_concurrent)))
    headers = {"User-Agent": "history-prepare/vision-head/1.0"}

    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:

        async def probe(sym: str) -> Optional[str]:
            s = format_symbol(sym)
            url = (
                f"{DATA_VISION_BASE}/data/futures/um/monthly/klines/{s}/5m/{s}-5m-{month_tag}.zip"
            )
            async with sem:
                try:
                    async with session.head(url, allow_redirects=True, timeout=timeout) as resp:
                        if resp.status == 200:
                            return s
                except Exception:
                    pass
            return None

        results = await asyncio.gather(*(probe(s) for s in seed))
    ok = sorted({s for s in results if s})
    _vision_month_candidates_cache[key] = ok
    print(
        f"  Vision: monthly 5m kline ZIP HEAD ok for {len(ok)}/{len(seed)} seeds (UTC month={month_tag})",
        flush=True,
    )
    if not ok:
        raise RuntimeError(
            "Vision HEAD 未命中任何种子符号：请检查 data.binance.vision 连通性，或扩充种子列表 "
            "(scripts/data/binance_um_futures_symbol_seed.txt / --candidate-seed-file)。"
        )
    return ok


async def _fetch_exchange_symbols_as_of_day(
    api_base: str,
    as_of_day: date,
    retries: int,
    retry_delay_s: float,
    candidate_seed: Sequence[str],
    vision_head_concurrent: int,
) -> List[str]:
    global _CANDIDATE_POOL_MODE
    if _CANDIDATE_POOL_MODE == "fapi":
        payload = await _fetch_exchange_info_payload_with_retries(api_base, retries, retry_delay_s)
        return _symbols_as_of_utc_day_from_exchange_info(list(payload.get("symbols", [])), as_of_day)
    return await _fetch_symbols_via_vision_monthly_heads(as_of_day, candidate_seed, vision_head_concurrent)


def _save_universe_csv(universe_dir: Path, day: date, symbols: Sequence[str]) -> Path:
    p = universe_dir / day.isoformat() / "v1" / "universe.csv"
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["symbol"])
        writer.writeheader()
        for s in symbols:
            writer.writerow({"symbol": format_symbol(s)})
    return p


async def _resolve_candidate_symbols_for_day(
    *,
    args: argparse.Namespace,
    cfg: PullCompareConfig,
    uroot: Path,
    target_day: date,
    candidate_seed: Sequence[str],
    vision_head_concurrent: int,
) -> List[str]:
    """
    - ``symbol_source=universe``：仅用本地 ``<= target_day`` 的 universe.csv（无则空）。
    - ``auto``：先尝试本地；不足 ``min_candidate_symbols`` 或为空时，才用交易所 / Vision
      按 ``target_day`` 解析池。**若本地已有足够符号，不会按日刷新**（长区间 history 不推荐）。
    - ``exchange``：**每个 target_day** 都用 exchangeInfo+onboardDate 或 Vision HEAD+seed 解析候选池（history 默认）。
    """
    api_base = str(config.get("execution.live.api_base", "https://fapi.binance.com"))
    symbols: List[str] = []
    if args.symbol_source in ("universe", "auto"):
        symbols = _load_universe_symbols(target_day=target_day, universe_dir=uroot)
    if args.symbol_source == "exchange":
        symbols = await _fetch_exchange_symbols_as_of_day(
            api_base,
            target_day,
            retries=max(2, int(args.retries)),
            retry_delay_s=float(args.retry_delay_s),
            candidate_seed=candidate_seed,
            vision_head_concurrent=vision_head_concurrent,
        )
        src = "fapi onboardDate" if _CANDIDATE_POOL_MODE == "fapi" else "Vision monthly ZIP HEAD + seed"
        print(
            f"  Candidate symbols ({src}, as-of {target_day.isoformat()} UTC): {len(symbols)}",
            flush=True,
        )
    elif (len(symbols) < int(args.min_candidate_symbols)) and args.symbol_source in ("auto",):
        symbols = await _fetch_exchange_symbols_as_of_day(
            api_base,
            target_day,
            retries=max(2, int(args.retries)),
            retry_delay_s=float(args.retry_delay_s),
            candidate_seed=candidate_seed,
            vision_head_concurrent=vision_head_concurrent,
        )
        src = "fapi onboardDate" if _CANDIDATE_POOL_MODE == "fapi" else "Vision monthly ZIP HEAD + seed"
        print(
            f"  Candidate symbols ({src}, as-of {target_day.isoformat()} UTC): {len(symbols)}",
            flush=True,
        )
    if args.symbol_limit > 0:
        symbols = symbols[: args.symbol_limit]
    return symbols


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
                binance_kline_df_by_symbol[format_symbol(sym)] = df

        await asyncio.gather(*(load_kline_for_universe(s) for s in candidate_symbols))

        # 当日 universe 与候选池一致：按「交易所/Vision 解析出的候选」落盘并全量跑 funding/premium/agg/对比。
        # 官方日 K 仅填入 binance_kline_df_by_symbol；某 symbol 当日无日 K 则 remote 为空，对比可标 fail，但不从 universe 里剔除。
        day_universe = sorted({format_symbol(s) for s in candidate_symbols})
        universe_csv = _save_universe_csv(Path(cfg.local_universe_dir), day, day_universe)
        print(
            f"[{day}] universe generated: {len(day_universe)} symbols (official 5m day kline ok: "
            f"{len(binance_kline_df_by_symbol)}/{len(day_universe)}) -> {universe_csv}",
            flush=True,
        )

        # 关键：每次生成某一天的回测数据前，清掉本地同一天可能残留的文件。
        # 避免历史残留导致：即使本次网络下载失败，仍拿到旧数据参与对比/通过判断。
        local_klines_dir = Path(cfg.local_klines_dir)
        local_funding_dir = Path(cfg.local_funding_rates_dir)
        local_premium_dir = Path(cfg.local_premium_index_dir)
        for sym in day_universe:
            s = format_symbol(sym)
            (local_klines_dir / s / f"{day.isoformat()}.parquet").unlink(missing_ok=True)
            (local_funding_dir / s / f"{day.isoformat()}.parquet").unlink(missing_ok=True)
            (local_premium_dir / s / f"{day.isoformat()}.parquet").unlink(missing_ok=True)

        results = []

        async def prepare_symbol(sym: str) -> None:
            remote_df = binance_kline_df_by_symbol.get(format_symbol(sym), pd.DataFrame())
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

        await asyncio.gather(*(prepare_symbol(s) for s in day_universe))

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

    hr = str(config.get("data.backtest_history_data_root", "") or "").strip()
    if hr:
        root = Path(hr)
        local_klines_dir = args.local_klines_dir or str(root / "klines")
        local_funding_rates_dir = args.local_funding_dir or str(root / "funding_rates")
        local_premium_index_dir = args.local_premium_dir or str(root / "premium_index")
        local_universe_dir = args.local_universe_dir or str(root / "universe")
        local_trades_dir = args.trades_dir or str(root / "trades")
    else:
        local_klines_dir = args.local_klines_dir or str(config.get("data.klines_directory", "data/klines"))
        local_funding_rates_dir = args.local_funding_dir or str(config.get("data.funding_rates_directory", "data/funding_rates"))
        local_premium_index_dir = args.local_premium_dir or str(config.get("data.premium_index_directory", "data/premium_index"))
        local_universe_dir = args.local_universe_dir or str(config.get("data.universe_directory", "data/universe"))
        local_trades_dir = args.trades_dir or str(config.get("data.trades_directory", "data/trades"))

    cfg = PullCompareConfig(
        live_host=str(config.get("data.backtest_pull_live_host", "")),
        live_user=str(config.get("data.backtest_pull_live_user", "")),
        live_universe_dir=str(config.get("data.backtest_pull_live_universe_dir", "")),
        local_klines_dir=local_klines_dir,
        local_funding_rates_dir=local_funding_rates_dir,
        local_premium_index_dir=local_premium_index_dir,
        local_universe_dir=local_universe_dir,
        local_trades_dir=local_trades_dir,
        max_concurrent=int(args.max_concurrent),
        retries=int(args.retries),
        retry_delay_s=float(args.retry_delay_s),
    )

    report_dir = Path(args.output_dir or default_backtest_compare_output_dir())
    uroot = Path(cfg.local_universe_dir)

    global _CANDIDATE_POOL_MODE, _vision_month_candidates_cache, _exchange_info_payload_cache
    _CANDIDATE_POOL_MODE = "fapi"
    _vision_month_candidates_cache.clear()
    _exchange_info_payload_cache = None

    candidate_seed = _load_candidate_seed_symbols(getattr(args, "candidate_seed_file", "") or "")
    api_base = str(config.get("execution.live.api_base", "https://fapi.binance.com"))
    print(
        f"[history_prepare] pid={os.getpid()} calendar_days={len(days)} range={start_day}..{end_day} "
        f"seed_symbols={len(candidate_seed)}",
        flush=True,
    )
    print(
        f"[history_prepare] report_dir={report_dir} local_klines_dir={cfg.local_klines_dir}",
        flush=True,
    )
    if await _try_prime_fapi_exchange_info_quick(api_base, timeout_s=12.0, attempts=2):
        print(
            "[history_prepare] exchangeInfo (fapi) OK — 候选池在需要时使用 onboardDate",
            flush=True,
        )
    else:
        _CANDIDATE_POOL_MODE = "vision"
        print(
            "[history_prepare] 候选池改为 data.binance.vision 月度 5m K 线 ZIP HEAD + 种子列表",
            flush=True,
        )

    vhead = int(getattr(args, "vision_head_concurrent", 32))

    # 显式 --symbols：全程固定；否则按 fapi+onboardDate 或 Vision HEAD+种子解析候选池
    explicit_symbols: List[str] | None = None
    if args.symbols:
        explicit_symbols = [format_symbol(s.strip()) for s in args.symbols.split(",") if s.strip()]
        print(f"Candidate symbols (explicit): {len(explicit_symbols)}")

    anchored_symbols: List[str] | None = None
    if explicit_symbols is None and args.anchor_universe_to_start_day:
        anchored_symbols = await _resolve_candidate_symbols_for_day(
            args=args,
            cfg=cfg,
            uroot=uroot,
            target_day=start_day,
            candidate_seed=candidate_seed,
            vision_head_concurrent=vhead,
        )
        print(
            f"Candidate symbols (anchored to start_day={start_day}): {len(anchored_symbols)}"
        )

    days_with_kline_fail: List[str] = []
    for d in days:
        day_report = report_dir / f"{d.isoformat()}.history.json"
        if args.resume and _history_report_day_fully_ok(day_report):
            print(
                f"[{d}] skip (--resume: existing {day_report.name} has klines fail=0)",
                flush=True,
            )
            continue

        if explicit_symbols is not None:
            symbols = explicit_symbols
        elif anchored_symbols is not None:
            symbols = anchored_symbols
        else:
            symbols = await _resolve_candidate_symbols_for_day(
                args=args,
                cfg=cfg,
                uroot=uroot,
                target_day=d,
                candidate_seed=candidate_seed,
                vision_head_concurrent=vhead,
            )
            print(f"[{d}] Candidate symbols: {len(symbols)}")

        out_path, ok, fail = await _prepare_one_day(
            day=d,
            cfg=cfg,
            report_dir=report_dir,
            candidate_symbols=symbols,
            max_concurrent=int(args.max_concurrent),
            persist_trades=bool(args.persist_trades),
        )
        print(f"[{d}] history report: {out_path} | ok={ok} fail={fail}")
        if fail > 0:
            days_with_kline_fail.append(d.isoformat())

    summary_path = report_dir / f"{start_day.isoformat()}_{end_day.isoformat()}.history_range_summary.json"
    range_complete = len(days_with_kline_fail) == 0
    summary_payload: Dict[str, Any] = {
        "mode": "history_prepare_range",
        "start_day": start_day.isoformat(),
        "end_day": end_day.isoformat(),
        "total_calendar_days": len(days),
        "days_with_kline_failures": days_with_kline_fail,
        "data_complete": range_complete,
    }
    summary_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(
        f"\n[Range summary] complete={range_complete} days_with_fail={len(days_with_kline_fail)} "
        f"-> {summary_path}"
    )
    if not range_complete and not args.allow_partial:
        print(
            "Exit 2: 存在 K 线校验未通过的日历日；数据完整性未满足。"
            "若需先跑通流水线再排查，可加 --allow-partial。",
            file=sys.stderr,
        )
        return 2
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
        default="exchange",
        help="exchange (default)=each UTC day fresh pool from exchangeInfo+onboardDate or Vision HEAD+seed; "
        "auto=prefer local universe (<=day) if >=min-candidate (may NOT refresh daily—avoid for long history); "
        "universe=local CSV only. No live SSH.",
    )
    parser.add_argument("--min-candidate-symbols", type=int, default=50, help="If candidates below this threshold, fallback to other source.")
    parser.add_argument(
        "--candidate-seed-file",
        default="",
        help="Optional extra seed file (one symbol per line, # comments ok); merged with scripts/data/binance_um_futures_symbol_seed.txt.",
    )
    parser.add_argument(
        "--vision-head-concurrent",
        type=int,
        default=32,
        help="When fapi is unreachable, concurrent HEAD requests to data.binance.vision monthly kline ZIPs (default 32).",
    )
    parser.add_argument(
        "--anchor-universe-to-start-day",
        action="store_true",
        help="Use one candidate pool from universe <= start_day for the entire range (legacy). "
        "Default: resolve candidates per day (same as backtest_pull_compare for that day's target_day).",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Report JSON output dir (default: data.backtest_compare_output_directory, same as backtest_pull_compare).",
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
    parser.add_argument(
        "--allow-partial",
        action="store_true",
        help="Exit 0 even if some days have kline compare failures (default: exit 2 when any day has fail>0).",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip a calendar day if report_dir/YYYY-MM-DD.history.json exists and summary.klines.fail is 0 "
        "(avoids re-deleting parquet and re-downloading that day). Re-runs days with fail>0 or missing report.",
    )
    args = parser.parse_args()
    return asyncio.run(_main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())

