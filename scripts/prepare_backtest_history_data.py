#!/usr/bin/env python3
"""长区间回测历史数据准备（无实盘 SSH）。

**Universe 口径（本脚本默认）**：Binance **U 本位** = **USDT-M（UM）永续合约** 的 **全部 symbol**
（``/fapi/v1/exchangeInfo``：`contractType=PERPETUAL` 且 ``quoteAsset=USDT`` 且 ``status=TRADING``，
再按 ``onboardDate`` 截断至当日 UTC 日终）。**不含**币本位交割（dapi）、现货、UM 交割线（quarterly 等）。
Vision 仅使用 ``data/futures/um/``（官方 UM 数据前缀），与上述 U 本位定义一致。

与 ``backtest_pull_compare.py`` 分工：
- **本脚本**：按 UTC 日批量从 data.binance.vision 拉取 funding / premium / aggTrade，
  用与实盘 ``data_layer`` / ``KlineAggregator`` 相同的 aggTrade→5m K 线聚合（经
  ``aggregate_vision_agg_trades_to_klines_dataframe``）写入本地目录；agg 为空时按官方 5m 时间轴
  写无成交 K 线（与实盘缺窗补线一致），再做官方 K 线校验。

  **交易对来源（``--symbol-source exchange``，默认）**：**每个 UTC 日**单独解析 **U 本位永续全量**——
  优先 ``/fapi/v1/exchangeInfo`` + ``onboardDate``；若 fapi 不可达，则从 **data.binance.vision**
  S3 列举 ``data/futures/um/daily/klines/`` 下目录名，再对该列表做月度 5m ZIP HEAD 过滤（按年月缓存）。
  不依赖任何本地种子文件。

**倒推 universe（``--infer-universe-from-fetch``）**：对每个 UTC 日按上述方式得到 **U 本位永续** 枚举，
  拉 Vision 官方 5m，**非空**则计入当日 universe；忽略 ``--symbol-source``。可选 ``--symbols`` /
  ``--symbol-limit`` 缩小范围。

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
import re
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

# 日志与文档统一用语：U 本位 = Binance USDT-M（UM）永续；与 fapi PERPETUAL+USDT、Vision ``futures/um`` 对齐。
UM_USDT_PERPETUAL_LABEL = "U本位(USDT-M)永续"

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
    _build_no_trade_klines_aligned_to_official,
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


# 进程内只拉一次 exchangeInfo，再按日历日过滤 onboardDate（U 本位永续按日换池）
_exchange_info_payload_cache: Dict[str, Any] | None = None
# fapi 不可达时：Vision S3 列举 futures/um + 月度 K 线 ZIP HEAD（按 UTC 年月缓存）
_SYMBOL_POOL_MODE: str = "fapi"
_vision_month_probe_cache: Dict[Tuple[int, int], List[str]] = {}
_vision_s3_um_symbols_cache: Optional[List[str]] = None

# AWS 要求该桶用虚拟主机名访问；部分环境对该主机校验证书会失败，列举请求单独 ssl=False（仅 ListObjects）
VISION_S3_LIST_ENDPOINT = "https://data.binance.vision.s3.amazonaws.com/"
VISION_S3_DAILY_KLINES_PREFIX = "data/futures/um/daily/klines/"


def _utc_day_end_ms(d: date) -> int:
    end = datetime(d.year, d.month, d.day, 23, 59, 59, 999000, tzinfo=timezone.utc)
    return int(end.timestamp() * 1000)


def _symbols_as_of_utc_day_from_exchange_info(symbols: List[Dict[str, Any]], as_of_day: date) -> List[str]:
    """
    U 本位（USDT-M）**永续**全量：``/fapi/v1/exchangeInfo`` 中 ``contractType==PERPETUAL``、
    ``quoteAsset==USDT``、``status==TRADING``，且 ``onboardDate`` <= as_of_day（UTC）日终。
    不含币本位(dapi)、现货、UM 交割合约。已下架合约若不在当前 snapshot 中则无法包含（API 局限）。
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


async def _list_um_kline_symbols_from_vision_s3(session: aiohttp.ClientSession) -> List[str]:
    """
    通过 Vision 桶（S3 ListObjectsV2）列举 ``data/futures/um/daily/klines/{SYMBOL}/`` —— 官方 **UM（U 本位）**
    K 线路径下的 symbol 目录名，与 Binance ``futures/um`` 定义一致（相对币本位 ``cm`` / 现货）。
    无需 fapi、无需本地文件。结果进程内缓存。
    """
    global _vision_s3_um_symbols_cache
    if _vision_s3_um_symbols_cache is not None:
        return _vision_s3_um_symbols_cache
    symbols: set[str] = set()
    token: Optional[str] = None
    timeout = aiohttp.ClientTimeout(total=240, connect=30, sock_read=180)
    headers = {"User-Agent": "history-prepare/vision-s3-list/1.0"}
    while True:
        params: Dict[str, Any] = {
            "list-type": "2",
            "prefix": VISION_S3_DAILY_KLINES_PREFIX,
            "delimiter": "/",
        }
        if token:
            params["continuation-token"] = token
        # ssl=False：避免 TLS 对 data.binance.vision.s3.amazonaws.com 主机名/企业代理与证书不一致
        async with session.get(
            VISION_S3_LIST_ENDPOINT,
            params=params,
            headers=headers,
            timeout=timeout,
            ssl=False,
        ) as resp:
            text = await resp.text()
            if resp.status != 200:
                raise RuntimeError(
                    f"Vision S3 列举失败 HTTP {resp.status}: {text[:800]}"
                )
        for m in re.finditer(
            re.escape(VISION_S3_DAILY_KLINES_PREFIX) + r"([^<]+)/</Prefix>",
            text,
        ):
            sym = format_symbol(m.group(1).strip())
            if sym:
                symbols.add(sym)
        if "<IsTruncated>true</IsTruncated>" in text:
            m = re.search(r"<NextContinuationToken>([^<]+)</NextContinuationToken>", text)
            if m:
                token = m.group(1)
                continue
        break
    if not symbols:
        raise RuntimeError(
            f"Vision S3 列举 {UM_USDT_PERPETUAL_LABEL}（data/futures/um/daily/klines）为空；请检查 "
            "data.binance.vision S3 桶列举连通性（虚拟主机 endpoint）。"
        )
    _vision_s3_um_symbols_cache = sorted(symbols)
    return _vision_s3_um_symbols_cache


async def _fetch_symbols_via_vision_monthly_heads(
    as_of_day: date,
    probe_symbols: Sequence[str],
    head_concurrent: int,
) -> List[str]:
    """
    当 fapi 不可用时：对 **U 本位(USDT-M) 永续** 枚举列表做 HEAD
    ``.../monthly/klines/{SYM}/5m/{SYM}-5m-{YYYY-MM}.zip``（路径仍为 ``futures/um``）。
    命中表示该月在 Vision 上存在官方 5m K 线归档，按 UTC 年月缓存。
    """
    global _vision_month_probe_cache
    key = (as_of_day.year, as_of_day.month)
    if key in _vision_month_probe_cache:
        return _vision_month_probe_cache[key]
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

        results = await asyncio.gather(*(probe(s) for s in probe_symbols))
    ok = sorted({s for s in results if s})
    _vision_month_probe_cache[key] = ok
    n = len(probe_symbols)
    print(
        f"  [{UM_USDT_PERPETUAL_LABEL}] Vision monthly 5m ZIP HEAD: {len(ok)}/{n} (UTC month={month_tag})",
        flush=True,
    )
    if not ok:
        raise RuntimeError(
            f"Vision HEAD 未命中任何 {UM_USDT_PERPETUAL_LABEL} 符号：请检查 data.binance.vision 连通性或该月归档。"
        )
    return ok


async def _fetch_exchange_symbols_as_of_day(
    api_base: str,
    as_of_day: date,
    retries: int,
    retry_delay_s: float,
    vision_head_concurrent: int,
) -> List[str]:
    global _SYMBOL_POOL_MODE
    if _SYMBOL_POOL_MODE == "fapi":
        payload = await _fetch_exchange_info_payload_with_retries(api_base, retries, retry_delay_s)
        return _symbols_as_of_utc_day_from_exchange_info(list(payload.get("symbols", [])), as_of_day)
    timeout = aiohttp.ClientTimeout(total=240, connect=30, sock_read=180)
    async with aiohttp.ClientSession(
        timeout=timeout,
        headers={"User-Agent": "history-prepare/vision-s3-list/1.0"},
    ) as session:
        full = await _list_um_kline_symbols_from_vision_s3(session)
    return await _fetch_symbols_via_vision_monthly_heads(
        as_of_day, full, vision_head_concurrent
    )


def _save_universe_csv(universe_dir: Path, day: date, symbols: Sequence[str]) -> Path:
    p = universe_dir / day.isoformat() / "v1" / "universe.csv"
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["symbol"])
        writer.writeheader()
        for s in symbols:
            writer.writerow({"symbol": format_symbol(s)})
    return p


async def _resolve_day_symbols(
    *,
    args: argparse.Namespace,
    cfg: PullCompareConfig,
    uroot: Path,
    target_day: date,
    vision_head_concurrent: int,
) -> List[str]:
    """
    - ``symbol_source=universe``：仅用本地 ``<= target_day`` 的 universe.csv（无则空；应已为 U 本位永续）。
    - ``auto``：先尝试本地；不足 ``min_universe_symbols`` 或为空时，才用 fapi / Vision S3+HEAD
      解析 **U 本位(USDT-M)永续全量**。**若本地已有足够符号，不会按日刷新**（长区间 history 不推荐）。
    - ``exchange``：**每个 target_day** 都用 fapi+onboardDate 或 Vision ``um``+HEAD（history 默认）。
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
            vision_head_concurrent=vision_head_concurrent,
        )
        src = "fapi+onboardDate" if _SYMBOL_POOL_MODE == "fapi" else "Vision S3(um)+monthly HEAD"
        print(
            f"  [{UM_USDT_PERPETUAL_LABEL}] 枚举 {len(symbols)} | {src} | as-of {target_day.isoformat()} UTC",
            flush=True,
        )
    elif (len(symbols) < int(args.min_universe_symbols)) and args.symbol_source in ("auto",):
        symbols = await _fetch_exchange_symbols_as_of_day(
            api_base,
            target_day,
            retries=max(2, int(args.retries)),
            retry_delay_s=float(args.retry_delay_s),
            vision_head_concurrent=vision_head_concurrent,
        )
        src = "fapi+onboardDate" if _SYMBOL_POOL_MODE == "fapi" else "Vision S3(um)+monthly HEAD"
        print(
            f"  [{UM_USDT_PERPETUAL_LABEL}] 枚举 {len(symbols)} | {src} | as-of {target_day.isoformat()} UTC",
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
    probe_symbols: Sequence[str],
    max_concurrent: int,
    persist_trades: bool,
    infer_universe_from_fetch: bool = False,
) -> Tuple[Path, int, int]:
    sem = asyncio.Semaphore(max(1, max_concurrent))
    timeout = aiohttp.ClientTimeout(total=90)
    headers = {"User-Agent": "history-prepare/1.0"}

    binance_kline_df_by_symbol: Dict[str, pd.DataFrame] = {}
    enum_list = [format_symbol(s) for s in probe_symbols]
    total_enum = len(enum_list)

    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        kline_done = 0
        kline_lock = asyncio.Lock()

        async def load_kline_for_universe(sym: str) -> None:
            nonlocal kline_done
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
            async with kline_lock:
                kline_done += 1
                if infer_universe_from_fetch and (
                    kline_done % 40 == 0 or kline_done == total_enum
                ):
                    print(
                        f"[{day}] {UM_USDT_PERPETUAL_LABEL} | official 5m(Vision) progress: "
                        f"{kline_done}/{total_enum} | nonempty: {len(binance_kline_df_by_symbol)}",
                        flush=True,
                    )

        if infer_universe_from_fetch and total_enum > 0:
            print(
                f"[{day}] pulling official 5m for {total_enum} {UM_USDT_PERPETUAL_LABEL} symbols "
                f"(max_concurrent={max_concurrent})…",
                flush=True,
            )

        await asyncio.gather(*(load_kline_for_universe(s) for s in enum_list))

        if infer_universe_from_fetch:
            day_universe = sorted(binance_kline_df_by_symbol.keys())
            universe_csv = _save_universe_csv(Path(cfg.local_universe_dir), day, day_universe)
            print(
                f"[{day}] universe.csv ({UM_USDT_PERPETUAL_LABEL}) inferred from official 5m: "
                f"{len(day_universe)}/{total_enum} -> {universe_csv}",
                flush=True,
            )
            if not day_universe:
                print(
                    f"[{day}] warn: empty universe (no nonempty official 5m for {UM_USDT_PERPETUAL_LABEL})",
                    flush=True,
                )
        else:
            day_universe = sorted({format_symbol(s) for s in probe_symbols})
            universe_csv = _save_universe_csv(Path(cfg.local_universe_dir), day, day_universe)
            print(
                f"[{day}] universe.csv ({UM_USDT_PERPETUAL_LABEL}): {len(day_universe)} symbols | "
                f"official 5m ok {len(binance_kline_df_by_symbol)}/{len(day_universe)} -> {universe_csv}",
                flush=True,
            )

        # 关键：每次生成某一天的回测数据前，清掉本地同一天可能残留的文件。
        # 倒推模式对全部枚举符号清当日文件，避免未进 universe 的 symbol 残留。
        local_klines_dir = Path(cfg.local_klines_dir)
        local_funding_dir = Path(cfg.local_funding_rates_dir)
        local_premium_dir = Path(cfg.local_premium_index_dir)
        clean_syms = (
            sorted({format_symbol(s) for s in probe_symbols})
            if infer_universe_from_fetch
            else day_universe
        )
        for sym in clean_syms:
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
                own_df = pd.DataFrame()
                if trades:
                    if persist_trades:
                        _save_trades_parquet(Path(cfg.local_trades_dir), sym, day, trades)
                    own_df = _aggregate_agg_trades_to_klines(sym, trades)
                if own_df.empty and not remote_df.empty:
                    own_df = _build_no_trade_klines_aligned_to_official(sym, remote_df)
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
        "mode": "history_prepare_infer" if infer_universe_from_fetch else "history_prepare",
        "universe_definition": "binance_um_usdt_perpetual",
        "universe_note": UM_USDT_PERPETUAL_LABEL + "；fapi: PERPETUAL+USDT+TRADING+onboardDate；Vision: futures/um only",
        "universe_inference": ("official_5m_nonempty" if infer_universe_from_fetch else None),
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

    global _SYMBOL_POOL_MODE, _vision_month_probe_cache, _exchange_info_payload_cache, _vision_s3_um_symbols_cache
    _SYMBOL_POOL_MODE = "fapi"
    _vision_month_probe_cache.clear()
    _exchange_info_payload_cache = None
    _vision_s3_um_symbols_cache = None

    api_base = str(config.get("execution.live.api_base", "https://fapi.binance.com"))
    print(
        f"[history_prepare] pid={os.getpid()} calendar_days={len(days)} range={start_day}..{end_day}",
        flush=True,
    )
    print(
        f"[history_prepare] report_dir={report_dir} local_klines_dir={cfg.local_klines_dir}",
        flush=True,
    )
    if await _try_prime_fapi_exchange_info_quick(api_base, timeout_s=12.0, attempts=2):
        print(
            f"[history_prepare] fapi exchangeInfo OK — {UM_USDT_PERPETUAL_LABEL} 在需要时按 onboardDate 截断",
            flush=True,
        )
    else:
        _SYMBOL_POOL_MODE = "vision_s3"
        print(
            f"[history_prepare] fapi 不可用 — {UM_USDT_PERPETUAL_LABEL} 改为 Vision S3(um) + 月度 5m ZIP HEAD",
            flush=True,
        )

    vhead = int(getattr(args, "vision_head_concurrent", 32))

    explicit_symbols: List[str] | None = None
    if args.symbols:
        explicit_symbols = [format_symbol(s.strip()) for s in args.symbols.split(",") if s.strip()]
        print(
            f"[history_prepare] {UM_USDT_PERPETUAL_LABEL} — 显式 --symbols: {len(explicit_symbols)}",
            flush=True,
        )

    anchored_symbols: List[str] | None = None
    if (
        explicit_symbols is None
        and args.anchor_universe_to_start_day
        and not args.infer_universe_from_fetch
    ):
        anchored_symbols = await _resolve_day_symbols(
            args=args,
            cfg=cfg,
            uroot=uroot,
            target_day=start_day,
            vision_head_concurrent=vhead,
        )
        print(
            f"[history_prepare] {UM_USDT_PERPETUAL_LABEL} — 锚定 start_day={start_day}: {len(anchored_symbols)}",
            flush=True,
        )

    async def _infer_probe_symbols_for_day(d: date) -> List[str]:
        """infer：显式 --symbols；否则 fapi 按日 onboardDate 得 U 本位永续全量；否则 Vision S3(um) 全量（可 --symbol-limit）。"""
        if explicit_symbols is not None:
            return list(explicit_symbols)
        if _SYMBOL_POOL_MODE == "fapi":
            payload = await _fetch_exchange_info_payload_with_retries(
                api_base, max(2, int(args.retries)), float(args.retry_delay_s)
            )
            raw = _symbols_as_of_utc_day_from_exchange_info(
                list(payload.get("symbols", [])), d
            )
        else:
            timeout = aiohttp.ClientTimeout(total=240, connect=30, sock_read=180)
            async with aiohttp.ClientSession(
                timeout=timeout,
                headers={"User-Agent": "history-prepare/vision-s3-list/1.0"},
            ) as session:
                raw = await _list_um_kline_symbols_from_vision_s3(session)
        if args.symbol_limit > 0:
            raw = raw[: args.symbol_limit]
        return raw

    days_with_kline_fail: List[str] = []
    for d in days:
        day_report = report_dir / f"{d.isoformat()}.history.json"
        if args.resume and _history_report_day_fully_ok(day_report):
            print(
                f"[{d}] skip (--resume: existing {day_report.name} has klines fail=0)",
                flush=True,
            )
            continue

        if args.infer_universe_from_fetch:
            symbols = await _infer_probe_symbols_for_day(d)
            print(
                f"[{d}] infer-universe | {UM_USDT_PERPETUAL_LABEL} | 枚举 {len(symbols)} | "
                f"({'fapi+onboardDate' if _SYMBOL_POOL_MODE == 'fapi' else 'Vision S3(um) 目录全量'})",
                flush=True,
            )
            if not symbols:
                print(
                    f"[history_prepare] infer mode: empty symbol list for {d}",
                    file=sys.stderr,
                )
                return 2
        elif explicit_symbols is not None:
            symbols = explicit_symbols
        elif anchored_symbols is not None:
            symbols = anchored_symbols
        else:
            symbols = await _resolve_day_symbols(
                args=args,
                cfg=cfg,
                uroot=uroot,
                target_day=d,
                vision_head_concurrent=vhead,
            )
            print(f"[{d}] {UM_USDT_PERPETUAL_LABEL} | 枚举 {len(symbols)}", flush=True)

        out_path, ok, fail = await _prepare_one_day(
            day=d,
            cfg=cfg,
            report_dir=report_dir,
            probe_symbols=symbols,
            max_concurrent=int(args.max_concurrent),
            persist_trades=bool(args.persist_trades),
            infer_universe_from_fetch=bool(args.infer_universe_from_fetch),
        )
        print(f"[{d}] history report: {out_path} | ok={ok} fail={fail}")
        if fail > 0:
            days_with_kline_fail.append(d.isoformat())

    summary_path = report_dir / f"{start_day.isoformat()}_{end_day.isoformat()}.history_range_summary.json"
    range_complete = len(days_with_kline_fail) == 0
    summary_payload: Dict[str, Any] = {
        "mode": "history_prepare_range",
        "universe_definition": "binance_um_usdt_perpetual",
        "universe_note": "U本位=USDT-M(UM)永续；fapi: PERPETUAL+USDT+TRADING+onboardDate；Vision: data/futures/um only",
        "universe_policy": (
            "infer_official_5m_nonempty" if args.infer_universe_from_fetch else "symbol_source"
        ),
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
        description=(
            "Prepare backtest history by UTC day range. "
            "Default universe = Binance U本位(USDT-M)永续全量 symbol（fapi PERPETUAL+USDT 或 Vision data/futures/um）；"
            "not coin-margined (dapi) or spot."
        )
    )
    parser.add_argument("--start-day", required=True, help="Start day (UTC), YYYY-MM-DD.")
    parser.add_argument("--end-day", required=True, help="End day (UTC), YYYY-MM-DD.")
    parser.add_argument("--max-concurrent", type=int, default=8)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--retry-delay-s", type=float, default=0.8)
    parser.add_argument(
        "--symbol-limit",
        type=int,
        default=0,
        help="Cap enumeration size (quick tests). 0 = full U本位(USDT-M)永续 set for that day.",
    )
    parser.add_argument(
        "--symbols",
        default="",
        help="Optional comma-separated symbols; overrides symbol-source / infer enumeration.",
    )
    parser.add_argument(
        "--symbol-source",
        choices=["auto", "universe", "exchange"],
        default="exchange",
        help="exchange (default)=each UTC day: full U本位(USDT-M)永续 via fapi+onboardDate, or Vision S3(um)+monthly HEAD; "
        "auto=prefer local universe.csv (<=day) if >=min-universe-symbols; universe=local CSV only. No SSH.",
    )
    parser.add_argument(
        "--min-universe-symbols",
        type=int,
        default=50,
        dest="min_universe_symbols",
        help="symbol-source=auto: if local universe.csv has fewer rows than this, fall back to fapi/Vision U本位永续全量.",
    )
    parser.add_argument(
        "--vision-head-concurrent",
        type=int,
        default=32,
        help="When using Vision branch: concurrent HEAD to monthly um kline ZIPs (default 32).",
    )
    parser.add_argument(
        "--anchor-universe-to-start-day",
        action="store_true",
        help="Use one UM symbol pool from universe <= start_day for the entire range (legacy). "
        "Default: resolve per day (same as backtest_pull_compare for that day's target_day).",
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
    parser.add_argument(
        "--infer-universe-from-fetch",
        action="store_true",
        help="Each UTC day: enumerate full U本位(USDT-M)永续 set (fapi+onboardDate or Vision um S3), fetch official 5m; "
        "universe.csv = nonempty subset; ignores --symbol-source. Optional --symbols / --symbol-limit.",
    )
    args = parser.parse_args()
    return asyncio.run(_main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())

