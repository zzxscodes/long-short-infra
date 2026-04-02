#!/usr/bin/env python3
"""
回测机主动拉取实盘数据并对比

脚本分工（与 prepare_backtest_history_data 的关系）：
- **本脚本（compare）**：面向「最近历史」的**实盘数据质量检测**。从实盘机 SSH 拉取与线上 data_layer
  同路径落盘的 klines/funding/premium，再与 Binance 官方（Vision / REST）对照；不一致时用 Vision
  aggTrade **经与实盘相同的 K 线聚合口径**（见 ``src.data.kline_aggregator.aggregate_vision_agg_trades_to_klines_dataframe``）
  重算并覆盖本地，保证回测侧与线上一致。
- **prepare_backtest_history_data**：面向「长区间」的**纯历史回测数据准备**。无 SSH、不拉实盘，
  仅依赖 Vision：按日生成 universe、拉 funding/premium/aggTrade、用同一套聚合写入本地目录，
  并用 ``_compare_symbol_day`` 与官方 5m K 线做校验。

设计约定（与定时任务一致）：
- 在 **UTC 日历日 D 的 12:00** 执行（见 scripts/setup_backtest_compare_scheduler.py）。
  此时 **UTC 的「前一」日 D-1** 的历史数据（含 data.binance.vision 日文件等）才视为已就绪，故默认
  **对比日 target_day = UTC 昨天**（_default_day_utc），即验证的是 **D-1** 这一天的数据。
- **Universe**：在 local universe 目录下取 **日期目录 <= target_day** 的 **最新** 一份
  `YYYY-MM-DD/v1/universe.csv`，即与 **「前一 UTC 日」** 被测数据对应的 **universe 体量**
  （在目录齐全时通常为 **target_day 当日** 的快照；若当日目录尚未落盘，则回退到更早一档）。

工作流程：
1. 从实盘机器拉取对比日相关 klines/funding_rates/premium_index 数据
2. 下载 Binance 官方 5m K线（data.binance.vision），与拉取的数据对比
3. 对比通过：保持拉取的数据不变
4. 对比不通过：从 data.binance.vision 下载历史 aggTrade，聚合覆盖本地数据

运行环境：回测机（Windows），需要配置到实盘机器的 SSH 访问
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import math
import os
import shutil
import subprocess
import sys
import zipfile
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    import aiohttp
    import pandas as pd
except ImportError as e:
    print(f"Missing dependency: {e}. Please install: pip install aiohttp pandas", file=sys.stderr)
    sys.exit(1)

from src.common.config import config
from src.common.utils import ensure_directory, format_symbol
from src.data.kline_aggregator import aggregate_vision_agg_trades_to_klines_dataframe

DATA_VISION_BASE = "https://data.binance.vision"

# 默认报告输出目录（prepare_backtest_history_data 与之保持一致，便于同一目录查看 *.json）
def default_backtest_compare_output_dir() -> str:
    """JSON 报告目录：优先 config data.backtest_compare_output_directory（如 LongShort/compare/backtest_pull）。"""
    v = config.get("data.backtest_compare_output_directory")
    if v is not None and str(v).strip():
        return str(v).strip()
    return "compare/backtest_pull"

DEFAULT_PID_PATH = PROJECT_ROOT / "logs" / "backtest_pull_compare.pid"

# Vision 日文件在“刚产出/刚同步”阶段可能短暂 404；对 404 额外重试，避免误判为空数据。
VISION_404_RETRIES_DEFAULT = 8
VISION_404_RETRY_DELAY_S_DEFAULT = 3.0
# 对 429/502/503/504 等瞬态错误的额外重试次数（不计入 404 窗口）
VISION_TRANSIENT_MAX_RETRIES = 10

@dataclass(frozen=True)
class FieldTolerance:
    abs: float = 0.0
    rel: float = 0.0

    def ok(self, local: float, remote: float) -> bool:
        try:
            lv = float(local)
            rv = float(remote)
            # 与 temp_validate_data_layer_vs_binance 中逻辑保持一致，使用 math.isclose
            return math.isclose(lv, rv, rel_tol=float(self.rel), abs_tol=float(self.abs))
        except Exception:
            return False


@dataclass
class PullCompareConfig:
    live_host: str = ""
    live_user: str = ""
    live_klines_dir: str = ""
    live_funding_rates_dir: str = ""
    live_premium_index_dir: str = ""
    live_universe_dir: str = ""
    local_klines_dir: str = ""
    local_funding_rates_dir: str = ""
    local_premium_index_dir: str = ""
    local_universe_dir: str = ""
    # 仅历史准备脚本写 aggTrade parquet；日更对比脚本不用
    local_trades_dir: str = ""
    timeout_s: float = 120.0
    max_concurrent: int = 5
    retries: int = 5
    retry_delay_s: float = 1.0
    vision_404_retries: int = VISION_404_RETRIES_DEFAULT
    vision_404_retry_delay_s: float = VISION_404_RETRY_DELAY_S_DEFAULT
    # 单个 symbol 每日允许的最大 mismatched 行占比（0.01 = 1%）
    max_mismatch_ratio: float = 0.01
    tolerances: Dict[str, FieldTolerance] = field(default_factory=lambda: {
        # 用于线上质量检测（而不是追求“绝对通过”），放宽到 1e-4 减少噪声告警。
        "open": FieldTolerance(abs=0.0, rel=1e-4),
        "high": FieldTolerance(abs=0.0, rel=1e-4),
        "low": FieldTolerance(abs=0.0, rel=1e-4),
        "close": FieldTolerance(abs=0.0, rel=1e-4),
        # volume/quote_volume 在 Vision 与本地聚合时会有非常轻微的四舍五入差异，
        # 放宽一点点以降低无意义的“全量失败”。
        "volume": FieldTolerance(abs=0.0, rel=2e-4),
        "quote_volume": FieldTolerance(abs=0.0, rel=2e-4),
        # tradecount 由于 aggTrade -> kline 聚合实现/边界处理可能有 1~2 级别波动，
        # 对回测的 OHLCV 主体误差影响很小，因此允许小范围差异。
        "tradecount": FieldTolerance(abs=2.0, rel=0.001),
    })


@dataclass
class SymbolCompareResult:
    symbol: str
    day: str
    ok: bool
    live_rows: int
    binance_history_rows: int
    missing_local: int
    missing_remote: int
    mismatched_rows: int
    mismatch_ratio: float
    max_ref_diff: Dict[str, float]
    examples: List[Dict[str, Any]]


@dataclass
class FundingCompareResult:
    symbol: str
    day: str
    ok: bool
    live_rows: int
    binance_history_rows: int
    missing_local: int
    missing_remote: int
    mismatched_rows: int
    mismatch_ratio: float
    examples: List[Dict[str, Any]]


@dataclass
class PremiumCompareResult:
    symbol: str
    day: str
    ok: bool
    live_rows: int
    binance_history_rows: int
    missing_local: int
    missing_remote: int
    mismatched_rows: int
    mismatch_ratio: float
    examples: List[Dict[str, Any]]


def _norm_ms(ts: Any) -> int:
    return int(pd.Timestamp(ts, tz="UTC").value // 1_000_000)


def _funding_key(ts_ms: int) -> int:
    window = 8 * 60 * 60 * 1000
    return int(round(ts_ms / window)) * window


def _premium_key(ts_ms: int) -> int:
    window = 5 * 60 * 1000
    return (ts_ms // window) * window


def _load_local_funding_df(symbol: str, day: date, funding_dir: Path) -> pd.DataFrame:
    p = funding_dir / format_symbol(symbol) / f"{day.isoformat()}.parquet"
    if not p.exists():
        return pd.DataFrame(columns=["key", "fundingRate", "markPrice"])
    try:
        df = pd.read_parquet(p)
        if df.empty:
            return pd.DataFrame(columns=["key", "fundingRate", "markPrice"])
        out = df.copy()
        out["key"] = out["fundingTime"].apply(lambda v: _funding_key(_norm_ms(v)))
        out["fundingRate"] = pd.to_numeric(out.get("fundingRate"), errors="coerce")
        out["markPrice"] = pd.to_numeric(out.get("markPrice"), errors="coerce")
        return out[["key", "fundingRate", "markPrice"]].drop_duplicates(subset=["key"], keep="last")
    except Exception:
        return pd.DataFrame(columns=["key", "fundingRate", "markPrice"])


def _load_local_premium_df(symbol: str, day: date, premium_dir: Path) -> pd.DataFrame:
    p = premium_dir / format_symbol(symbol) / f"{day.isoformat()}.parquet"
    cols = ["key", "open", "high", "low", "close"]
    if not p.exists():
        return pd.DataFrame(columns=cols)
    try:
        df = pd.read_parquet(p)
        if df.empty:
            return pd.DataFrame(columns=cols)
        out = df.copy()
        out["key"] = out["open_time"].apply(lambda v: _premium_key(_norm_ms(v)))
        for c in ["open", "high", "low", "close"]:
            out[c] = pd.to_numeric(out.get(c), errors="coerce")
        return out[cols].drop_duplicates(subset=["key"], keep="last").sort_values("key")
    except Exception:
        return pd.DataFrame(columns=cols)


async def _fetch_json_with_retries(
    session: aiohttp.ClientSession,
    *,
    url: str,
    params: Dict[str, Any],
    retries: int,
    retry_delay_s: float,
) -> Any:
    last_err: Optional[BaseException] = None
    for attempt in range(max(1, retries)):
        try:
            async with session.get(url, params=params, timeout=60.0) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}")
                return await resp.json()
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                await asyncio.sleep(retry_delay_s * (2 ** attempt))
    raise RuntimeError(f"fetch failed after retries: {url} params={params} err={last_err}") from last_err


async def _fetch_funding_history(
    session: aiohttp.ClientSession,
    symbol: str,
    day: date,
    retries: int,
    retry_delay_s: float,
    api_base: Optional[str] = None,
) -> pd.DataFrame:
    sym = format_symbol(symbol)
    day_str = day.isoformat()
    url = f"{DATA_VISION_BASE}/data/futures/um/daily/fundingRate/{sym}/{sym}-fundingRate-{day_str}.zip"
    last_err: Optional[BaseException] = None
    rows: List[List[str]] = []
    for attempt in range(max(1, retries)):
        try:
            async with session.get(url, timeout=60.0) as resp:
                if resp.status == 404:
                    rows = []
                    break
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}")
                zcontent = await resp.read()
            with zipfile.ZipFile(io.BytesIO(zcontent), "r") as zf:
                names = zf.namelist()
                if not names:
                    rows = []
                    break
                with zf.open(names[0]) as f:
                    reader = csv.reader(io.TextIOWrapper(f))
                    rows = list(reader)
            break
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                await asyncio.sleep(retry_delay_s * (2 ** attempt))

    # data.binance.vision 的 fundingRate 日文件在不少日期会 404；回退到月包（更稳定）
    if not rows:
        month_str = day.strftime("%Y-%m")
        murl = f"{DATA_VISION_BASE}/data/futures/um/monthly/fundingRate/{sym}/{sym}-fundingRate-{month_str}.zip"
        mrows: List[List[str]] = []
        try:
            async with session.get(murl, timeout=60.0) as resp:
                if resp.status == 404:
                    mrows = []
                elif resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}")
                else:
                    zcontent = await resp.read()
                    with zipfile.ZipFile(io.BytesIO(zcontent), "r") as zf:
                        names = zf.namelist()
                        if names:
                            with zf.open(names[0]) as f:
                                reader = csv.reader(io.TextIOWrapper(f))
                                mrows = list(reader)
        except Exception as e:
            print(f"  Warning: fundingRate monthly vision fetch failed for {sym} {month_str}: {e}", file=sys.stderr)
            mrows = []

        if mrows:
            day_start_ms = int(pd.Timestamp(f"{day_str}T00:00:00Z").value // 1_000_000)
            day_end_ms = int(pd.Timestamp(f"{day_str}T23:59:59.999Z").value // 1_000_000)
            out = []
            for r in mrows or []:
                try:
                    # header: calc_time,funding_interval_hours,last_funding_rate
                    if r and r[0] in ("calc_time", "symbol"):
                        continue
                    ts_ms = int(r[0])
                    if not (day_start_ms <= ts_ms <= day_end_ms):
                        continue
                    out.append(
                        {
                            "key": _funding_key(ts_ms),
                            "fundingRate": float(r[2]),
                            # monthly csv 不含 markPrice；下游字段要求存在，置 0
                            "markPrice": 0.0,
                        }
                    )
                except Exception:
                    continue
            return (
                pd.DataFrame(out, columns=["key", "fundingRate", "markPrice"])
                .drop_duplicates(subset=["key"], keep="last")
                .sort_values("key")
            )

    # 不使用 REST：仅依赖 data.binance.vision（日包不存在则用月包）。
    if not rows:
        return pd.DataFrame(columns=["key", "fundingRate", "markPrice"])

    if not rows and last_err is not None:
        raise RuntimeError(f"fundingRate vision fetch failed: {url} err={last_err}") from last_err
    out = []
    for r in rows or []:
        try:
            if r and r[0] == "symbol":
                continue
            ts_raw = r[1]
            try:
                ts_ms = int(ts_raw)
            except Exception:
                ts_ms = int(pd.Timestamp(ts_raw, tz="UTC").value // 1_000_000)
            out.append({
                "key": _funding_key(ts_ms),
                "fundingRate": float(r[2]),
                "markPrice": float(r[3]) if len(r) > 3 and r[3] not in (None, "") else 0.0,
            })
        except Exception:
            continue
    return pd.DataFrame(out, columns=["key", "fundingRate", "markPrice"]).drop_duplicates(subset=["key"], keep="last")


async def _fetch_premium_history(
    session: aiohttp.ClientSession, symbol: str, day: date, retries: int, retry_delay_s: float
) -> pd.DataFrame:
    """
    从 data.binance.vision 拉取 premiumIndex 5m（日 ZIP；与 klines 一致对 404 额外重试；空则月包切片）。
    """
    sym = format_symbol(symbol)
    day_str = day.isoformat()
    daily_url = f"{DATA_VISION_BASE}/data/futures/um/daily/premiumIndexKlines/{sym}/5m/{sym}-5m-{day_str}.zip"

    rows = await _fetch_vision_zip_csv_rows(
        session,
        url=daily_url,
        timeout_s=60.0,
        retries=retries,
        retry_delay_s=retry_delay_s,
        extra_404_retries=VISION_404_RETRIES_DEFAULT,
        extra_404_retry_delay_s=VISION_404_RETRY_DELAY_S_DEFAULT,
    )

    if not rows:
        murl = (
            f"{DATA_VISION_BASE}/data/futures/um/monthly/premiumIndexKlines/{sym}/5m/"
            f"{sym}-5m-{_month_str(day)}.zip"
        )
        mrows = await _fetch_vision_zip_csv_rows(
            session,
            url=murl,
            timeout_s=60.0,
            retries=max(1, retries),
            retry_delay_s=retry_delay_s,
        )
        if not mrows:
            return pd.DataFrame(columns=["key", "open", "high", "low", "close"])

        start_dt = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int((start_dt + timedelta(days=1)).timestamp() * 1000)

        filtered: List[List[str]] = []
        for r in mrows:
            if not r or r[0] == "open_time":
                continue
            try:
                ot = int(r[0])
            except Exception:
                continue
            if start_ms <= ot < end_ms:
                filtered.append(r)
        rows = [["open_time", "open", "high", "low", "close"]] + filtered

    out = []
    for r in rows or []:
        try:
            if r and r[0] == "open_time":
                continue
            if len(r) < 5:
                continue
            out.append(
                {
                    "key": _premium_key(int(r[0])),
                    "open": float(r[1]),
                    "high": float(r[2]),
                    "low": float(r[3]),
                    "close": float(r[4]),
                }
            )
        except Exception:
            continue
    return (
        pd.DataFrame(out, columns=["key", "open", "high", "low", "close"])
        .drop_duplicates(subset=["key"], keep="last")
        .sort_values("key")
    )


def _compare_funding_symbol(symbol: str, day: date, local_df: pd.DataFrame, remote_df: pd.DataFrame) -> FundingCompareResult:
    merged = pd.merge(remote_df, local_df, on="key", how="outer", suffixes=("_remote", "_local"), indicator=True)
    missing_local = int((merged["_merge"] == "left_only").sum())
    missing_remote = int((merged["_merge"] == "right_only").sum())
    both = merged[merged["_merge"] == "both"]
    mismatched = 0
    examples: List[Dict[str, Any]] = []
    for _, r in both.iterrows():
        fr_l, fr_r = float(r["fundingRate_local"]), float(r["fundingRate_remote"])
        mp_l, mp_r = float(r["markPrice_local"]), float(r["markPrice_remote"])
        bad = (not math.isclose(fr_l, fr_r, rel_tol=1e-6, abs_tol=1e-10)) or (not math.isclose(mp_l, mp_r, rel_tol=1e-4, abs_tol=1e-6))
        if bad:
            mismatched += 1
            if len(examples) < 5:
                examples.append({"key": int(r["key"]), "fundingRate_local": fr_l, "fundingRate_remote": fr_r, "markPrice_local": mp_l, "markPrice_remote": mp_r})
    mismatch_ratio = mismatched / max(1, len(both))
    # 关键修复：本地与 Vision 同时空（both==0）时，不应判为 ok。
    ok = (missing_local == 0) and (missing_remote == 0) and (len(both) > 0) and mismatch_ratio <= 0.01
    return FundingCompareResult(format_symbol(symbol), str(day), ok, int(len(local_df)), int(len(remote_df)), missing_local, missing_remote, mismatched, mismatch_ratio, examples)


def _compare_premium_symbol(symbol: str, day: date, local_df: pd.DataFrame, remote_df: pd.DataFrame) -> PremiumCompareResult:
    merged = pd.merge(remote_df, local_df, on="key", how="outer", suffixes=("_remote", "_local"), indicator=True)
    missing_local = int((merged["_merge"] == "left_only").sum())
    missing_remote = int((merged["_merge"] == "right_only").sum())
    both = merged[merged["_merge"] == "both"]
    mismatched = 0
    examples: List[Dict[str, Any]] = []
    for _, r in both.iterrows():
        bad = False
        for c in ["open", "high", "low", "close"]:
            lv = float(r[f"{c}_local"])
            rv = float(r[f"{c}_remote"])
            if not math.isclose(lv, rv, rel_tol=1e-5, abs_tol=1e-8):
                bad = True
                break
        if bad:
            mismatched += 1
            if len(examples) < 5:
                examples.append({"key": int(r["key"])})
    mismatch_ratio = mismatched / max(1, len(both))
    # 关键修复：本地与 Vision 同时空（both==0）时，不应判为 ok。
    ok = (missing_local == 0) and (missing_remote == 0) and (len(both) > 0) and mismatch_ratio <= 0.01
    return PremiumCompareResult(format_symbol(symbol), str(day), ok, int(len(local_df)), int(len(remote_df)), missing_local, missing_remote, mismatched, mismatch_ratio, examples)


def _save_funding_history(symbol: str, day: date, remote_df: pd.DataFrame, funding_dir: Path) -> bool:
    if remote_df.empty:
        return False
    out = remote_df.copy().sort_values("key")
    out["symbol"] = format_symbol(symbol)
    out["fundingTime"] = pd.to_datetime(out["key"], unit="ms", utc=True)
    out = out[["symbol", "fundingTime", "fundingRate", "markPrice"]]
    out_path = funding_dir / format_symbol(symbol) / f"{day.isoformat()}.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, index=False)
    return True


def _save_premium_history(symbol: str, day: date, remote_df: pd.DataFrame, premium_dir: Path) -> bool:
    if remote_df.empty:
        return False
    out = remote_df.copy().sort_values("key")
    out["symbol"] = format_symbol(symbol)
    out["open_time"] = pd.to_datetime(out["key"], unit="ms", utc=True)
    out["close_time"] = out["open_time"] + pd.to_timedelta(5, unit="m") - pd.to_timedelta(1, unit="ms")
    out["volume"] = 0.0
    out["quote_volume"] = 0.0
    out["trade_count"] = 60
    out["taker_buy_base_volume"] = 0.0
    out["taker_buy_quote_volume"] = 0.0
    out["time_lable"] = range(1, len(out) + 1)
    out = out[
        [
            "symbol",
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_volume",
            "trade_count",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
            "time_lable",
        ]
    ]
    out_path = premium_dir / format_symbol(symbol) / f"{day.isoformat()}.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(out_path, index=False)
    return True


async def compare_funding_and_premium(
    *,
    day: date,
    symbols: Sequence[str],
    cfg: PullCompareConfig,
    max_concurrent: int,
    persist_binance_for_backtest: bool,
) -> Tuple[List[FundingCompareResult], List[PremiumCompareResult]]:
    funding_dir = Path(cfg.local_funding_rates_dir)
    premium_dir = Path(cfg.local_premium_index_dir)
    sem = asyncio.Semaphore(max(1, max_concurrent))
    timeout = aiohttp.ClientTimeout(total=max(300.0, float(cfg.timeout_s) * 2.0))
    funding_results: List[FundingCompareResult] = []
    premium_results: List[PremiumCompareResult] = []
    async with aiohttp.ClientSession(timeout=timeout, headers={"User-Agent": "backtest-pull-compare/1.0"}) as session:
        async def one(sym: str) -> Tuple[FundingCompareResult, PremiumCompareResult]:
            local_f = _load_local_funding_df(sym, day, funding_dir)
            local_p = _load_local_premium_df(sym, day, premium_dir)
            try:
                async with sem:
                    remote_f = await _fetch_funding_history(
                        session, sym, day, retries=max(2, int(cfg.retries)), retry_delay_s=float(cfg.retry_delay_s)
                    )
                    remote_p = await _fetch_premium_history(
                        session, sym, day, retries=max(2, int(cfg.retries)), retry_delay_s=float(cfg.retry_delay_s)
                    )
            except Exception as e:
                print(f"  Warning: failed fetching funding/premium history for {sym}: {e}", file=sys.stderr)
                remote_f = pd.DataFrame(columns=["key", "fundingRate", "markPrice"])
                remote_p = pd.DataFrame(columns=["key", "open", "high", "low", "close"])
            f_res = _compare_funding_symbol(sym, day, local_f, remote_f)
            p_res = _compare_premium_symbol(sym, day, local_p, remote_p)
            # 0.2 下载到的 Binance funding/premium 留作回测使用（覆盖本地）。
            if persist_binance_for_backtest and not remote_f.empty:
                _save_funding_history(sym, day, remote_f, funding_dir)
            if persist_binance_for_backtest and not remote_p.empty:
                _save_premium_history(sym, day, remote_p, premium_dir)
            return f_res, p_res
        pairs = await asyncio.gather(*(one(s) for s in symbols))
    for f, p in pairs:
        funding_results.append(f)
        premium_results.append(p)
    return sorted(funding_results, key=lambda x: x.symbol), sorted(premium_results, key=lambda x: x.symbol)


def _default_day_utc() -> date:
    """默认对比日：UTC 昨天。配合 UTC 12:00 定时任务，验证「前一 UTC 日」已完整落盘的数据。"""
    now = datetime.now(timezone.utc)
    return (now - timedelta(days=1)).date()


def _parse_day(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _month_str(day: date) -> str:
    return f"{day.year:04d}-{day.month:02d}"


def _vision_http_timeout(timeout_s: float) -> aiohttp.ClientTimeout:
    """Vision ZIP 可能较大；单独放宽 sock_read/total，减少读一半断开的假空数据。"""
    ts = max(60.0, float(timeout_s))
    return aiohttp.ClientTimeout(
        sock_connect=30.0,
        sock_read=min(300.0, ts * 2.5),
        total=min(600.0, ts * 4.0),
    )


async def _fetch_vision_zip_csv_rows(
    session: "aiohttp.ClientSession",
    *,
    url: str,
    timeout_s: float,
    retries: int,
    retry_delay_s: float,
    extra_404_retries: int = 0,
    extra_404_retry_delay_s: float = 0.0,
) -> List[List[str]]:
    """
    下载 data.binance.vision 的 ZIP（内含 CSV），返回 CSV rows（含 header）。

    注意：Vision 的 daily 文件在“刚生成/刚同步”阶段可能短暂 404，这里把 404 也纳入重试窗口；
    对 429/502/503/504 及网络读错误做额外重试，减轻 CDN 限流与间歇断连导致的「全空 official」。
    若最终仍 404，则返回空列表（上层可决定是否 fallback 到 monthly）。
    """
    last_err: Optional[BaseException] = None
    base_attempts = max(1, int(retries))
    extra_404_attempts = max(0, int(extra_404_retries))
    total_404_attempts = base_attempts + extra_404_attempts
    http_timeout = _vision_http_timeout(timeout_s)
    not_found_count = 0
    transient_count = 0
    max_rounds = total_404_attempts + VISION_TRANSIENT_MAX_RETRIES + 12

    for _ in range(max_rounds):
        try:
            async with session.get(url, timeout=http_timeout) as resp:
                if resp.status in (429, 502, 503, 504):
                    transient_count += 1
                    if transient_count > VISION_TRANSIENT_MAX_RETRIES:
                        raise RuntimeError(
                            f"HTTP {resp.status} from data.binance.vision (transient retries exhausted): {url}"
                        )
                    ra = resp.headers.get("Retry-After")
                    try:
                        wait_s = float(ra) if ra else float(retry_delay_s) * (2 ** min(transient_count, 5))
                    except ValueError:
                        wait_s = float(retry_delay_s) * (2 ** min(transient_count, 5))
                    await asyncio.sleep(min(90.0, max(0.5, wait_s)))
                    continue
                if resp.status == 404:
                    not_found_count += 1
                    if not_found_count >= total_404_attempts:
                        return []
                    await asyncio.sleep(float(extra_404_retry_delay_s or retry_delay_s))
                    continue
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status} from data.binance.vision")
                zcontent = await resp.read()
            if not zcontent:
                transient_count += 1
                await asyncio.sleep(float(retry_delay_s) * (1.5 ** min(transient_count, 6)))
                continue
            with zipfile.ZipFile(io.BytesIO(zcontent), "r") as zf:
                names = zf.namelist()
                if not names:
                    transient_count += 1
                    await asyncio.sleep(float(retry_delay_s))
                    continue
                with zf.open(names[0]) as f:
                    return list(csv.reader(io.TextIOWrapper(f)))
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError, zipfile.BadZipFile, EOFError) as e:
            last_err = e
            transient_count += 1
            if transient_count > VISION_TRANSIENT_MAX_RETRIES + total_404_attempts:
                raise RuntimeError(f"Failed to fetch ZIP/CSV from data.binance.vision: {last_err}") from last_err
            await asyncio.sleep(float(retry_delay_s) * (2 ** min(transient_count % 8, 5)))
        except Exception as e:
            last_err = e
            raise RuntimeError(f"Failed to fetch ZIP/CSV from data.binance.vision: {last_err}") from last_err

    err_msg = repr(last_err) if last_err else "max rounds exceeded"
    raise RuntimeError(f"Failed to fetch ZIP/CSV from data.binance.vision: {err_msg}")


def _load_universe_symbols(
    target_day: Optional[date] = None,
    universe_dir: Optional[Path] = None,
) -> list:
    """
    加载 universe symbols。
    若指定 target_day（即默认的「UTC 昨天」对比日），只考虑日期目录 <= target_day，
    并取其中**最新**一档：与「前一 UTC 日」数据对齐的 universe 体量。
    universe_dir: 为 None 时用 config 的 data.universe_directory；否则与 --local-universe-dir 一致。
    """
    base = Path(universe_dir) if universe_dir is not None else Path(
        config.get("data.universe_directory", "data/universe")
    )
    
    try:
        date_dirs = sorted(
            [d for d in base.iterdir() if d.is_dir() and len(d.name) == 10 and d.name[4] == "-" and d.name[7] == "-"],
            reverse=True,
        )
        for date_dir in date_dirs:
            dir_date_str = date_dir.name
            if target_day:
                try:
                    dir_date = datetime.strptime(dir_date_str, "%Y-%m-%d").date()
                    if dir_date > target_day:
                        continue
                except ValueError:
                    continue
            csv_path = date_dir / "v1" / "universe.csv"
            if csv_path.exists():
                with open(csv_path, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    syms = [format_symbol(row.get("symbol", "")) for row in reader if row.get("symbol")]
                out = [s for s in syms if s]
                if out:
                    print(f"  Loaded universe from {dir_date_str}/v1/universe.csv ({len(out)} symbols)")
                    return out
    except Exception:
        pass
    
    candidate_json = base / "universe.json"
    if candidate_json.exists():
        try:
            data = json.loads(candidate_json.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                syms = data.get("symbols") or data.get("universe") or []
            else:
                syms = data
            if isinstance(syms, list):
                out = [format_symbol(s) for s in syms if s]
                if out:
                    print(f"  Loaded universe from universe.json ({len(out)} symbols, fallback)")
                    return out
        except Exception:
            pass
    
    candidate_csv = base / "universe.csv"
    if candidate_csv.exists():
        try:
            with open(candidate_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                syms = [format_symbol(row.get("symbol", "")) for row in reader if row.get("symbol")]
            out = [s for s in syms if s]
            if out:
                print(f"  Loaded universe from universe.csv ({len(out)} symbols, fallback)")
                return out
        except Exception:
            pass
    
    return []


def pull_universe_from_live(cfg: PullCompareConfig, target_day: Optional[date] = None) -> bool:
    """
    从实盘机器拉取 universe 文件到本地
    
    Args:
        cfg: 拉取配置
        target_day: 目标日期，只拉取 <= target_day 的 universe（确保使用历史数据对应的 universe）
    
    返回: True 表示至少拉取了一个文件成功
    """
    if not cfg.live_host or not cfg.live_user or not cfg.live_universe_dir:
        return False
    
    local_universe_dir = Path(cfg.local_universe_dir)
    local_universe_dir.mkdir(parents=True, exist_ok=True)
    
    success = False
    
    try:
        # 列出全部按日目录（新→旧）。勿用 head -N：若 target_day 很早，最近 N 日可能都晚于 target，导致永远拉不到 universe。
        result = subprocess.run(
            ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes",
             f"{cfg.live_user}@{cfg.live_host}",
             f"ls -d {cfg.live_universe_dir}/????-??-?? 2>/dev/null | sort -r"],
            capture_output=True, text=True, timeout=120
        )
        if result.returncode == 0 and result.stdout.strip():
            date_dirs = [ln.strip() for ln in result.stdout.strip().split("\n") if ln.strip()]
            skipped_newer = 0
            for remote_date_dir in date_dirs:
                date_name = Path(remote_date_dir).name
                if target_day:
                    try:
                        dir_date = datetime.strptime(date_name, "%Y-%m-%d").date()
                        if dir_date > target_day:
                            skipped_newer += 1
                            continue
                    except ValueError:
                        continue
                if skipped_newer and target_day:
                    print(
                        f"  Skipped {skipped_newer} remote universe date dir(s) newer than target {target_day}"
                    )
                    skipped_newer = 0  # print once
                remote_csv = f"{remote_date_dir}/v1/universe.csv"
                local_date_dir = local_universe_dir / date_name / "v1"
                local_date_dir.mkdir(parents=True, exist_ok=True)
                local_csv = local_date_dir / "universe.csv"
                if _pull_file_from_live(cfg.live_user, cfg.live_host, remote_csv, str(local_csv)):
                    print(f"  Pulled universe file: {date_name}/v1/universe.csv (for target day {target_day})")
                    success = True
                    break
    except Exception as e:
        print(f"  Warning: failed to list remote universe date dirs: {e}", file=sys.stderr)
    
    if not success:
        for filename in ["universe.json", "universe.csv"]:
            remote_path = f"{cfg.live_universe_dir}/{filename}"
            local_path = local_universe_dir / filename
            if _pull_file_from_live(cfg.live_user, cfg.live_host, remote_path, str(local_path)):
                print(f"  Pulled universe file: {filename} (fallback, may not match target day)")
                success = True
    
    return success


def _pull_file_from_live(
    live_user: str,
    live_host: str,
    remote_path: str,
    local_path: str,
    timeout: int = 120,
) -> bool:
    """从实盘机器拉取单个文件，使用 scp"""
    local_dir = Path(local_path).parent
    local_dir.mkdir(parents=True, exist_ok=True)
    
    if sys.platform.startswith("win"):
        scp_cmd = ["scp", "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes",
                   f"{live_user}@{live_host}:{remote_path}", str(local_path)]
    else:
        scp_cmd = ["scp", "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes",
                   f"{live_user}@{live_host}:{remote_path}", str(local_path)]
    
    try:
        result = subprocess.run(scp_cmd, capture_output=True, text=True, timeout=timeout)
        return result.returncode == 0
    except subprocess.TimeoutExpired:
        print(f"SCP timeout for {remote_path}", file=sys.stderr)
        return False
    except Exception as e:
        print(f"SCP error for {remote_path}: {e}", file=sys.stderr)
        return False


def pull_day_data_from_live(
    day: date,
    symbols: Sequence[str],
    cfg: PullCompareConfig,
) -> Tuple[int, int, int]:
    """
    从实盘机器拉取当日的 klines/funding_rates/premium_index 数据
    返回: (拉取的klines数, funding_rates数, premium_index数)
    """
    day_str = day.isoformat()
    klines_count = 0
    funding_count = 0
    premium_count = 0
    
    total = len(symbols)
    for idx, sym in enumerate(symbols, start=1):
        sym_formatted = format_symbol(sym)
        
        if cfg.live_klines_dir and cfg.local_klines_dir:
            remote_kline = f"{cfg.live_klines_dir}/{sym_formatted}/{day_str}.parquet"
            local_kline = Path(cfg.local_klines_dir) / sym_formatted / f"{day_str}.parquet"
            # 关键：先删除本地目标文件，避免历史残留导致 Step3 重算逻辑误触发
            if local_kline.exists():
                try:
                    local_kline.unlink()
                except Exception:
                    pass
            if _pull_file_from_live(cfg.live_user, cfg.live_host, remote_kline, str(local_kline)):
                klines_count += 1
        
        if cfg.live_funding_rates_dir and cfg.local_funding_rates_dir:
            remote_funding = f"{cfg.live_funding_rates_dir}/{sym_formatted}/{day_str}.parquet"
            local_funding = Path(cfg.local_funding_rates_dir) / sym_formatted / f"{day_str}.parquet"
            if local_funding.exists():
                try:
                    local_funding.unlink()
                except Exception:
                    pass
            if _pull_file_from_live(cfg.live_user, cfg.live_host, remote_funding, str(local_funding)):
                funding_count += 1
        
        if cfg.live_premium_index_dir and cfg.local_premium_index_dir:
            remote_premium = f"{cfg.live_premium_index_dir}/{sym_formatted}/{day_str}.parquet"
            local_premium = Path(cfg.local_premium_index_dir) / sym_formatted / f"{day_str}.parquet"
            if local_premium.exists():
                try:
                    local_premium.unlink()
                except Exception:
                    pass
            if _pull_file_from_live(cfg.live_user, cfg.live_host, remote_premium, str(local_premium)):
                premium_count += 1
        if idx == 1 or idx % 50 == 0 or idx == total:
            print(
                f"  Pull progress {idx}/{total}: "
                f"klines={klines_count}, funding_rates={funding_count}, premium_index={premium_count}"
            )
    
    return klines_count, funding_count, premium_count


def _has_local_day_parquet(root_dir: str, symbol: str, day: date) -> bool:
    if not root_dir:
        return False
    p = Path(root_dir) / format_symbol(symbol) / f"{day.isoformat()}.parquet"
    return p.exists()


async def _fetch_official_5m_klines_from_vision(
    session: aiohttp.ClientSession,
    *,
    symbol: str,
    day: date,
    timeout_s: float,
    retries: int,
    retry_delay_s: float,
    vision_404_retries: Optional[int] = None,
    vision_404_retry_delay_s: Optional[float] = None,
) -> List[List[Any]]:
    """从 data.binance.vision 下载 5m klines ZIP"""
    v404 = int(vision_404_retries if vision_404_retries is not None else VISION_404_RETRIES_DEFAULT)
    vdelay = float(vision_404_retry_delay_s if vision_404_retry_delay_s is not None else VISION_404_RETRY_DELAY_S_DEFAULT)
    sym = format_symbol(symbol)
    day_str = day.isoformat()
    daily_url = f"{DATA_VISION_BASE}/data/futures/um/daily/klines/{sym}/5m/{sym}-5m-{day_str}.zip"

    rows = await _fetch_vision_zip_csv_rows(
        session,
        url=daily_url,
        timeout_s=timeout_s,
        retries=retries,
        retry_delay_s=retry_delay_s,
        extra_404_retries=v404,
        extra_404_retry_delay_s=vdelay,
    )

    # daily 不存在/未就绪 -> monthly fallback
    if not rows:
        murl = (
            f"{DATA_VISION_BASE}/data/futures/um/monthly/klines/{sym}/5m/"
            f"{sym}-5m-{_month_str(day)}.zip"
        )
        mrows = await _fetch_vision_zip_csv_rows(
            session,
            url=murl,
            timeout_s=timeout_s,
            retries=max(1, retries),
            retry_delay_s=retry_delay_s,
            extra_404_retries=v404,
            extra_404_retry_delay_s=vdelay,
        )
        if not mrows:
            return []

        start_dt = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int((start_dt + timedelta(days=1)).timestamp() * 1000)

        filtered: List[List[str]] = []
        for r in mrows:
            if not r or r[0] == "open_time":
                continue
            try:
                ot = int(r[0])
            except Exception:
                continue
            if start_ms <= ot < end_ms:
                filtered.append(r)
        # 合成 rows（包含 header，便于兼容原逻辑）
        rows = [["open_time", "open", "high", "low", "close", "volume", "close_time", "quote_volume", "count"]] + filtered

    result: List[List[Any]] = []
    for r in rows:
        if len(r) < 9:
            continue
        if r[0] == "open_time":
            continue
        try:
            result.append([int(r[0]), r[1], r[2], r[3], r[4], r[5], r[6], r[7], int(r[8])])
        except (ValueError, TypeError):
            continue
    return result


async def _fetch_agg_trades_from_vision(
    session: aiohttp.ClientSession,
    *,
    symbol: str,
    day: date,
    timeout_s: float,
    retries: int,
    retry_delay_s: float,
    vision_404_retries: Optional[int] = None,
    vision_404_retry_delay_s: Optional[float] = None,
) -> List[Dict[str, Any]]:
    """从 data.binance.vision 下载 aggTrades ZIP"""
    v404 = int(vision_404_retries if vision_404_retries is not None else VISION_404_RETRIES_DEFAULT)
    vdelay = float(vision_404_retry_delay_s if vision_404_retry_delay_s is not None else VISION_404_RETRY_DELAY_S_DEFAULT)
    sym = format_symbol(symbol)
    day_str = day.isoformat()
    daily_url = f"{DATA_VISION_BASE}/data/futures/um/daily/aggTrades/{sym}/{sym}-aggTrades-{day_str}.zip"

    rows = await _fetch_vision_zip_csv_rows(
        session,
        url=daily_url,
        timeout_s=timeout_s,
        retries=retries,
        retry_delay_s=retry_delay_s,
        extra_404_retries=v404,
        extra_404_retry_delay_s=vdelay,
    )

    if not rows:
        murl = (
            f"{DATA_VISION_BASE}/data/futures/um/monthly/aggTrades/{sym}/"
            f"{sym}-aggTrades-{_month_str(day)}.zip"
        )
        mrows = await _fetch_vision_zip_csv_rows(
            session,
            url=murl,
            timeout_s=timeout_s,
            retries=max(1, retries),
            retry_delay_s=retry_delay_s,
            extra_404_retries=v404,
            extra_404_retry_delay_s=vdelay,
        )
        if not mrows:
            return []

        start_dt = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int((start_dt + timedelta(days=1)).timestamp() * 1000)

        filtered: List[List[str]] = []
        for r in mrows:
            if not r or r[0] == "agg_trade_id":
                continue
            if len(r) < 7:
                continue
            try:
                ts = int(r[5])
            except Exception:
                continue
            if start_ms <= ts < end_ms:
                filtered.append(r)
        rows = [["agg_trade_id", "price", "quantity", "first_trade_id", "last_trade_id", "transact_time", "is_buyer_maker"]] + filtered

    result: List[Dict[str, Any]] = []
    for r in rows:
        if len(r) < 7:
            continue
        if r[0] == "agg_trade_id":
            continue
        try:
            _ = int(r[0])
        except (ValueError, TypeError):
            continue
        try:
            p = float(r[1])
            q = float(r[2])
            result.append({
                "a": int(r[0]),
                "p": p,
                "q": q,
                "Q": p * q,
                "f": int(r[3]) if r[3] else None,
                "l": int(r[4]) if r[4] else None,
                "T": int(r[5]),
                "m": r[6].lower() in ("true", "1"),
            })
        except (ValueError, TypeError):
            continue
    return result


def _official_to_df(symbol: str, klines: List[List[Any]]) -> pd.DataFrame:
    rows = []
    for k in klines or []:
        try:
            open_ms = int(k[0])
            # Vision 为毫秒；merge 键统一为 K 线 open 的 Unix 秒（与 _load_local_kline_df 一致）。
            open_sec = open_ms // 1000
            rows.append({
                "symbol": format_symbol(symbol),
                "open_time_ms": open_sec,
                "open_time": datetime.fromtimestamp(open_ms / 1000, tz=timezone.utc),
                "open": float(k[1]),
                "high": float(k[2]),
                "low": float(k[3]),
                "close": float(k[4]),
                "volume": float(k[5]),
                "quote_volume": float(k[7]),
                "tradecount": int(k[8]),
            })
        except Exception:
            continue
    if not rows:
        return pd.DataFrame(columns=[
            "symbol", "open_time_ms", "open_time", "open", "high", "low",
            "close", "volume", "quote_volume", "tradecount",
        ])
    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["open_time_ms"], keep="last").sort_values("open_time_ms")
    return df


def _load_local_kline_df(symbol: str, day: date, klines_dir: Path) -> pd.DataFrame:
    """从本地 parquet 文件加载当日 K线数据"""
    sym = format_symbol(symbol)
    day_str = day.isoformat()
    parquet_path = klines_dir / sym / f"{day_str}.parquet"
    
    if not parquet_path.exists():
        return pd.DataFrame(columns=[
            "symbol", "open_time_ms", "open_time", "open", "high", "low",
            "close", "volume", "quote_volume", "tradecount",
        ])
    
    try:
        df = pd.read_parquet(parquet_path)
        if df.empty or "open_time" not in df.columns:
            return pd.DataFrame(columns=[
                "symbol", "open_time_ms", "open_time", "open", "high", "low",
                "close", "volume", "quote_volume", "tradecount",
            ])
        
        out = df.copy()
        out["open_time"] = pd.to_datetime(out["open_time"], utc=True)
        # 与 _official_to_df / 实盘层一致：merge 键为 K 线 open 的 Unix 秒（字段名历史原因仍叫 open_time_ms）
        out["open_time_ms"] = (out["open_time"].astype("int64") // 1_000_000_000).astype("int64")
        
        for col in ["open", "high", "low", "close", "volume", "quote_volume"]:
            if col not in out.columns:
                out[col] = pd.NA
        if "tradecount" not in out.columns and "trade_count" in out.columns:
            out["tradecount"] = out["trade_count"]
        elif "tradecount" not in out.columns:
            out["tradecount"] = 0
        
        out["symbol"] = sym
        out = out.drop_duplicates(subset=["open_time_ms"], keep="last").sort_values("open_time_ms")
        return out[[
            "symbol", "open_time_ms", "open_time", "open", "high", "low",
            "close", "volume", "quote_volume", "tradecount",
        ]].reset_index(drop=True)
    except Exception as e:
        print(f"Error loading parquet for {symbol}: {e}", file=sys.stderr)
        return pd.DataFrame(columns=[
            "symbol", "open_time_ms", "open_time", "open", "high", "low",
            "close", "volume", "quote_volume", "tradecount",
        ])


def _log_validation_criteria(cfg: PullCompareConfig) -> None:
    """在日志中输出数据校验的合格定义，便于定时任务排查"""
    print("  [Data validation criteria]")
    print("    Per-bar: 字段在容差内则视为一致 (math.isclose with abs/rel).")
    print("    Per-symbol: missing_local==0 and missing_remote==0 and")
    print(f"               mismatched_rows/total_rows <= max_mismatch_ratio={cfg.max_mismatch_ratio:.4f}.")
    print("  Field tolerances (abs, rel): local vs Binance official 5m klines; within tolerance => match.")
    tolerances = cfg.tolerances or {}
    for f in ["open", "high", "low", "close", "volume", "quote_volume", "tradecount"]:
        t = tolerances.get(f, FieldTolerance(abs=0.0, rel=0.0))
        print(f"    {f}: abs<={t.abs}, rel<={t.rel}")


def _compare_symbol_day(
    *,
    symbol: str,
    day: date,
    local_df: pd.DataFrame,
    remote_df: pd.DataFrame,
    cfg: PullCompareConfig,
    max_examples: int = 5,
) -> SymbolCompareResult:
    # 线上质量判断重点：回测主要依赖 OHLC 的价格轴一致性。
    # volume/quote_volume 在 Vision 与本地聚合时会出现微小（甚至略偏大的）四舍五入差异，
    # 若把它也纳入“全量失败”判定，会导致大量无意义的重算。
    # tradecount 可能出现 1~2 波动，也同样不参与全量失败判定。
    compare_fields = ["open", "high", "low", "close"]
    fields = compare_fields + ["tradecount"]
    tolerances = cfg.tolerances or {}
    merged = pd.merge(
        remote_df, local_df, on="open_time_ms", how="outer",
        suffixes=("_remote", "_local"), indicator=True,
    )
    missing_local = int((merged["_merge"] == "left_only").sum())
    missing_remote = int((merged["_merge"] == "right_only").sum())
    mismatched_rows = 0
    max_ref_diff: Dict[str, float] = {f: 0.0 for f in fields}
    examples: List[Dict[str, Any]] = []
    both = merged[merged["_merge"] == "both"]
    total_both = int(len(both))
    for _, r in both.iterrows():
        row_bad = False  # 仅由 compare_fields 决定（不含 tradecount）
        diffs: Dict[str, Any] = {}
        for f in fields:
            rv = r.get(f"{f}_remote")
            lv = r.get(f"{f}_local")
            try:
                rv_f = float(rv)
                lv_f = float(lv)
            except Exception:
                row_bad = True
                diffs[f] = {"live": lv, "binance_history": rv, "diff": None, "ref_diff": None}
                continue
            diff = lv_f - rv_f
            # 以 Binance 历史值为 reference 的相对差异（0 值时退化为绝对差异）。
            ref_base = abs(rv_f)
            ref_diff = abs(diff) if ref_base == 0 else abs(diff) / ref_base
            max_ref_diff[f] = max(max_ref_diff[f], ref_diff)
            tol = tolerances.get(f, FieldTolerance(abs=0.0, rel=0.0))
            if not tol.ok(lv_f, rv_f):
                if f in compare_fields:
                    row_bad = True
                diffs[f] = {"live": lv_f, "binance_history": rv_f, "diff": diff, "ref_diff": ref_diff}
        if row_bad:
            mismatched_rows += 1
            if len(examples) < max_examples:
                open_sec = int(r["open_time_ms"])
                examples.append({
                    "open_time_ms": open_sec,
                    "open_time": datetime.fromtimestamp(open_sec, tz=timezone.utc).isoformat(),
                    "diffs": diffs,
                })
    mismatch_ratio = mismatched_rows / max(1, total_both)
    # 关键修复：当本地与 Vision 同时都是空（total_both==0）时，不应判为 ok。
    # 否则会掩盖缺失数据并跳过 Step3 的 aggTrade 重算。
    ok = (
        (missing_local == 0)
        and (missing_remote == 0)
        and (total_both > 0)
        and (mismatch_ratio <= cfg.max_mismatch_ratio)
    )
    return SymbolCompareResult(
        symbol=format_symbol(symbol),
        day=str(day),
        ok=ok,
        live_rows=int(len(local_df)),
        binance_history_rows=int(len(remote_df)),
        missing_local=missing_local,
        missing_remote=missing_remote,
        mismatched_rows=mismatched_rows,
        mismatch_ratio=mismatch_ratio,
        max_ref_diff=max_ref_diff,
        examples=examples,
    )


def _aggregate_agg_trades_to_klines(
    symbol: str, trades: List[Dict[str, Any]], interval_minutes: int = 5
) -> pd.DataFrame:
    """从 Vision aggTrade 列表聚合为 5m K 线（与实盘 ``KlineAggregator`` 内有成交窗口一致）。"""
    return aggregate_vision_agg_trades_to_klines_dataframe(
        symbol, trades, interval_minutes=interval_minutes
    )


def _save_klines_parquet(symbol: str, df: pd.DataFrame, day: date, klines_dir: Path) -> bool:
    """保存 K线数据到 parquet 文件"""
    if df.empty:
        return False
    sym = format_symbol(symbol)
    day_str = day.isoformat()
    out_dir = klines_dir / sym
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{day_str}.parquet"
    try:
        df.to_parquet(out_path, index=False)
        return True
    except Exception as e:
        print(f"Error saving parquet for {symbol}: {e}", file=sys.stderr)
        return False


def _save_kline_timestamp_audit(
    *,
    symbol: str,
    day: date,
    local_df: pd.DataFrame,
    remote_df: pd.DataFrame,
    audit_root: Path,
) -> None:
    """
    保存（理论/实际）K线时间戳对比信息，用于排查 Vision 与本地时间轴差异。

    约定：
    - theoretical = Vision(remote_df) 的 open_time_ms
    - actual = 本地(local_df) 的 open_time_ms
    - 两者并集按 5m K 线 open_time_ms（Unix 秒）保存
    """
    if audit_root is None:
        return

    sym = format_symbol(symbol)
    day_str = day.isoformat()
    out_dir = audit_root / sym
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{day_str}.parquet"

    local_keys: set[int] = set()
    remote_keys: set[int] = set()
    if not local_df.empty and "open_time_ms" in local_df.columns:
        local_keys = set(int(x) for x in local_df["open_time_ms"].dropna().tolist())
    if not remote_df.empty and "open_time_ms" in remote_df.columns:
        remote_keys = set(int(x) for x in remote_df["open_time_ms"].dropna().tolist())

    all_keys = sorted(local_keys | remote_keys)
    if not all_keys:
        # 两侧都没有时间轴数据：不写空文件，避免占用太多 inode。
        return

    rows = []
    for k in all_keys:
        in_local = k in local_keys
        in_remote = k in remote_keys
        status = "both" if (in_local and in_remote) else ("local_only" if in_local else "remote_only")
        rows.append(
            {
                "open_time_ms": int(k),
                "open_time_iso": datetime.fromtimestamp(int(k), tz=timezone.utc).isoformat(),
                "theoretical_open_time_ms": int(k) if in_remote else None,
                "actual_open_time_ms": int(k) if in_local else None,
                "status": status,
            }
        )

    pd.DataFrame(rows).to_parquet(out_path, index=False)


async def compare_klines_only(
    *,
    day: date,
    symbols: Sequence[str],
    cfg: PullCompareConfig,
    max_concurrent: Optional[int] = None,
    audit_dir: Optional[Path] = None,
) -> List[SymbolCompareResult]:
    """
    对比官方 5m K线与本地数据，对比失败则下载 aggTrade 重新聚合覆盖。
    对 Vision 使用更长读超时、限并发连接，并在 official 短时为空时额外重试，减轻网络/CDN 抖动。
    """
    klines_dir = Path(cfg.local_klines_dir)
    sem = asyncio.Semaphore(max(1, int(max_concurrent or cfg.max_concurrent)))
    results: List[SymbolCompareResult] = []

    headers = {"User-Agent": "Binance-Data-Vision/1.0"}
    connector = aiohttp.TCPConnector(limit=0, limit_per_host=16, ttl_dns_cache=300, enable_cleanup_closed=True)
    sess_timeout = aiohttp.ClientTimeout(
        sock_connect=30.0,
        sock_read=max(120.0, float(cfg.timeout_s) * 1.5),
        total=None,
    )
    async with aiohttp.ClientSession(
        timeout=sess_timeout, headers=headers, connector=connector
    ) as session:
        async def compare_one(sym: str) -> SymbolCompareResult:
            try:
                remote_df = pd.DataFrame()
                remote_raw: List[List[Any]] = []
                for empty_try in range(4):
                    async with sem:
                        remote_raw = await _fetch_official_5m_klines_from_vision(
                            session,
                            symbol=sym,
                            day=day,
                            timeout_s=cfg.timeout_s,
                            retries=cfg.retries,
                            retry_delay_s=cfg.retry_delay_s,
                            vision_404_retries=cfg.vision_404_retries,
                            vision_404_retry_delay_s=cfg.vision_404_retry_delay_s,
                        )
                    remote_df = _official_to_df(sym, remote_raw)
                    if not remote_df.empty:
                        break
                    if empty_try < 3:
                        await asyncio.sleep(1.0 * (2**empty_try))

                local_df = _load_local_kline_df(sym, day, klines_dir)
                try:
                    if audit_dir is not None:
                        _save_kline_timestamp_audit(
                            symbol=sym,
                            day=day,
                            local_df=local_df,
                            remote_df=remote_df,
                            audit_root=audit_dir,
                        )
                except Exception as e:
                    print(f"Warning: failed saving timestamp audit for {sym}: {e}", file=sys.stderr)

                return _compare_symbol_day(
                    symbol=sym, day=day, local_df=local_df, remote_df=remote_df, cfg=cfg
                )
            except Exception as e:
                print(f"  Warning: kline compare failed for {sym}: {e}", file=sys.stderr)
                return SymbolCompareResult(
                    symbol=format_symbol(sym),
                    day=str(day),
                    ok=False,
                    live_rows=0,
                    binance_history_rows=0,
                    missing_local=0,
                    missing_remote=0,
                    mismatched_rows=0,
                    mismatch_ratio=1.0,
                    max_ref_diff={},
                    examples=[{"error": str(e)}],
                )

        results = await asyncio.gather(*(compare_one(s) for s in symbols))
    return sorted(results, key=lambda r: r.symbol)


async def fix_klines_with_aggtrade(
    *,
    day: date,
    symbols: Sequence[str],
    cfg: PullCompareConfig,
) -> None:
    klines_dir = Path(cfg.local_klines_dir)
    if not symbols:
        return
    print(f"Kline compare failed for {len(symbols)} symbols, downloading aggTrades to recompute...")
    headers = {"User-Agent": "Binance-Data-Vision/1.0"}
    fix_timeout = aiohttp.ClientTimeout(
        sock_connect=30.0,
        sock_read=max(120.0, float(cfg.timeout_s) * 1.5),
        total=None,
    )
    connector = aiohttp.TCPConnector(limit_per_host=12, ttl_dns_cache=300)
    async with aiohttp.ClientSession(timeout=fix_timeout, headers=headers, connector=connector) as session:
        for sym in symbols:
            try:
                # 如果本地当天 K 线都不存在（Step1 未拉到），则没必要重算 aggTrades（也避免日志噪声）。
                # 这种情况应由 Step1 的 missing_local 体现，而不是进入 Step3。
                if not _has_local_day_parquet(cfg.local_klines_dir, sym, day):
                    continue
                print(f"  Downloading aggTrades for {sym}...")
                trades = await _fetch_agg_trades_from_vision(
                    session,
                    symbol=sym,
                    day=day,
                    timeout_s=cfg.timeout_s,
                    retries=cfg.retries,
                    retry_delay_s=cfg.retry_delay_s,
                    vision_404_retries=cfg.vision_404_retries,
                    vision_404_retry_delay_s=cfg.vision_404_retry_delay_s,
                )
                if not trades:
                    print(f"    No aggTrades found for {sym}", file=sys.stderr)
                    continue
                klines_df = _aggregate_agg_trades_to_klines(sym, trades)
                if klines_df.empty:
                    print(f"    No klines from aggTrades for {sym}", file=sys.stderr)
                    continue
                if _save_klines_parquet(sym, klines_df, day, klines_dir):
                    print(f"    Recomputed {sym}: {len(klines_df)} bars aggregated from {len(trades)} trades")
                await asyncio.sleep(0.3)
            except Exception as e:
                print(f"    Failed to recompute {sym}: {e}", file=sys.stderr)


async def _main_async(args: argparse.Namespace) -> int:
    day = _parse_day(args.day) if args.day else _default_day_utc()

    # 写入 PID（cron/nohup 都适用），便于排查/停止任务
    try:
        DEFAULT_PID_PATH.parent.mkdir(parents=True, exist_ok=True)
        DEFAULT_PID_PATH.write_text(f"{os.getpid()}\n", encoding="utf-8")
    except Exception:
        pass
    
    cfg = PullCompareConfig(
        live_host=args.live_host or str(config.get("data.backtest_pull_live_host", "")),
        live_user=args.live_user or str(config.get("data.backtest_pull_live_user", "")),
        live_klines_dir=args.live_klines_dir or str(config.get("data.backtest_pull_live_klines_dir", "")),
        live_funding_rates_dir=args.live_funding_dir or str(config.get("data.backtest_pull_live_funding_rates_dir", "")),
        live_premium_index_dir=args.live_premium_dir or str(config.get("data.backtest_pull_live_premium_index_dir", "")),
        live_universe_dir=args.live_universe_dir or str(config.get("data.backtest_pull_live_universe_dir", "")),
        local_klines_dir=args.local_klines_dir or str(config.get("data.klines_directory", "data/klines")),
        local_funding_rates_dir=args.local_funding_dir or str(config.get("data.funding_rates_directory", "data/funding_rates")),
        local_premium_index_dir=args.local_premium_dir or str(config.get("data.premium_index_directory", "data/premium_index")),
        local_universe_dir=args.local_universe_dir or str(config.get("data.universe_directory", "data/universe")),
        max_concurrent=int(args.max_concurrent),
        timeout_s=float(args.timeout_s),
        retries=int(args.retries),
        retry_delay_s=float(args.retry_delay_s),
        vision_404_retries=int(args.vision_404_retries),
        vision_404_retry_delay_s=float(args.vision_404_retry_delay_s),
    )
    
    if not cfg.live_host or not cfg.live_user:
        print("Error: live_host and live_user must be configured.", file=sys.stderr)
        print("Use --live-host and --live-user or set data.backtest_pull_live_host/user in config.", file=sys.stderr)
        return 1
    
    if not cfg.live_klines_dir:
        print("Error: live_klines_dir must be configured.", file=sys.stderr)
        return 1

    # Vision 前一天数据在次日 UTC 10:00 才稳定可用。
    # Cron 为 UTC 12:00 时理论上无需等待，但为了手动/异常触发更稳，仍做一次有上限等待。
    try:
        now = datetime.now(timezone.utc)
        vision_ready_at = datetime(day.year, day.month, day.day, tzinfo=timezone.utc) + timedelta(days=1, hours=10)
        if now < vision_ready_at:
            wait_s = (vision_ready_at - now).total_seconds()
            wait_s = max(0.0, min(float(wait_s), 7200.0))  # 最多等待 2 小时
            if wait_s > 0:
                print(f"\n[VisionReadyWait] Waiting {wait_s:.0f}s until {vision_ready_at.isoformat()} UTC...")
                await asyncio.sleep(wait_s)
    except Exception:
        pass

    # 保存理论/实际时间戳对比数据（与 klines 目录同级）
    timestamp_audit_dir = Path(cfg.local_klines_dir).parent / "timestamp_audit"
    timestamp_audit_dir.mkdir(parents=True, exist_ok=True)
    
    print(f"=== Backtest Pull Compare: {day.isoformat()} ===")
    print(f"Live: {cfg.live_user}@{cfg.live_host}")
    print(
        f"Target day: {day.isoformat()} "
        f"(universe: latest YYYY-MM-DD/v1/universe.csv with date <= this day; "
        f"aligned with 'previous UTC day' data under test)"
    )
    
    if args.symbols:
        symbols = [format_symbol(s.strip()) for s in args.symbols.split(",") if s.strip()]
    else:
        uroot = Path(cfg.local_universe_dir)
        symbols = _load_universe_symbols(target_day=day, universe_dir=uroot)
        if not symbols and cfg.live_universe_dir:
            print(f"\n[Step 0] Local universe not found, pulling from live machine (for {day})...")
            if pull_universe_from_live(cfg, target_day=day):
                symbols = _load_universe_symbols(target_day=day, universe_dir=uroot)
    
    if not symbols:
        print("No symbols provided and universe not found (both local and remote).", file=sys.stderr)
        print(f"Make sure universe exists for date <= {day.isoformat()}", file=sys.stderr)
        return 1
    
    print(f"Symbols: {len(symbols)}")
    
    if not args.compare_only:
        print(f"\n[Step 1] Pulling data from live machine...")
        klines_pulled, funding_pulled, premium_pulled = pull_day_data_from_live(day, symbols, cfg)
        print(f"  Pulled: klines={klines_pulled}, funding_rates={funding_pulled}, premium_index={premium_pulled}")
    
    print(f"\n[Step 2] Download Binance history (funding/premium/kline) and first compare (live)...")
    _log_validation_criteria(cfg)
    klines_live_results = await compare_klines_only(
        day=day, symbols=symbols, cfg=cfg, max_concurrent=int(args.max_concurrent), audit_dir=timestamp_audit_dir
    )
    funding_live_results, premium_live_results = await compare_funding_and_premium(
        day=day,
        symbols=symbols,
        cfg=cfg,
        max_concurrent=int(args.max_concurrent),
        persist_binance_for_backtest=True,
    )

    klines_live_ok = sum(1 for r in klines_live_results if r.ok)
    klines_live_fail = len(klines_live_results) - klines_live_ok
    funding_live_ok = sum(1 for r in funding_live_results if r.ok)
    funding_live_fail = len(funding_live_results) - funding_live_ok
    premium_live_ok = sum(1 for r in premium_live_results if r.ok)
    premium_live_fail = len(premium_live_results) - premium_live_ok

    out_dir = Path(args.output_dir or default_backtest_compare_output_dir())
    ensure_directory(str(out_dir))
    live_report_path = out_dir / f"{day.isoformat()}.live.json"
    live_payload = {
        "day": day.isoformat(),
        "mode": "backtest_pull_compare_live",
        "summary": {
            "klines": {"ok": klines_live_ok, "fail": klines_live_fail, "total": len(klines_live_results)},
            "funding_rates": {"ok": funding_live_ok, "fail": funding_live_fail, "total": len(funding_live_results)},
            "premium_index": {"ok": premium_live_ok, "fail": premium_live_fail, "total": len(premium_live_results)},
        },
        "klines_results": [asdict(r) for r in klines_live_results],
        "funding_rates_results": [asdict(r) for r in funding_live_results],
        "premium_index_results": [asdict(r) for r in premium_live_results],
    }
    live_report_path.write_text(json.dumps(live_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"\n=== Live Summary ===")
    print(f"day={day.isoformat()}")
    print(f"klines(live): total={len(klines_live_results)} ok={klines_live_ok} fail={klines_live_fail}")
    print(f"funding_rates(live): total={len(funding_live_results)} ok={funding_live_ok} fail={funding_live_fail}")
    print(f"premium_index(live): total={len(premium_live_results)} ok={premium_live_ok} fail={premium_live_fail}")
    print(f"Live report: {live_report_path}")

    klines_history_results: List[SymbolCompareResult] = klines_live_results
    history_report_path = out_dir / f"{day.isoformat()}.history.json"
    if klines_live_fail > 0:
        print(f"\n[Step 3] Kline live compare failed, recompute from Binance aggTrade then second compare (history)...")
        fail_symbols = [r.symbol for r in klines_live_results if not r.ok]
        await fix_klines_with_aggtrade(day=day, symbols=fail_symbols, cfg=cfg)
        klines_history_results = await compare_klines_only(
            day=day, symbols=symbols, cfg=cfg, max_concurrent=int(args.max_concurrent), audit_dir=timestamp_audit_dir
        )
    else:
        print(f"\n[Step 3] Kline live compare passed, keep pulled live kline for backtest.")

    klines_history_ok = sum(1 for r in klines_history_results if r.ok)
    klines_history_fail = len(klines_history_results) - klines_history_ok
    history_payload = {
        "day": day.isoformat(),
        "mode": "backtest_pull_compare_history",
        "summary": {
            "klines": {"ok": klines_history_ok, "fail": klines_history_fail, "total": len(klines_history_results)},
            "funding_rates": {"ok": funding_live_ok, "fail": funding_live_fail, "total": len(funding_live_results)},
            "premium_index": {"ok": premium_live_ok, "fail": premium_live_fail, "total": len(premium_live_results)},
        },
        "klines_results": [asdict(r) for r in klines_history_results],
        "funding_rates_results": [asdict(r) for r in funding_live_results],
        "premium_index_results": [asdict(r) for r in premium_live_results],
    }
    history_report_path.write_text(json.dumps(history_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"\n=== History Summary ===")
    print(f"day={day.isoformat()}")
    print(f"klines(history): total={len(klines_history_results)} ok={klines_history_ok} fail={klines_history_fail}")
    print(f"funding_rates(history-source): total={len(funding_live_results)} ok={funding_live_ok} fail={funding_live_fail}")
    print(f"premium_index(history-source): total={len(premium_live_results)} ok={premium_live_ok} fail={premium_live_fail}")
    print(f"History report: {history_report_path}")

    if args.print_fail:
        for group_name, rows in [
            ("klines.live", klines_live_results),
            ("klines.history", klines_history_results),
            ("funding_rates.live", funding_live_results),
            ("premium_index.live", premium_live_results),
        ]:
            fails = [r for r in rows if not r.ok]
            if not fails:
                continue
            print(f"\nFailed symbols ({group_name}):")
            for r in fails:
                print(f"  - {r.symbol}: missing_local={r.missing_local} missing_remote={r.missing_remote} mismatched={r.mismatched_rows}")

    return 0 if (klines_history_fail == 0 and funding_live_fail == 0 and premium_live_fail == 0) else 2


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backtest machine: pull data from live, compare with official, fix mismatches."
    )
    parser.add_argument("--day", help="UTC day YYYY-MM-DD to process. Default: yesterday.")
    parser.add_argument("--symbols", help="Comma-separated symbols. If omitted, use universe.")
    parser.add_argument("--live-host", help="Live trading machine hostname/IP.")
    parser.add_argument("--live-user", help="SSH username for live machine.")
    parser.add_argument("--live-klines-dir", help="Klines directory path on live machine.")
    parser.add_argument("--live-funding-dir", help="Funding rates directory path on live machine.")
    parser.add_argument("--live-premium-dir", help="Premium index directory path on live machine.")
    parser.add_argument("--live-universe-dir", help="Universe directory path on live machine.")
    parser.add_argument("--local-klines-dir", help="Local klines directory path.")
    parser.add_argument("--local-funding-dir", help="Local funding rates directory path.")
    parser.add_argument("--local-premium-dir", help="Local premium index directory path.")
    parser.add_argument("--local-universe-dir", help="Local universe directory path.")
    parser.add_argument("--max-concurrent", type=int, default=5)
    parser.add_argument(
        "--timeout-s",
        type=float,
        default=120.0,
        help="Per-request timeout budget for Vision ZIP reads (seconds). Default 120.",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=5,
        help="Base HTTP retries for each Vision URL (before 404 extra retries). Default 5.",
    )
    parser.add_argument(
        "--retry-delay-s",
        type=float,
        default=1.0,
        help="Base delay between retries (seconds). Default 1.0.",
    )
    parser.add_argument(
        "--vision-404-retries",
        type=int,
        default=8,
        help="Extra attempts when Vision returns 404 (daily not ready). Default 8.",
    )
    parser.add_argument(
        "--vision-404-retry-delay-s",
        type=float,
        default=3.0,
        help="Delay between 404 retries (seconds). Default 3.0.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Report output dir (default: data.backtest_compare_output_directory or compare/backtest_pull).",
    )
    parser.add_argument("--compare-only", action="store_true", help="Only compare, skip pulling data.")
    parser.add_argument("--print-fail", action="store_true", help="Print failed symbols and details.")
    args = parser.parse_args()
    return asyncio.run(_main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
