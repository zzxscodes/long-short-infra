#!/usr/bin/env python3
"""
回测机主动拉取实盘数据并对比

工作流程：
1. 从实盘机器拉取当日 klines/funding_rates/premium_index 数据
2. 下载 Binance 官方 5m K线（data.binance.vision），与拉取的数据对比
3. 对比通过：保持拉取的数据不变
4. 对比不通过：从 data.binance.vision 下载历史 aggTrade，聚合覆盖本地数据

运行环境：回测机（Windows），需要配置到实盘机器的 SSH 访问

建议定时任务：UTC 10:30（data.binance.vision 当日文件已可用）
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

DATA_VISION_BASE = "https://data.binance.vision"

# 默认报告输出目录（prepare_backtest_history_data 与之保持一致，便于同一目录查看 *.json）
DEFAULT_BACKTEST_COMPARE_OUTPUT_DIR = "data/compare/backtest_pull"

DEFAULT_PID_PATH = PROJECT_ROOT / "logs" / "backtest_pull_compare.pid"

# Vision 日文件在“刚产出/刚同步”阶段可能短暂 404；对 404 额外重试，避免误判为空数据。
VISION_404_RETRIES_DEFAULT = 6
VISION_404_RETRY_DELAY_S_DEFAULT = 3.0

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
    timeout_s: float = 60.0
    max_concurrent: int = 5
    retries: int = 3
    retry_delay_s: float = 0.8
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
        "volume": FieldTolerance(abs=0.0, rel=1e-4),
        "quote_volume": FieldTolerance(abs=0.0, rel=1e-4),
        # tradecount 仍要求完全一致
        "tradecount": FieldTolerance(abs=0.0, rel=0.0),
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
    sym = format_symbol(symbol)
    day_str = day.isoformat()
    url = f"{DATA_VISION_BASE}/data/futures/um/daily/premiumIndexKlines/{sym}/5m/{sym}-5m-{day_str}.zip"
    last_err: Optional[BaseException] = None
    rows: List[List[str]] = []
    for attempt in range(max(1, retries)):
        try:
            async with session.get(url, timeout=60.0) as resp:
                if resp.status == 404:
                    return pd.DataFrame(columns=["key", "open", "high", "low", "close"])
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}")
                zcontent = await resp.read()
            with zipfile.ZipFile(io.BytesIO(zcontent), "r") as zf:
                names = zf.namelist()
                if not names:
                    return pd.DataFrame(columns=["key", "open", "high", "low", "close"])
                with zf.open(names[0]) as f:
                    reader = csv.reader(io.TextIOWrapper(f))
                    rows = list(reader)
            break
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                await asyncio.sleep(retry_delay_s * (2 ** attempt))
    if not rows and last_err is not None:
        raise RuntimeError(f"premiumIndexKlines vision fetch failed: {url} err={last_err}") from last_err
    out = []
    for r in rows or []:
        try:
            if r and r[0] == "open_time":
                continue
            out.append({"key": _premium_key(int(r[0])), "open": float(r[1]), "high": float(r[2]), "low": float(r[3]), "close": float(r[4])})
        except Exception:
            continue
    return pd.DataFrame(out, columns=["key", "open", "high", "low", "close"]).drop_duplicates(subset=["key"], keep="last").sort_values("key")


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
    ok = (missing_local == 0) and (missing_remote == 0) and mismatch_ratio <= 0.01
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
    ok = (missing_local == 0) and (missing_remote == 0) and mismatch_ratio <= 0.01
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
    timeout = aiohttp.ClientTimeout(total=60)
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
    now = datetime.now(timezone.utc)
    return (now - timedelta(days=1)).date()


def _parse_day(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _month_str(day: date) -> str:
    return f"{day.year:04d}-{day.month:02d}"


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
    若最终仍 404，则返回空列表（上层可决定是否 fallback 到 monthly）。
    """
    last_err: Optional[BaseException] = None
    base_attempts = max(1, int(retries))
    extra_404_attempts = max(0, int(extra_404_retries))
    total_attempts = base_attempts + extra_404_attempts
    for attempt in range(total_attempts):
        try:
            async with session.get(url, timeout=timeout_s) as resp:
                if resp.status == 404:
                    if attempt < total_attempts - 1:
                        await asyncio.sleep(float(extra_404_retry_delay_s or retry_delay_s))
                        continue
                    return []
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status} from data.binance.vision")
                zcontent = await resp.read()
            with zipfile.ZipFile(io.BytesIO(zcontent), "r") as zf:
                names = zf.namelist()
                if not names:
                    return []
                with zf.open(names[0]) as f:
                    return list(csv.reader(io.TextIOWrapper(f)))
        except Exception as e:
            last_err = e
            if attempt < total_attempts - 1:
                await asyncio.sleep(float(retry_delay_s) * (2 ** min(attempt, 4)))
    raise RuntimeError(f"Failed to fetch ZIP/CSV from data.binance.vision: {last_err}") from last_err


def _load_universe_symbols(
    target_day: Optional[date] = None,
    universe_dir: Optional[Path] = None,
) -> list:
    """
    加载 universe symbols。
    如果指定 target_day，则只加载 <= target_day 的 universe（确保使用历史数据对应的 universe）
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
        result = subprocess.run(
            ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=yes",
             f"{cfg.live_user}@{cfg.live_host}",
             f"ls -d {cfg.live_universe_dir}/????-??-?? 2>/dev/null | sort -r | head -10"],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode == 0 and result.stdout.strip():
            date_dirs = result.stdout.strip().split('\n')
            for remote_date_dir in date_dirs:
                date_name = Path(remote_date_dir).name
                if target_day:
                    try:
                        dir_date = datetime.strptime(date_name, "%Y-%m-%d").date()
                        if dir_date > target_day:
                            print(f"  Skipping {date_name} (newer than target {target_day})")
                            continue
                    except ValueError:
                        continue
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
            if _pull_file_from_live(cfg.live_user, cfg.live_host, remote_kline, str(local_kline)):
                klines_count += 1
        
        if cfg.live_funding_rates_dir and cfg.local_funding_rates_dir:
            remote_funding = f"{cfg.live_funding_rates_dir}/{sym_formatted}/{day_str}.parquet"
            local_funding = Path(cfg.local_funding_rates_dir) / sym_formatted / f"{day_str}.parquet"
            if _pull_file_from_live(cfg.live_user, cfg.live_host, remote_funding, str(local_funding)):
                funding_count += 1
        
        if cfg.live_premium_index_dir and cfg.local_premium_index_dir:
            remote_premium = f"{cfg.live_premium_index_dir}/{sym_formatted}/{day_str}.parquet"
            local_premium = Path(cfg.local_premium_index_dir) / sym_formatted / f"{day_str}.parquet"
            if _pull_file_from_live(cfg.live_user, cfg.live_host, remote_premium, str(local_premium)):
                premium_count += 1
        if idx == 1 or idx % 50 == 0 or idx == total:
            print(
                f"  Pull progress {idx}/{total}: "
                f"klines={klines_count}, funding_rates={funding_count}, premium_index={premium_count}"
            )
    
    return klines_count, funding_count, premium_count


async def _fetch_official_5m_klines_from_vision(
    session: aiohttp.ClientSession,
    *,
    symbol: str,
    day: date,
    timeout_s: float,
    retries: int,
    retry_delay_s: float,
) -> List[List[Any]]:
    """从 data.binance.vision 下载 5m klines ZIP"""
    sym = format_symbol(symbol)
    day_str = day.isoformat()
    daily_url = f"{DATA_VISION_BASE}/data/futures/um/daily/klines/{sym}/5m/{sym}-5m-{day_str}.zip"

    rows = await _fetch_vision_zip_csv_rows(
        session,
        url=daily_url,
        timeout_s=timeout_s,
        retries=retries,
        retry_delay_s=retry_delay_s,
        extra_404_retries=VISION_404_RETRIES_DEFAULT,
        extra_404_retry_delay_s=VISION_404_RETRY_DELAY_S_DEFAULT,
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
) -> List[Dict[str, Any]]:
    """从 data.binance.vision 下载 aggTrades ZIP"""
    sym = format_symbol(symbol)
    day_str = day.isoformat()
    daily_url = f"{DATA_VISION_BASE}/data/futures/um/daily/aggTrades/{sym}/{sym}-aggTrades-{day_str}.zip"

    rows = await _fetch_vision_zip_csv_rows(
        session,
        url=daily_url,
        timeout_s=timeout_s,
        retries=retries,
        retry_delay_s=retry_delay_s,
        extra_404_retries=VISION_404_RETRIES_DEFAULT,
        extra_404_retry_delay_s=VISION_404_RETRY_DELAY_S_DEFAULT,
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
    fields = ["open", "high", "low", "close", "volume", "quote_volume", "tradecount"]
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
        row_bad = False
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
    ok = (missing_local == 0) and (missing_remote == 0) and (mismatch_ratio <= cfg.max_mismatch_ratio)
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
    """从 aggTrade 列表批量聚合为 5m K线"""
    if not trades:
        return pd.DataFrame()
    interval_seconds = interval_minutes * 60
    by_window: Dict[int, List[Dict]] = {}
    for t in trades:
        ts_ms = int(t.get("T", 0))
        if ts_ms <= 0:
            continue
        window_s = (ts_ms // 1000) // interval_seconds * interval_seconds
        window_ms = window_s * 1000
        if window_ms not in by_window:
            by_window[window_ms] = []
        by_window[window_ms].append(t)
    rows = []
    for window_ms, window_trades in sorted(by_window.items()):
        if not window_trades:
            continue
        prices = []
        volumes = []
        quote_volumes = []
        trade_count_underlying = 0
        for t in window_trades:
            p = float(t.get("p", 0))
            q = float(t.get("q", 0))
            qq = float(t.get("Q", 0))
            if qq == 0:
                qq = p * q
            prices.append(p)
            volumes.append(q)
            quote_volumes.append(qq)
            f_id = t.get("f")
            l_id = t.get("l")
            if f_id is not None and l_id is not None:
                trade_count_underlying += max(1, int(l_id) - int(f_id) + 1)
            else:
                trade_count_underlying += 1
        open_p = prices[0]
        high_p = max(prices)
        low_p = min(prices)
        close_p = prices[-1]
        volume = sum(volumes)
        quote_volume = sum(quote_volumes)
        window_dt = datetime.fromtimestamp(window_ms / 1000, tz=timezone.utc)
        window_end_ms = window_ms + interval_minutes * 60 * 1000
        close_dt = datetime.fromtimestamp(window_end_ms / 1000, tz=timezone.utc)
        rows.append({
            "symbol": format_symbol(symbol),
            "open_time": window_dt,
            "close_time": close_dt,
            "open": open_p,
            "high": high_p,
            "low": low_p,
            "close": close_p,
            "volume": volume,
            "quote_volume": quote_volume,
            "tradecount": trade_count_underlying,
        })
    return pd.DataFrame(rows)


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


async def compare_klines_only(
    *,
    day: date,
    symbols: Sequence[str],
    cfg: PullCompareConfig,
    max_concurrent: Optional[int] = None,
) -> List[SymbolCompareResult]:
    """
    对比官方 5m K线与本地数据，对比失败则下载 aggTrade 重新聚合覆盖
    """
    klines_dir = Path(cfg.local_klines_dir)
    sem = asyncio.Semaphore(max(1, int(max_concurrent or cfg.max_concurrent)))
    timeout = aiohttp.ClientTimeout(total=cfg.timeout_s)
    results: List[SymbolCompareResult] = []
    
    headers = {"User-Agent": "Binance-Data-Vision/1.0"}
    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        async def compare_one(sym: str) -> SymbolCompareResult:
            async with sem:
                remote_raw = await _fetch_official_5m_klines_from_vision(
                    session, symbol=sym, day=day,
                    timeout_s=cfg.timeout_s, retries=cfg.retries, retry_delay_s=cfg.retry_delay_s,
                )
            remote_df = _official_to_df(sym, remote_raw)
            local_df = _load_local_kline_df(sym, day, klines_dir)
            return _compare_symbol_day(
                symbol=sym, day=day, local_df=local_df, remote_df=remote_df, cfg=cfg
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
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120.0), headers=headers) as session:
        for sym in symbols:
            try:
                print(f"  Downloading aggTrades for {sym}...")
                trades = await _fetch_agg_trades_from_vision(
                    session, symbol=sym, day=day,
                    timeout_s=cfg.timeout_s, retries=cfg.retries, retry_delay_s=cfg.retry_delay_s,
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
    )
    
    if not cfg.live_host or not cfg.live_user:
        print("Error: live_host and live_user must be configured.", file=sys.stderr)
        print("Use --live-host and --live-user or set data.backtest_pull_live_host/user in config.", file=sys.stderr)
        return 1
    
    if not cfg.live_klines_dir:
        print("Error: live_klines_dir must be configured.", file=sys.stderr)
        return 1
    
    print(f"=== Backtest Pull Compare: {day.isoformat()} ===")
    print(f"Live: {cfg.live_user}@{cfg.live_host}")
    print(f"Target day: {day.isoformat()} (universe must be <= this date)")
    
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
        day=day, symbols=symbols, cfg=cfg, max_concurrent=int(args.max_concurrent)
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

    out_dir = Path(args.output_dir or DEFAULT_BACKTEST_COMPARE_OUTPUT_DIR)
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
    # 向后兼容：保留无后缀报告为 live 版本。
    (out_dir / f"{day.isoformat()}.json").write_text(
        json.dumps(live_payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

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
            day=day, symbols=symbols, cfg=cfg, max_concurrent=int(args.max_concurrent)
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
        "--output-dir",
        default=DEFAULT_BACKTEST_COMPARE_OUTPUT_DIR,
        help="Report output dir.",
    )
    parser.add_argument("--compare-only", action="store_true", help="Only compare, skip pulling data.")
    parser.add_argument("--print-fail", action="store_true", help="Print failed symbols and details.")
    args = parser.parse_args()
    return asyncio.run(_main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
