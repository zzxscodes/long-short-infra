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
    timeout_s: float = 60.0
    max_concurrent: int = 5
    retries: int = 3
    retry_delay_s: float = 0.8
    # 单个 symbol 每日允许的最大 mismatched 行占比（0.01 = 1%）
    max_mismatch_ratio: float = 0.01
    tolerances: Dict[str, FieldTolerance] = field(default_factory=lambda: {
        # 价格：支持极小的浮点误差
        "open": FieldTolerance(abs=0.0, rel=1e-6),
        "high": FieldTolerance(abs=0.0, rel=1e-6),
        "low": FieldTolerance(abs=0.0, rel=1e-6),
        "close": FieldTolerance(abs=0.0, rel=1e-6),
        # 成交量：按量级给出更实用的误差
        "volume": FieldTolerance(abs=0.0, rel=1e-6),
        "quote_volume": FieldTolerance(abs=0.0, rel=1e-6),
        # tradecount 仍要求完全一致
        "tradecount": FieldTolerance(abs=0.0, rel=1e-6),
    })


@dataclass
class SymbolCompareResult:
    symbol: str
    day: str
    ok: bool
    local_rows: int
    remote_rows: int
    missing_local: int
    missing_remote: int
    mismatched_rows: int
    mismatch_ratio: float
    max_abs_diff: Dict[str, float]
    examples: List[Dict[str, Any]]


def _default_day_utc() -> date:
    now = datetime.now(timezone.utc)
    return (now - timedelta(days=1)).date()


def _parse_day(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def _load_universe_symbols(target_day: Optional[date] = None) -> list:
    """
    加载 universe symbols。
    如果指定 target_day，则只加载 <= target_day 的 universe（确保使用历史数据对应的 universe）
    """
    universe_dir = Path(config.get("data.universe_directory", "data/universe"))
    
    try:
        date_dirs = sorted(
            [d for d in universe_dir.iterdir() if d.is_dir() and len(d.name) == 10 and d.name[4] == "-" and d.name[7] == "-"],
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
    
    candidate_json = universe_dir / "universe.json"
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
    
    candidate_csv = universe_dir / "universe.csv"
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
    
    for sym in symbols:
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
    url = f"{DATA_VISION_BASE}/data/futures/um/daily/klines/{sym}/5m/{sym}-5m-{day_str}.zip"
    last_err: Optional[BaseException] = None
    for attempt in range(max(1, retries)):
        try:
            async with session.get(url, timeout=timeout_s) as resp:
                if resp.status == 404:
                    return []
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status} from data.binance.vision")
                zcontent = await resp.read()
            with zipfile.ZipFile(io.BytesIO(zcontent), "r") as zf:
                names = zf.namelist()
                if not names:
                    return []
                with zf.open(names[0]) as f:
                    reader = csv.reader(io.TextIOWrapper(f))
                    rows = list(reader)
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
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                await asyncio.sleep(retry_delay_s * (2 ** attempt))
    raise RuntimeError(f"Failed to fetch klines from data.binance.vision for {symbol}: {last_err}") from last_err


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
    url = f"{DATA_VISION_BASE}/data/futures/um/daily/aggTrades/{sym}/{sym}-aggTrades-{day_str}.zip"
    last_err: Optional[BaseException] = None
    for attempt in range(max(1, retries)):
        try:
            async with session.get(url, timeout=timeout_s) as resp:
                if resp.status == 404:
                    return []
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status} from data.binance.vision")
                zcontent = await resp.read()
            with zipfile.ZipFile(io.BytesIO(zcontent), "r") as zf:
                names = zf.namelist()
                if not names:
                    return []
                with zf.open(names[0]) as f:
                    reader = csv.reader(io.TextIOWrapper(f))
                    rows = list(reader)
            result: List[Dict[str, Any]] = []
            for r in rows:
                if len(r) < 7:
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
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                await asyncio.sleep(retry_delay_s * (2 ** attempt))
    raise RuntimeError(f"Failed to fetch aggTrades from data.binance.vision for {symbol}: {last_err}") from last_err


def _official_to_df(symbol: str, klines: List[List[Any]]) -> pd.DataFrame:
    rows = []
    for k in klines or []:
        try:
            open_ms = int(k[0])
            # data.binance.vision 使用毫秒时间戳；本地 parquet 当前使用“秒级”时间戳作为 open_time_ms。
            # 为了与实盘数据层产物保持一致，这里将毫秒转换为秒。
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
        # 统一 open_time 的解析为 UTC，并且将 join key 标准化为「秒级 epoch」。
        # 本仓库实盘数据层产出的 parquet 存在不同 datetime 精度/表示（秒/毫秒/纳秒），
        # 使用 datetime64[ns] 再转 epoch 秒可避免单位不一致导致 merge 全缺失。
        out["open_time"] = pd.to_datetime(out["open_time"], utc=True)
        out["open_time_sec"] = (out["open_time"].astype("int64") // 1_000_000_000).astype("int64")
        out["open_time_ms"] = out["open_time_sec"]
        
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
    max_abs_diff: Dict[str, float] = {f: 0.0 for f in fields}
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
                diffs[f] = {"local": lv, "remote": rv, "diff": None}
                continue
            diff = lv_f - rv_f
            max_abs_diff[f] = max(max_abs_diff[f], abs(diff))
            tol = tolerances.get(f, FieldTolerance(abs=0.0, rel=0.0))
            if not tol.ok(lv_f, rv_f):
                row_bad = True
                diffs[f] = {"local": lv_f, "remote": rv_f, "diff": diff}
        if row_bad:
            mismatched_rows += 1
            if len(examples) < max_examples:
                open_ms = int(r["open_time_ms"])
                examples.append({
                    "open_time_ms": open_ms,
                    "open_time": datetime.fromtimestamp(open_ms, tz=timezone.utc).isoformat(),
                    "diffs": diffs,
                })
    mismatch_ratio = mismatched_rows / max(1, total_both)
    ok = (missing_local == 0) and (missing_remote == 0) and (mismatch_ratio <= cfg.max_mismatch_ratio)
    return SymbolCompareResult(
        symbol=format_symbol(symbol),
        day=str(day),
        ok=ok,
        local_rows=int(len(local_df)),
        remote_rows=int(len(remote_df)),
        missing_local=missing_local,
        missing_remote=missing_remote,
        mismatched_rows=mismatched_rows,
        mismatch_ratio=mismatch_ratio,
        max_abs_diff=max_abs_diff,
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


async def compare_and_fix(
    *,
    day: date,
    symbols: Sequence[str],
    cfg: PullCompareConfig,
    output_dir: Optional[Path] = None,
) -> Tuple[bool, List[SymbolCompareResult]]:
    """
    对比官方 5m K线与本地数据，对比失败则下载 aggTrade 重新聚合覆盖
    """
    klines_dir = Path(cfg.local_klines_dir)
    sem = asyncio.Semaphore(max(1, cfg.max_concurrent))
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
    
    results = sorted(results, key=lambda r: r.symbol)
    ok_count = sum(1 for r in results if r.ok)
    fail_symbols = [r.symbol for r in results if not r.ok]
    
    if output_dir:
        ensure_directory(str(output_dir))
        out_path = output_dir / f"{day.isoformat()}.json"
        payload = {
            "day": day.isoformat(),
            "mode": "backtest_pull",
            "summary": {"ok": ok_count, "fail": len(fail_symbols), "total": len(results)},
            "results": [asdict(r) for r in results],
        }
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"Compare report saved: {out_path}")
    
    if fail_symbols:
        print(f"Compare failed for {len(fail_symbols)} symbols, downloading aggTrades to fix...")
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120.0), headers=headers) as session:
            for sym in fail_symbols:
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
                        print(f"    Fixed {sym}: {len(klines_df)} bars aggregated from {len(trades)} trades")
                    await asyncio.sleep(0.3)
                except Exception as e:
                    print(f"    Failed to fix {sym}: {e}", file=sys.stderr)
    
    return (len(fail_symbols) == 0, results)


async def _main_async(args: argparse.Namespace) -> int:
    day = _parse_day(args.day) if args.day else _default_day_utc()
    
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
        symbols = _load_universe_symbols(target_day=day)
        if not symbols and cfg.live_universe_dir:
            print(f"\n[Step 0] Local universe not found, pulling from live machine (for {day})...")
            if pull_universe_from_live(cfg, target_day=day):
                symbols = _load_universe_symbols(target_day=day)
    
    if not symbols:
        print("No symbols provided and universe not found (both local and remote).", file=sys.stderr)
        print(f"Make sure universe exists for date <= {day.isoformat()}", file=sys.stderr)
        return 1
    
    print(f"Symbols: {len(symbols)}")
    
    if not args.compare_only:
        print(f"\n[Step 1] Pulling data from live machine...")
        klines_pulled, funding_pulled, premium_pulled = pull_day_data_from_live(day, symbols, cfg)
        print(f"  Pulled: klines={klines_pulled}, funding_rates={funding_pulled}, premium_index={premium_pulled}")
    
    print(f"\n[Step 2] Comparing with official Binance data...")
    _log_validation_criteria(cfg)
    all_ok, results = await compare_and_fix(
        day=day,
        symbols=symbols,
        cfg=cfg,
        output_dir=Path(args.output_dir or "data/compare/backtest_pull"),
    )
    
    ok = sum(1 for r in results if r.ok)
    fail = len(results) - ok
    out_dir = Path(args.output_dir or "data/compare/backtest_pull")
    report_path = out_dir / f"{day.isoformat()}.json"

    print(f"\n=== Summary ===")
    print(f"day={day.isoformat()} total={len(results)} ok={ok} fail={fail}")
    pct = (100.0 * ok / len(results)) if results else 0.0
    print(f"Data quality: {ok}/{len(results)} symbols passed ({pct:.1f}%)")
    print(f"Report (per-symbol details): {report_path}")

    if fail:
        n_missing_local = sum(1 for r in results if not r.ok and r.missing_local > 0)
        n_missing_remote = sum(1 for r in results if not r.ok and r.missing_remote > 0)
        n_mismatched = sum(1 for r in results if not r.ok and r.mismatched_rows > 0)
        print(f"Failure breakdown: {n_missing_local} with missing_local, {n_missing_remote} with missing_remote, {n_mismatched} with mismatched_rows")
        if args.print_fail:
            print("\nFailed symbols:")
            for r in results:
                if not r.ok:
                    print(f"  - {r.symbol}: missing_local={r.missing_local} missing_remote={r.missing_remote} mismatched={r.mismatched_rows}")

    return 0 if all_ok else 2


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
    parser.add_argument("--output-dir", default="data/compare/backtest_pull", help="Report output dir.")
    parser.add_argument("--compare-only", action="store_true", help="Only compare, skip pulling data.")
    parser.add_argument("--print-fail", action="store_true", help="Print failed symbols and details.")
    args = parser.parse_args()
    return asyncio.run(_main_async(args))


if __name__ == "__main__":
    raise SystemExit(main())
