"""
每日官方 5m K线对比与同步模块

5. 线上每日任务：下载 Binance 官方 5m K线，与自聚合 K线对比
6. 对比结果：
   - 通过：将当日自聚合 K线增量同步到回测服务器
   - 不通过：重新下载当日 aggTrade、重新聚合、覆盖本地、再同步到回测服务器

默认使用 data.binance.vision 下载（不走 fapi.binance.com），避免与实盘共享限流。
独立于其他模块，可作为 cron 定时任务运行（建议 UTC 01:30）。
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import shutil
import subprocess
import zipfile
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import aiohttp
import pandas as pd

from ..common.config import config
from ..common.logger import get_logger
from ..common.utils import format_symbol, ensure_directory
from .storage import get_data_storage

logger = get_logger("daily_official_compare")

# data.binance.vision 与 fapi 不同域名，不共享限流
DATA_VISION_BASE = "https://data.binance.vision"


@dataclass(frozen=True)
class FieldTolerance:
    abs: float = 0.0
    rel: float = 0.0

    def ok(self, local: float, remote: float) -> bool:
        try:
            diff = abs(float(local) - float(remote))
            if diff <= float(self.abs):
                return True
            denom = max(1.0, abs(float(remote)))
            return (diff / denom) <= float(self.rel)
        except Exception:
            return False


@dataclass
class DailyCompareConfig:
    api_base: str = "https://fapi.binance.com"
    use_data_vision: bool = True  # 使用 data.binance.vision，避免占用 fapi 限流
    timeout_s: float = 30.0
    max_concurrent: int = 5
    retries: int = 3
    retry_delay_s: float = 0.8
    agg_trades_delay_per_symbol_s: float = 0.3
    tolerances: Dict[str, FieldTolerance] = field(default_factory=lambda: {
        "open": FieldTolerance(abs=0.0, rel=0.0),
        "high": FieldTolerance(abs=0.0, rel=0.0),
        "low": FieldTolerance(abs=0.0, rel=0.0),
        "close": FieldTolerance(abs=0.0, rel=0.0),
        "volume": FieldTolerance(abs=1e-10, rel=1e-10),
        "quote_volume": FieldTolerance(abs=1e-8, rel=1e-10),
        "tradecount": FieldTolerance(abs=0.0, rel=0.0),
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
    max_abs_diff: Dict[str, float]
    examples: List[Dict[str, Any]]


def _day_range_utc(d: date) -> Tuple[datetime, datetime]:
    start = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


def _to_ms(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


# ---------------------------------------------------------------------------
# 1. 拉取 Binance 官方 5m K线（data.binance.vision 或 REST）
# ---------------------------------------------------------------------------


async def _fetch_official_5m_klines_from_vision(
    session: aiohttp.ClientSession,
    *,
    symbol: str,
    day: date,
    timeout_s: float,
    retries: int,
    retry_delay_s: float,
) -> List[List[Any]]:
    """从 data.binance.vision 下载 5m klines ZIP，不占用 fapi 限流"""
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
            # CSV: open_time, open, high, low, close, volume, close_time, quote_volume, count, ...
            # 转为与 REST 一致的 [open_time, open, high, low, close, volume, close_time, quote_volume, count]
            result: List[List[Any]] = []
            for r in rows:
                if len(r) >= 9:
                    result.append([int(r[0]), r[1], r[2], r[3], r[4], r[5], r[6], r[7], int(r[8])])
            return result
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                await asyncio.sleep(retry_delay_s * (2 ** attempt))
    raise RuntimeError(f"Failed to fetch klines from data.binance.vision for {symbol}: {last_err}") from last_err


async def _fetch_official_5m_klines_rest(
    session: aiohttp.ClientSession,
    *,
    api_base: str,
    symbol: str,
    start_ms: int,
    end_ms: int,
    timeout_s: float,
    retries: int,
    retry_delay_s: float,
) -> List[List[Any]]:
    """REST /fapi/v1/klines，会占用 fapi 限流"""
    url = f"{api_base.rstrip('/')}/fapi/v1/klines"
    params = {
        "symbol": format_symbol(symbol),
        "interval": "5m",
        "startTime": int(start_ms),
        "endTime": int(end_ms),
        "limit": 1500,
    }
    last_err: Optional[BaseException] = None
    for attempt in range(max(1, retries)):
        try:
            async with session.get(url, params=params, timeout=timeout_s) as resp:
                text = await resp.text()
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}: {text[:300]}")
                data = json.loads(text)
                if not isinstance(data, list):
                    raise RuntimeError(f"Unexpected response type: {type(data).__name__}")
                return data
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                await asyncio.sleep(retry_delay_s * (2 ** attempt))
    raise RuntimeError(f"Failed to fetch official klines for {symbol}: {last_err}") from last_err


def _official_to_df(symbol: str, klines: List[List[Any]]) -> pd.DataFrame:
    rows = []
    for k in klines or []:
        try:
            open_ms = int(k[0])
            rows.append({
                "symbol": format_symbol(symbol),
                "open_time_ms": open_ms,
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


# ---------------------------------------------------------------------------
# 2. 加载本地自聚合 K线
# ---------------------------------------------------------------------------


def _local_day_df(symbol: str, day_start: datetime, day_end: datetime) -> pd.DataFrame:
    storage = get_data_storage()
    df = storage.load_klines(symbol, start_date=day_start, end_date=day_end)
    if df is None or df.empty or "open_time" not in df.columns:
        return pd.DataFrame(columns=[
            "symbol", "open_time_ms", "open_time", "open", "high", "low",
            "close", "volume", "quote_volume", "tradecount",
        ])
    out = df.copy()
    out["open_time"] = pd.to_datetime(out["open_time"], utc=True)
    out = out[(out["open_time"] >= day_start) & (out["open_time"] < day_end)]
    out["open_time_ms"] = (out["open_time"].astype("int64") // 1_000_000).astype("int64")
    for col in ["open", "high", "low", "close", "volume", "quote_volume"]:
        if col not in out.columns:
            out[col] = pd.NA
    if "tradecount" not in out.columns and "trade_count" in out.columns:
        out["tradecount"] = out["trade_count"]
    elif "tradecount" not in out.columns:
        out["tradecount"] = 0
    out["symbol"] = format_symbol(symbol)
    out = out.drop_duplicates(subset=["open_time_ms"], keep="last").sort_values("open_time_ms")
    return out[[
        "symbol", "open_time_ms", "open_time", "open", "high", "low",
        "close", "volume", "quote_volume", "tradecount",
    ]].reset_index(drop=True)


# ---------------------------------------------------------------------------
# 3. 对比
# ---------------------------------------------------------------------------


def _compare_symbol_day(
    *,
    symbol: str,
    day: date,
    local_df: pd.DataFrame,
    remote_df: pd.DataFrame,
    cfg: DailyCompareConfig,
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
                    "open_time": datetime.fromtimestamp(open_ms / 1000, tz=timezone.utc).isoformat(),
                    "diffs": diffs,
                })
    ok = (missing_local == 0) and (missing_remote == 0) and (mismatched_rows == 0)
    return SymbolCompareResult(
        symbol=format_symbol(symbol),
        day=str(day),
        ok=ok,
        local_rows=int(len(local_df)),
        remote_rows=int(len(remote_df)),
        missing_local=missing_local,
        missing_remote=missing_remote,
        mismatched_rows=mismatched_rows,
        max_abs_diff=max_abs_diff,
        examples=examples,
    )


async def compare_official_vs_local_5m(
    *,
    day: date,
    symbols: Sequence[str],
    cfg: Optional[DailyCompareConfig] = None,
) -> List[SymbolCompareResult]:
    cfg = cfg or DailyCompareConfig(
        api_base=str(config.get("execution.live.api_base", "https://fapi.binance.com")),
        use_data_vision=bool(config.get("data.daily_compare_use_data_vision", True)),
    )
    day_start, day_end = _day_range_utc(day)
    start_ms = _to_ms(day_start)
    end_ms = _to_ms(day_end) - 1
    sem = asyncio.Semaphore(max(1, cfg.max_concurrent))
    timeout = aiohttp.ClientTimeout(total=cfg.timeout_s)
    results: List[SymbolCompareResult] = []

    use_vision = cfg.use_data_vision
    session_headers = {"User-Agent": "Binance-Data-Vision/1.0"} if use_vision else {"User-Agent": "daily-official-compare/1.0"}

    async with aiohttp.ClientSession(timeout=timeout, headers=session_headers) as session:
        async def one(sym: str) -> None:
            async with sem:
                if use_vision:
                    remote_raw = await _fetch_official_5m_klines_from_vision(
                        session, symbol=sym, day=day,
                        timeout_s=cfg.timeout_s, retries=cfg.retries, retry_delay_s=cfg.retry_delay_s,
                    )
                else:
                    remote_raw = await _fetch_official_5m_klines_rest(
                        session, api_base=cfg.api_base, symbol=sym,
                        start_ms=start_ms, end_ms=end_ms,
                        timeout_s=cfg.timeout_s, retries=cfg.retries, retry_delay_s=cfg.retry_delay_s,
                    )
            remote_df = _official_to_df(sym, remote_raw)
            local_df = _local_day_df(sym, day_start, day_end)
            results.append(_compare_symbol_day(
                symbol=sym, day=day, local_df=local_df, remote_df=remote_df, cfg=cfg
            ))

        await asyncio.gather(*(one(s) for s in symbols))
    results.sort(key=lambda r: r.symbol)
    return results


# ---------------------------------------------------------------------------
# 4. 拉取当日 aggTrade 并重新聚合（data.binance.vision 或 REST）
# ---------------------------------------------------------------------------


async def _fetch_agg_trades_from_vision(
    session: aiohttp.ClientSession,
    *,
    symbol: str,
    day: date,
    timeout_s: float,
    retries: int,
    retry_delay_s: float,
) -> List[Dict[str, Any]]:
    """从 data.binance.vision 下载 aggTrades ZIP，不占用 fapi 限流"""
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
            # CSV: agg_trade_id, price, quantity, first_trade_id, last_trade_id, timestamp, is_buyer_maker
            # REST: a, p, q, Q, f, l, T, m
            result: List[Dict[str, Any]] = []
            for r in rows:
                if len(r) >= 7:
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
            return result
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                await asyncio.sleep(retry_delay_s * (2 ** attempt))
    raise RuntimeError(f"Failed to fetch aggTrades from data.binance.vision for {symbol}: {last_err}") from last_err


async def fetch_agg_trades_day(
    session: aiohttp.ClientSession,
    *,
    api_base: str,
    symbol: str,
    start_ms: int,
    end_ms: int,
    timeout_s: float = 15.0,
    retries: int = 3,
) -> List[Dict[str, Any]]:
    url = f"{api_base.rstrip('/')}/fapi/v1/aggTrades"
    all_rows: List[Dict[str, Any]] = []
    last_err: Optional[BaseException] = None
    params: Dict[str, Any] = {
        "symbol": format_symbol(symbol),
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": 1000,
    }
    for attempt in range(max(1, retries)):
        try:
            while True:
                async with session.get(url, params=params, timeout=timeout_s) as resp:
                    text = await resp.text()
                    if resp.status != 200:
                        raise RuntimeError(f"HTTP {resp.status}: {text[:300]}")
                    rows = json.loads(text) if isinstance(text, str) else text
                if not rows:
                    break
                all_rows.extend(rows)
                last_ts = rows[-1].get("T", 0)
                if last_ts >= end_ms - 1:
                    break
                params["startTime"] = last_ts + 1
            return all_rows
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                await asyncio.sleep(0.5 * (2 ** attempt))
    raise RuntimeError(f"Failed to fetch aggTrades for {symbol}: {last_err}") from last_err


def _aggregate_agg_trades_to_klines(
    symbol: str, trades: List[Dict[str, Any]], interval_minutes: int = 5
) -> pd.DataFrame:
    """从 aggTrade 列表批量聚合为 5m K线（与 KlineAggregator 输出格式兼容）"""
    if not trades:
        return pd.DataFrame()
    interval_seconds = interval_minutes * 60
    # 按窗口分组
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


# ---------------------------------------------------------------------------
# 5. 同步到回测服务器
# ---------------------------------------------------------------------------


def sync_klines_to_backtest(
    day: date,
    symbols: Sequence[str],
    backtest_dest: str,
    *,
    klines_dir: Optional[Path] = None,
) -> bool:
    """
    将当日各 symbol 的 K 线文件同步到回测服务器。

    路径与格式（与 src/data/storage 及 src/backtest 一致）：
    - 目录结构: backtest_dest / {SYMBOL} / YYYY-MM-DD.parquet
    - 文件格式: Parquet（与 data.klines_directory 下本地 K 线一致）
    - 列: open_time, close_time, open, high, low, close, volume, quote_volume, tradecount 等

    回测机配置：在运行 backtest 的服务器上，须将 data.klines_directory 指向同步目标根路径，
    例如 rsync 到 user@host:/data/backtest_klines 时，该主机配置中应设 data.klines_directory: /data/backtest_klines，
    这样 src/backtest 通过 get_data_storage().load_klines() 读取的路径与同步布局一致。

    backtest_dest: 本地路径（如 /path/to/backtest/klines）或 rsync 目标（如 user@host:/path/klines）
    """
    storage = get_data_storage()
    base_dir = klines_dir or storage.klines_dir
    day_str = day.isoformat()
    synced = 0
    if ":" in backtest_dest:
        for sym in symbols:
            sym_dir = base_dir / format_symbol(sym)
            src = sym_dir / f"{day_str}.parquet"
            if src.exists():
                dest_dir = backtest_dest.rstrip("/")
                cmd = ["rsync", "-az", str(src), f"{dest_dir}/{sym}/"]
                try:
                    subprocess.run(cmd, check=True, capture_output=True, timeout=60)
                    synced += 1
                except Exception as e:
                    logger.error(f"rsync failed for {sym}: {e}")
        logger.info(f"Synced {synced}/{len(symbols)} kline files to {backtest_dest}")
        return synced == len(symbols)
    else:
        dest_path = Path(backtest_dest)
        ensure_directory(str(dest_path))
        for sym in symbols:
            sym_dir = base_dir / format_symbol(sym)
            src = sym_dir / f"{day_str}.parquet"
            if src.exists():
                d = dest_path / format_symbol(sym)
                d.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, d / f"{day_str}.parquet")
                synced += 1
        logger.info(f"Synced {synced}/{len(symbols)} kline files to {dest_path}")
        return synced == len(symbols)


def _sync_day_parquet_to_backtest(
    day: date,
    symbols: Sequence[str],
    backtest_dest: str,
    source_dir: Path,
    label: str,
) -> bool:
    """通用：将当日各 symbol 的 {source_dir}/{SYMBOL}/{YYYY-MM-DD}.parquet 同步到 backtest_dest，与 K 线同理。"""
    day_str = day.isoformat()
    synced = 0
    if ":" in backtest_dest:
        dest_dir = backtest_dest.rstrip("/")
        for sym in symbols:
            sym_formatted = format_symbol(sym)
            src = source_dir / sym_formatted / f"{day_str}.parquet"
            if src.exists():
                try:
                    cmd = ["rsync", "-az", str(src), f"{dest_dir}/{sym_formatted}/"]
                    subprocess.run(cmd, check=True, capture_output=True, timeout=60)
                    synced += 1
                except Exception as e:
                    logger.error(f"rsync {label} failed for {sym_formatted}: {e}")
        logger.info(f"Synced {synced}/{len(symbols)} {label} files to {backtest_dest}")
    else:
        dest_path = Path(backtest_dest)
        ensure_directory(str(dest_path))
        for sym in symbols:
            sym_formatted = format_symbol(sym)
            src = source_dir / sym_formatted / f"{day_str}.parquet"
            if src.exists():
                d = dest_path / sym_formatted
                d.mkdir(parents=True, exist_ok=True)
                shutil.copy2(src, d / f"{day_str}.parquet")
                synced += 1
        logger.info(f"Synced {synced}/{len(symbols)} {label} files to {dest_path}")
    return synced == len(symbols)


def sync_funding_rates_to_backtest(
    day: date,
    symbols: Sequence[str],
    backtest_dest: str,
    *,
    funding_rates_dir: Optional[Path] = None,
) -> bool:
    """
    将当日各 symbol 的 funding_rates 同步到回测机（不做对比，仅同步）。
    布局与 K 线一致: backtest_dest / {SYMBOL} / YYYY-MM-DD.parquet。
    回测机须将 data.funding_rates_directory 指向 backtest_dest 根路径。
    """
    storage = get_data_storage()
    base = funding_rates_dir or storage.funding_rates_dir
    return _sync_day_parquet_to_backtest(day, symbols, backtest_dest, base, "funding_rates")


def sync_premium_index_to_backtest(
    day: date,
    symbols: Sequence[str],
    backtest_dest: str,
    *,
    premium_index_dir: Optional[Path] = None,
) -> bool:
    """
    将当日各 symbol 的 premium_index 同步到回测机（不做对比，仅同步）。
    布局与 K 线一致: backtest_dest / {SYMBOL} / YYYY-MM-DD.parquet。
    回测机须将 data.premium_index_directory 指向 backtest_dest 根路径。
    """
    storage = get_data_storage()
    base = premium_index_dir or storage.premium_index_dir
    return _sync_day_parquet_to_backtest(day, symbols, backtest_dest, base, "premium_index")


# ---------------------------------------------------------------------------
# 6. 完整流程
# ---------------------------------------------------------------------------


async def run_daily_compare_and_sync(
    *,
    day: date,
    symbols: Sequence[str],
    backtest_dest: Optional[str] = None,
    backtest_funding_rates_dest: Optional[str] = None,
    backtest_premium_index_dest: Optional[str] = None,
    cfg: Optional[DailyCompareConfig] = None,
    output_dir: Optional[Path] = None,
) -> Tuple[bool, List[SymbolCompareResult]]:
    """
    执行每日对比与同步：
    1. 对比官方 5m vs 自聚合 5m
    2. 若全部通过：将自聚合 K 线同步到 backtest_dest
    3. 若不通过：对失败的 symbol 重新拉 aggTrade、聚合、覆盖本地、再同步
    4. 若配置了 backtest_funding_rates_dest / backtest_premium_index_dest，同步当日 funding_rates / premium_index（不做对比）
    """
    cfg = cfg or DailyCompareConfig(
        api_base=str(config.get("execution.live.api_base", "https://fapi.binance.com")),
        use_data_vision=bool(config.get("data.daily_compare_use_data_vision", True)),
    )
    backtest_dest = backtest_dest or str(config.get("data.backtest_klines_dest", "data/backtest_klines"))
    storage = get_data_storage()
    day_start, day_end = _day_range_utc(day)
    start_ms = _to_ms(day_start)
    end_ms = _to_ms(day_end) - 1

    results = await compare_official_vs_local_5m(day=day, symbols=list(symbols), cfg=cfg)
    ok_count = sum(1 for r in results if r.ok)
    fail_symbols = [r.symbol for r in results if not r.ok]

    if output_dir:
        ensure_directory(str(output_dir))
        out_path = output_dir / f"{day.isoformat()}.json"
        payload = {
            "day": day.isoformat(),
            "summary": {"ok": ok_count, "fail": len(fail_symbols), "total": len(results)},
            "results": [asdict(r) for r in results],
        }
        out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info(f"Compare report saved: {out_path}")

    if fail_symbols:
        logger.warning(f"Compare failed for {fail_symbols}, re-downloading aggTrades and re-aggregating")
        timeout = aiohttp.ClientTimeout(total=60.0)
        headers = {"User-Agent": "Binance-Data-Vision/1.0"} if cfg.use_data_vision else {"User-Agent": "daily-official-compare/1.0"}
        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            for sym in fail_symbols:
                try:
                    if cfg.use_data_vision:
                        trades = await _fetch_agg_trades_from_vision(
                            session, symbol=sym, day=day,
                            timeout_s=cfg.timeout_s, retries=cfg.retries, retry_delay_s=cfg.retry_delay_s,
                        )
                    else:
                        trades = await fetch_agg_trades_day(
                            session, api_base=cfg.api_base, symbol=sym,
                            start_ms=start_ms, end_ms=end_ms,
                            timeout_s=cfg.timeout_s, retries=cfg.retries,
                        )
                    await asyncio.sleep(cfg.agg_trades_delay_per_symbol_s)
                    klines_df = _aggregate_agg_trades_to_klines(sym, trades)
                    if klines_df.empty:
                        logger.warning(f"No klines from aggTrades for {sym}")
                        continue
                    day_dt = datetime.combine(day, datetime.min.time()).replace(tzinfo=timezone.utc)
                    storage.save_klines(sym, klines_df, date=day_dt)
                    logger.info(f"Re-aggregated and saved klines for {sym} ({len(klines_df)} bars)")
                except Exception as e:
                    logger.error(f"Failed to re-aggregate {sym}: {e}", exc_info=True)

    if backtest_dest:
        sync_klines_to_backtest(day, symbols, backtest_dest)
    funding_dest = backtest_funding_rates_dest or str(config.get("data.backtest_funding_rates_dest", "")).strip()
    if funding_dest:
        sync_funding_rates_to_backtest(day, symbols, funding_dest)
    premium_dest = backtest_premium_index_dest or str(config.get("data.backtest_premium_index_dest", "")).strip()
    if premium_dest:
        sync_premium_index_to_backtest(day, symbols, premium_dest)
    return (len(fail_symbols) == 0, results)
