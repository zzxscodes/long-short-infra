from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Sequence, Tuple

import aiohttp
import pandas as pd

from ..common.config import config
from ..common.logger import get_logger
from ..common.utils import format_symbol
from .storage import get_data_storage

logger = get_logger("official_kline_compare")


@dataclass(frozen=True)
class FieldTolerance:
    abs: float = 0.0
    rel: float = 0.0  # relative to max(1, |remote|)

    def ok(self, local: float, remote: float) -> bool:
        try:
            diff = abs(float(local) - float(remote))
            if diff <= float(self.abs):
                return True
            denom = max(1.0, abs(float(remote)))
            return (diff / denom) <= float(self.rel)
        except Exception:
            return False


@dataclass(frozen=True)
class DailyCompareConfig:
    api_base: str = "https://fapi.binance.com"
    timeout_s: float = 15.0
    max_concurrent: int = 5
    retries: int = 3
    retry_delay_s: float = 0.6
    tolerances: Dict[str, FieldTolerance] = None  # type: ignore[assignment]

    def __post_init__(self):
        if self.tolerances is None:
            object.__setattr__(
                self,
                "tolerances",
                {
                    "open": FieldTolerance(abs=0.0, rel=0.0),
                    "high": FieldTolerance(abs=0.0, rel=0.0),
                    "low": FieldTolerance(abs=0.0, rel=0.0),
                    "close": FieldTolerance(abs=0.0, rel=0.0),
                    "volume": FieldTolerance(abs=1e-10, rel=1e-10),
                    "quote_volume": FieldTolerance(abs=1e-8, rel=1e-10),
                    "tradecount": FieldTolerance(abs=0.0, rel=0.0),
                },
            )


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


async def _fetch_official_5m_klines(
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
    url = f"{api_base.rstrip('/')}/fapi/v1/klines"
    params = {
        "symbol": format_symbol(symbol),
        "interval": "5m",
        "startTime": int(start_ms),
        "endTime": int(end_ms),
        "limit": 1500,
    }

    last_err: Optional[BaseException] = None
    for attempt in range(max(1, int(retries))):
        try:
            async with session.get(url, params=params, timeout=timeout_s) as resp:
                text = await resp.text()
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}: {text[:300]}")
                data = await resp.json()
                if not isinstance(data, list):
                    raise RuntimeError(f"Unexpected response type: {type(data).__name__}")
                return data
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                await asyncio.sleep(retry_delay_s * (2**attempt))
    raise RuntimeError(f"Failed to fetch official klines for {symbol}: {last_err}") from last_err


def _official_to_df(symbol: str, klines: List[List[Any]]) -> pd.DataFrame:
    rows = []
    for k in klines or []:
        try:
            open_ms = int(k[0])
            rows.append(
                {
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
                }
            )
        except Exception:
            continue
    if not rows:
        return pd.DataFrame(
            columns=[
                "symbol",
                "open_time_ms",
                "open_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "quote_volume",
                "tradecount",
            ]
        )
    df = pd.DataFrame(rows)
    df = df.drop_duplicates(subset=["open_time_ms"], keep="last").sort_values("open_time_ms")
    return df


def _local_day_df(symbol: str, day_start: datetime, day_end: datetime) -> pd.DataFrame:
    storage = get_data_storage()
    df = storage.load_klines(symbol, start_date=day_start, end_date=day_end)
    if df is None or df.empty or "open_time" not in df.columns:
        return pd.DataFrame(
            columns=[
                "symbol",
                "open_time_ms",
                "open_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "quote_volume",
                "tradecount",
            ]
        )

    out = df.copy()
    out["open_time"] = pd.to_datetime(out["open_time"], utc=True)
    out = out[(out["open_time"] >= day_start) & (out["open_time"] < day_end)]
    out["open_time_ms"] = (out["open_time"].astype("int64") // 1_000_000).astype("int64")

    for col in ["open", "high", "low", "close", "volume", "quote_volume", "tradecount"]:
        if col not in out.columns:
            out[col] = pd.NA

    out["symbol"] = format_symbol(symbol)
    out = out.drop_duplicates(subset=["open_time_ms"], keep="last").sort_values("open_time_ms")
    return out[
        [
            "symbol",
            "open_time_ms",
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_volume",
            "tradecount",
        ]
    ].reset_index(drop=True)


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
        remote_df,
        local_df,
        on="open_time_ms",
        how="outer",
        suffixes=("_remote", "_local"),
        indicator=True,
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
                examples.append(
                    {
                        "open_time_ms": open_ms,
                        "open_time": datetime.fromtimestamp(open_ms / 1000, tz=timezone.utc).isoformat(),
                        "diffs": diffs,
                    }
                )

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
        api_base=str(config.get("execution.live.api_base", "https://fapi.binance.com"))
    )
    day_start, day_end = _day_range_utc(day)
    start_ms = _to_ms(day_start)
    # Binance klines 的 endTime 是闭区间语义（可能包含 day_end 当根窗口），这里用 day_end-1ms 做半开区间对齐
    end_ms = _to_ms(day_end) - 1

    sem = asyncio.Semaphore(max(1, int(cfg.max_concurrent)))
    timeout = aiohttp.ClientTimeout(total=cfg.timeout_s)
    headers = {"User-Agent": "official-kline-compare/1.0"}

    results: List[SymbolCompareResult] = []

    async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
        async def one(sym: str) -> None:
            async with sem:
                remote_raw = await _fetch_official_5m_klines(
                    session,
                    api_base=cfg.api_base,
                    symbol=sym,
                    start_ms=start_ms,
                    end_ms=end_ms,
                    timeout_s=cfg.timeout_s,
                    retries=cfg.retries,
                    retry_delay_s=cfg.retry_delay_s,
                )
            remote_df = _official_to_df(sym, remote_raw)
            local_df = _local_day_df(sym, day_start, day_end)
            results.append(
                _compare_symbol_day(symbol=sym, day=day, local_df=local_df, remote_df=remote_df, cfg=cfg)
            )

        await asyncio.gather(*(one(s) for s in symbols))

    results.sort(key=lambda r: r.symbol)
    return results

