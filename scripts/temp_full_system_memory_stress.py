#!/usr/bin/env python3
"""
临时脚本：离线状态下对整套实盘系统进行 540 symbols × 30 天数据量的极端内存压力测试。

与实盘机制完全一致（仅脚本离线、无 IPC/WebSocket/网络）：
- 数据种类：K线(klines)、逐笔成交(trades)、资金费率(funding_rates)、溢价指数(premium_index)
- K 线来源：与实盘相同，由逐笔经 add_trade → pending_trades → flush_closed_windows 产生（可选 --prefill-klines 快速预填）
- 保存/清理：与 _periodic_save 完全同一套逻辑（flush_pending_snapshot → flush_closed_windows → 批量 save_klines → force_cleanup_klines → save trades → flush funding/premium → cleanup_old_data 按 cleanup_interval）
- 配置：data.cleanup_interval、data.memory_cleanup_interval 等与实盘一致

运行方式：离线（mock 模式），不依赖 IPC/WebSocket/网络。
"""

import argparse
import asyncio
import csv
import gc
import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Any

import pandas as pd
import polars as pl

try:
    import psutil
except ImportError:
    psutil = None

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.common.config import config
from src.common.utils import format_symbol
from src.processes import data_layer as data_layer_module
from src.processes.data_layer import DataLayerProcess


# ---------- 离线：不依赖 IPC ----------
class NoopIPCClient:
    async def connect(self):
        return None

    async def disconnect(self):
        return None

    async def send_message(self, _message):
        return None

    async def send_data_complete(self, _timestamp, _symbols):
        return None

    async def send_target_position_ready(self, _account_id, _file_path):
        return None

    async def send_order_executed(self, _account_id, _order_info):
        return None


def build_symbols(count: int) -> List[str]:
    base = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT"]
    if count <= len(base):
        return base[:count]
    extra = [f"MOCK{i:04d}USDT" for i in range(1, count - len(base) + 1)]
    return base + extra


def write_universe(symbols: List[str]) -> Path:
    day_dir = PROJECT_ROOT / "data" / "universe" / datetime.now().strftime("%Y-%m-%d")
    day_dir.mkdir(parents=True, exist_ok=True)
    target = day_dir / "universe.csv"
    with open(target, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["symbol"])
        for s in symbols:
            writer.writerow([s])
    return target


# ---------- 30 天 5 分钟 K 线：每 symbol 8640 根 ----------
KLINES_30D_COUNT = 30 * 24 * (60 // 5)  # 8640
INTERVAL_MINUTES = 5


def _make_kline_row(symbol: str, i: int, base_ts: datetime) -> Dict[str, Any]:
    """单根 K 线 dict，与 kline_aggregator 产出结构一致（bar + tran_stats）。"""
    window_start = base_ts + timedelta(minutes=i * INTERVAL_MINUTES)
    window_end = window_start + timedelta(minutes=INTERVAL_MINUTES)
    day_start = window_start.replace(hour=0, minute=0, second=0, microsecond=0)
    minutes_since_midnight = (window_start - day_start).total_seconds() / 60
    time_lable = int(minutes_since_midnight // INTERVAL_MINUTES) + 1
    price = 100.0 + (i % 1000) * 0.01
    return {
        "symbol": symbol,
        "open_time": window_start,
        "close_time": window_end,
        "quote_volume": 1e6,
        "trade_count": 100,
        "interval_minutes": INTERVAL_MINUTES,
        "microsecond_since_trade": int(window_end.timestamp() * 1_000_000),
        "span_begin_datetime": int(window_start.timestamp() * 1000),
        "span_end_datetime": int(window_end.timestamp() * 1000),
        "span_status": "",
        "high": price * 1.01,
        "low": price * 0.99,
        "open": price,
        "close": price,
        "vwap": price,
        "dolvol": 1e6,
        "buydolvol": 5e5,
        "selldolvol": 5e5,
        "volume": 1000.0,
        "buyvolume": 500.0,
        "sellvolume": 500.0,
        "tradecount": 100,
        "buytradecount": 50,
        "selltradecount": 50,
        "time_lable": time_lable,
        "buy_volume": 500.0,
        "buy_dolvol": 5e5,
        "buy_trade_count": 50,
        "sell_volume": 500.0,
        "sell_dolvol": 5e5,
        "sell_trade_count": 50,
        "buy_volume1": 100.0,
        "buy_volume2": 100.0,
        "buy_volume3": 100.0,
        "buy_volume4": 200.0,
        "buy_dolvol1": 1e4,
        "buy_dolvol2": 1e4,
        "buy_dolvol3": 1e4,
        "buy_dolvol4": 2e4,
        "buy_trade_count1": 10,
        "buy_trade_count2": 10,
        "buy_trade_count3": 10,
        "buy_trade_count4": 20,
        "sell_volume1": 100.0,
        "sell_volume2": 100.0,
        "sell_volume3": 100.0,
        "sell_volume4": 200.0,
        "sell_dolvol1": 1e4,
        "sell_dolvol2": 1e4,
        "sell_dolvol3": 1e4,
        "sell_dolvol4": 2e4,
        "sell_trade_count1": 10,
        "sell_trade_count2": 10,
        "sell_trade_count3": 10,
        "sell_trade_count4": 20,
    }


def build_klines_30d_polars(symbols: List[str], base_ts: datetime) -> Dict[str, pl.DataFrame]:
    """为每个 symbol 生成 30 天 5m K 线的 Polars DataFrame（与 data_layer 内存结构一致）。"""
    out = {}
    for symbol in symbols:
        rows = [_make_kline_row(symbol, i, base_ts) for i in range(KLINES_30D_COUNT)]
        df = pl.DataFrame(rows)
        df = df.with_columns(
            pl.col("open_time").cast(pl.Datetime("ns", time_zone="UTC")),
            pl.col("close_time").cast(pl.Datetime("ns", time_zone="UTC")),
        )
        out[symbol] = df
    return out


# ---------- 逐笔成交：mock 格式，用于 trades_buffer ----------
def make_mock_trade(symbol: str, ts_ms: int, price: float = 100.0) -> Dict:
    return {
        "symbol": format_symbol(symbol),
        "tradeId": ts_ms % 10_000_000,
        "price": price,
        "qty": 1.0,
        "quoteQty": price,
        "isBuyerMaker": False,
        "ts": pd.Timestamp(ts_ms, unit="ms", tz="UTC"),
        "ts_ms": ts_ms,
        "ts_us": ts_ms * 1000,
    }


def build_trades_buffer(symbols: List[str], trades_per_symbol: int, base_ts: datetime) -> Dict[str, List[dict]]:
    """预填 trades_buffer：每 symbol 若干条，用于压力测试 _save_trades_batch。"""
    buf = {}
    base_ms = int(base_ts.timestamp() * 1000)
    for symbol in symbols:
        buf[symbol] = [
            make_mock_trade(symbol, base_ms + i * 60_000, 100.0 + (i % 100) * 0.01)
            for i in range(trades_per_symbol)
        ]
    return buf


# ---------- 资金费率：30 天每 8h 一条 = 90 条/symbol ----------
FUNDING_COUNT_30D = 30 * 3  # 90


def build_funding_rate_buffer(symbols: List[str], base_ts: datetime) -> Dict[str, List[Dict]]:
    """预填 _ws_funding_rate_buffer（与 _on_ws_funding_rate 写入格式一致）。"""
    buf = {}
    for symbol in symbols:
        records = []
        for i in range(FUNDING_COUNT_30D):
            t = base_ts + timedelta(hours=i * 8)
            funding_time_ms = int(t.timestamp() * 1000)
            records.append({
                "fundingTime": funding_time_ms,
                "fundingRate": 0.0001 * (i % 10),
                "markPrice": 100.0 + (i % 50),
            })
        buf[symbol] = records
    return buf


# ---------- 溢价指数 K 线：30 天 5m = 8640 条/symbol（与 K 线同量级） ----------
def build_premium_index_buffer(symbols: List[str], base_ts: datetime) -> Dict[str, List[Dict]]:
    """预填 _ws_premium_index_buffer（每条含 open_time 等，与 save_premium_index_klines 一致）。"""
    buf = {}
    for symbol in symbols:
        klines = []
        for i in range(KLINES_30D_COUNT):
            ot = base_ts + timedelta(minutes=i * INTERVAL_MINUTES)
            ct = ot + timedelta(minutes=INTERVAL_MINUTES)
            klines.append({
                "open_time": pd.Timestamp(ot),
                "close_time": pd.Timestamp(ct),
                "open": 0.0001,
                "high": 0.0002,
                "low": 0.00005,
                "close": 0.00015,
                "volume": 1000.0,
            })
        buf[symbol] = klines
    return buf


def pending_trade_count(pending_trades: Dict[str, Dict[int, List]]) -> int:
    total = 0
    for windows in pending_trades.values():
        for trades in windows.values():
            total += len(trades)
    return total


async def run_one_save_cycle(process: DataLayerProcess) -> None:
    """
    执行一次与实盘 _periodic_save 完全一致的保存周期（无 sleep）。
    顺序与 data_layer._periodic_save 内层逻辑一致。
    """
    if process.kline_aggregator:
        await process.kline_aggregator.flush_pending_snapshot()
        await process._check_and_notify_data_complete()
        await process.kline_aggregator.flush_closed_windows()
        universe = process.universe_manager.current_universe
        if universe:
            await process.kline_aggregator.check_and_generate_empty_windows(list(universe))

    if process.kline_aggregator and not process._kline_reconcile_enabled:
        saved_count = 0
        symbols_to_save = list(process.kline_aggregator.klines.keys())
        batch_size = 50
        for i in range(0, len(symbols_to_save), batch_size):
            batch_symbols = symbols_to_save[i : i + batch_size]
            for symbol in batch_symbols:
                if symbol not in process.kline_aggregator.klines:
                    continue
                df_pl = process.kline_aggregator.klines[symbol]
                if not df_pl.is_empty():
                    try:
                        process.storage.save_klines(symbol, df_pl)
                        saved_count += 1
                    except Exception as e:
                        from src.common.logger import get_logger
                        get_logger("data_layer").error(f"Failed to save klines for {symbol}: {e}", exc_info=True)
            gc.collect()
        max_klines = int(config.get("data.kline_aggregator_max_klines", 288))
        cleaned_count, total_trimmed = process.kline_aggregator.force_cleanup_klines(
            max_klines, force_rebuild_all=False
        )
        if cleaned_count > 0 and total_trimmed > 0:
            from src.common.logger import get_logger
            get_logger("data_layer").info(
                f"Periodic save trim completed: cleaned={cleaned_count}, "
                f"trimmed={total_trimmed}, max_klines={max_klines}"
            )

    for symbol in list(process.trades_buffer.keys()):
        if process.trades_buffer[symbol]:
            await process._save_trades_batch(symbol)
    empty_symbols = [
        sym for sym, buf in process.trades_buffer.items() if not buf
    ]
    for sym in empty_symbols:
        del process.trades_buffer[sym]

    await process._flush_ws_funding_rate_buffer()
    await process._flush_ws_premium_index_buffer()
    process.last_save_time = time.time()

    current_time = time.time()
    if current_time - process.last_cleanup_time >= process.cleanup_interval:
        max_days = config.get("data.max_history_days", 30)
        process.storage.cleanup_old_data(days=max_days)
        process.last_cleanup_time = current_time


def _trade_for_window(symbol: str, window_start_ms: int, price: float = 100.0) -> Dict:
    """生成一个 5 分钟窗口内的单笔 trade（用于逐笔驱动 K 线）。"""
    return make_mock_trade(symbol, window_start_ms + 60_000, price)


async def build_klines_from_trades(
    process: DataLayerProcess,
    symbols: List[str],
    base_ts: datetime,
    trades_per_window: int = 1,
    yield_every: int = 50000,
) -> int:
    """
    与实盘一致：通过 _on_trade_received 注入逐笔，再 flush_pending_snapshot + flush_closed_windows 产生 K 线。
    返回注入的 trade 数量。
    """
    base_ms = int(base_ts.timestamp() * 1000)
    interval_ms = INTERVAL_MINUTES * 60 * 1000
    now_ms = int((base_ts + timedelta(days=31)).timestamp() * 1000)

    total = 0
    for i in range(KLINES_30D_COUNT):
        window_start_ms = base_ms + i * interval_ms
        price = 100.0 + (i % 1000) * 0.01
        for symbol in symbols:
            for _ in range(trades_per_window):
                trade = _trade_for_window(symbol, window_start_ms, price)
                await process._on_trade_received(symbol, trade)
                total += 1
                if total % yield_every == 0:
                    await asyncio.sleep(0)
    await process.kline_aggregator.flush_pending_snapshot()
    await process.kline_aggregator.flush_closed_windows(now_ms=now_ms)
    return total


async def run(args: argparse.Namespace) -> Path:
    symbols = build_symbols(args.symbols)
    universe_file = write_universe(symbols)

    config.set("execution.mode", "mock")
    config.set("data.kline_reconcile_enabled", False)
    config.set("data.save_interval", args.save_interval)
    config.set("data.memory_cleanup_interval", args.memory_cleanup_interval)
    config.set("data.kline_aggregator_max_klines", args.max_klines)
    config.set("data.kline_aggregator_max_pending_windows", args.max_pending_windows)
    config.set("data.kline_aggregator_max_trades_per_window", args.max_trades_per_window)
    config.set("data.kline_close_grace_windows", args.close_grace_windows)
    config.set("data.trades_buffer_max_size", args.trades_buffer_max_size)
    config.set("data.trades_buffer_total_max_size", args.trades_buffer_total_max_size)
    config.set("data.cleanup_interval", args.cleanup_interval)
    # 使用独立目录，避免污染实盘 data
    stress_data = PROJECT_ROOT / "data" / "stress_test"
    config.set("data.data_directory", str(stress_data))
    config.set("data.klines_directory", str(stress_data / "klines"))
    config.set("data.trades_directory", str(stress_data / "trades"))
    config.set("data.funding_rates_directory", str(stress_data / "funding_rates"))
    config.set("data.premium_index_directory", str(stress_data / "premium_index"))

    data_layer_module.IPCClient = NoopIPCClient
    process = DataLayerProcess()
    local_proc = psutil.Process(os.getpid()) if psutil else None
    started = time.time()
    metrics = []

    build_from_trades = args.build_klines_from_trades and not args.prefill_klines
    print("=" * 88)
    print("Full-system (data_layer) offline memory stress: 540 symbols × 30 days")
    print(f"Universe: {universe_file}")
    print(f"Data dir: {stress_data}")
    print(
        f"Config: max_klines={args.max_klines} duration={args.duration}s "
        f"sample_interval={args.sample_interval}s cleanup_interval={args.cleanup_interval}s"
    )
    print(f"K-line source: {'trades (same as live)' if build_from_trades else 'prefill (fast)'}")
    print("=" * 88)

    await process.start()
    # 使第一次 run_one_save_cycle 内触发 cleanup_old_data（与实盘首周期一致）
    process.last_cleanup_time = time.time() - process.cleanup_interval

    run_error = None
    rss_baseline = rss_after_klines = rss_after_trades = rss_after_funding = rss_after_premium = 0.0
    rss_after_save = rss_after_cleanup = 0.0

    try:
        # ----- 基准 RSS（仅 mock collector 运行） -----
        await asyncio.sleep(2)
        rss_baseline = local_proc.memory_info().rss / 1024 / 1024 if local_proc else 0
        print(f"[baseline] RSS ≈ {rss_baseline:.2f} MB")

        base_ts = datetime.now(timezone.utc) - timedelta(days=30)

        if build_from_trades:
            # ----- 与实盘一致：逐笔 → add_trade/trades_buffer → flush 产生 K 线 -----
            print("Prefilling _ws_funding_rate_buffer and _ws_premium_index_buffer ...")
            process._ws_funding_rate_buffer.update(build_funding_rate_buffer(symbols, base_ts))
            process._ws_premium_index_buffer.update(build_premium_index_buffer(symbols, base_ts))
            rss_after_funding = local_proc.memory_info().rss / 1024 / 1024 if local_proc else 0
            rss_after_premium = rss_after_funding
            print("Building klines from trades (same path as live: _on_trade_received → flush) ...")
            n_trades = await build_klines_from_trades(
                process, symbols, base_ts,
                trades_per_window=args.trades_per_window,
                yield_every=args.trades_yield_every,
            )
            rss_after_klines = local_proc.memory_info().rss / 1024 / 1024 if local_proc else 0
            rss_after_trades = rss_after_klines
            print(f"  -> injected {n_trades} trades, klines built via flush, RSS ≈ {rss_after_klines:.2f} MB")
        else:
            # ----- 快速路径：直接预填 K 线 + trades/funding/premium -----
            print("Prefilling klines (30d × 5m) ...")
            klines_30d = build_klines_30d_polars(symbols, base_ts)
            for sym, df in klines_30d.items():
                process.kline_aggregator.klines[sym] = df
            del klines_30d
            rss_after_klines = local_proc.memory_info().rss / 1024 / 1024 if local_proc else 0
            print(f"  -> klines: {len(symbols)} × {KLINES_30D_COUNT} bars, RSS ≈ {rss_after_klines:.2f} MB")

            print("Prefilling trades_buffer ...")
            trades_buf = build_trades_buffer(symbols, args.trades_per_symbol, base_ts)
            process.trades_buffer.update(trades_buf)
            process._trades_buffer_total_size = sum(len(v) for v in trades_buf.values())
            del trades_buf
            rss_after_trades = local_proc.memory_info().rss / 1024 / 1024 if local_proc else 0
            print(f"  -> trades_buffer: {process._trades_buffer_total_size} total, RSS ≈ {rss_after_trades:.2f} MB")

            print("Prefilling _ws_funding_rate_buffer ...")
            process._ws_funding_rate_buffer.update(build_funding_rate_buffer(symbols, base_ts))
            rss_after_funding = local_proc.memory_info().rss / 1024 / 1024 if local_proc else 0
            print(f"  -> funding: {len(symbols)} × {FUNDING_COUNT_30D}, RSS ≈ {rss_after_funding:.2f} MB")

            print("Prefilling _ws_premium_index_buffer ...")
            process._ws_premium_index_buffer.update(build_premium_index_buffer(symbols, base_ts))
            rss_after_premium = local_proc.memory_info().rss / 1024 / 1024 if local_proc else 0
            print(f"  -> premium_index: {len(symbols)} × {KLINES_30D_COUNT}, RSS ≈ {rss_after_premium:.2f} MB")

        # ----- 与实盘完全一致：执行一次 _periodic_save 周期 -----
        print("Running one save cycle (same as _periodic_save: flush → save klines → force_cleanup_klines → trades → funding/premium → cleanup_old_data) ...")
        await run_one_save_cycle(process)
        rss_after_save = local_proc.memory_info().rss / 1024 / 1024 if local_proc else 0
        rss_after_cleanup = rss_after_save
        print(f"  -> RSS after save cycle ≈ {rss_after_save:.2f} MB")

        # ----- 持续采样一段时间（mock 继续打 trade，模拟实盘持续运行） -----
        while True:
            elapsed = time.time() - started
            if elapsed >= args.duration:
                break
            rss_mb = local_proc.memory_info().rss / 1024 / 1024 if local_proc else 0
            row = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "elapsed_sec": round(elapsed, 2),
                "rss_mb": round(rss_mb, 2),
                "pending_symbols": 0,
                "pending_windows": 0,
                "pending_trades_total": 0,
                "klines_symbols": 0,
                "klines_total": 0,
            }
            if process.kline_aggregator:
                agg = process.kline_aggregator
                row["pending_symbols"] = len(agg.pending_trades)
                row["pending_windows"] = sum(len(v) for v in agg.pending_trades.values())
                row["pending_trades_total"] = pending_trade_count(agg.pending_trades)
                row["klines_symbols"] = len(agg.klines)
                row["klines_total"] = sum(
                    len(df) for df in agg.klines.values() if not df.is_empty()
                )
            metrics.append(row)
            print(
                f"[{row['elapsed_sec']:7.2f}s] rss={row['rss_mb']:8.2f} MB "
                f"klines_total={row['klines_total']:6} pending_trades={row['pending_trades_total']:9}"
            )
            await asyncio.sleep(args.sample_interval)
    except Exception as e:
        run_error = str(e)
        import traceback
        traceback.print_exc()
    finally:
        if args.fast_stop:
            for task in list(process.tasks):
                try:
                    task.cancel()
                except Exception:
                    pass
            if process.tasks:
                await asyncio.gather(*process.tasks, return_exceptions=True)
            if process.collector:
                await process.collector.stop()
            if process.universe_manager:
                await process.universe_manager.stop()
            if process.ipc_client:
                await process.ipc_client.disconnect()
            process.running = False
        else:
            await process.stop()

    # ----- 报告 -----
    logs_dir = PROJECT_ROOT / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    report = logs_dir / f"full_system_memory_stress_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    summary = {
        "config": vars(args),
        "symbols": len(symbols),
        "report_generated_at": datetime.now(timezone.utc).isoformat(),
        "run_error": run_error,
        "rss_baseline_mb": round(rss_baseline, 2) if local_proc else None,
        "rss_after_klines_mb": round(rss_after_klines, 2) if local_proc else None,
        "rss_after_trades_mb": round(rss_after_trades, 2) if local_proc else None,
        "rss_after_funding_mb": round(rss_after_funding, 2) if local_proc else None,
        "rss_after_premium_mb": round(rss_after_premium, 2) if local_proc else None,
        "rss_after_save_mb": round(rss_after_save, 2) if local_proc else None,
        "rss_after_cleanup_mb": round(rss_after_cleanup, 2) if local_proc else None,
        "data_directory_used": str(stress_data),
        "metrics": metrics,
    }
    with open(report, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=True)
    print(f"Report saved: {report}")
    return report


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Offline full-system memory stress: 540 symbols × 30 days (same mechanism as live)"
    )
    p.add_argument("--symbols", type=int, default=540)
    p.add_argument("--duration", type=int, default=120, help="Sampling duration after prefill (seconds)")
    p.add_argument("--sample-interval", type=int, default=15)
    p.add_argument("--save-interval", type=int, default=60)
    p.add_argument("--memory-cleanup-interval", type=int, default=60)
    p.add_argument("--cleanup-interval", type=int, default=3600, help="data.cleanup_interval (seconds), same as live")
    p.add_argument("--max-klines", type=int, default=8640, help="30d 5m = 8640")
    p.add_argument("--max-pending-windows", type=int, default=8)
    p.add_argument("--max-trades-per-window", type=int, default=0)
    p.add_argument("--close-grace-windows", type=int, default=6)
    p.add_argument("--trades-buffer-max-size", type=int, default=500)
    p.add_argument("--trades-buffer-total-max-size", type=int, default=300000)
    p.add_argument("--trades-per-symbol", type=int, default=400, help="Prefill trades per symbol (prefill-klines path)")
    p.add_argument(
        "--build-klines-from-trades",
        action="store_true",
        default=True,
        help="Build klines from trades via _on_trade_received (same as live); default True",
    )
    p.add_argument(
        "--prefill-klines",
        action="store_true",
        default=False,
        help="Fast path: prefill klines directly instead of building from trades",
    )
    p.add_argument("--trades-per-window", type=int, default=1, help="Trades per 5m window when building from trades")
    p.add_argument("--trades-yield-every", type=int, default=50000, help="Yield to event loop every N trades")
    p.add_argument("--fast-stop", action="store_true", default=True)
    return p.parse_args()


def main() -> int:
    asyncio.run(run(parse_args()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
