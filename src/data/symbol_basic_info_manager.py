"""
币对基础信息表维护模块

目的：
- 由数据层定时拉取（默认 1 小时一次）交易所 exchangeInfo
- 解析出下单需要的 tickSize/stepSize/minQty/minNotional
- 落盘为 JSON（用于下单进程读取，避免频繁 get_exchange_info 限流）
"""

import asyncio
import json
from pathlib import Path
from typing import Any, Dict, Optional

import aiohttp

from ..common.config import config
from ..common.logger import get_logger
from ..common.network_utils import safe_http_request, log_network_error
from ..common.utils import format_symbol

logger = get_logger("symbol_basic_info_manager")


class SymbolBasicInfoManager:
    """维护 exchangeInfo -> 交易对基础信息表（JSON）"""

    def __init__(self):
        self.api_base = config.get_binance_api_base_for_data_layer()
        self.update_interval_seconds = float(
            config.get("data.symbol_basic_info_update_interval_seconds", 3600)
        )

        self.info_dir = Path(config.get("data.symbol_basic_info_directory", "data/symbol_info"))
        self.info_dir.mkdir(parents=True, exist_ok=True)
        self.table_path = Path(
            config.get(
                "data.symbol_basic_info_table_path",
                str(self.info_dir / "symbol_basic_info.json"),
            )
        )

        self.running = False
        self._task: Optional[asyncio.Task] = None

    async def fetch_exchange_info(self) -> Dict[str, Any]:
        """从 Binance 拉取 exchangeInfo（公开端点，无需签名）"""
        url = f"{self.api_base}/fapi/v1/exchangeInfo"
        async with aiohttp.ClientSession() as session:
            data = await safe_http_request(
                session,
                "GET",
                url,
                max_retries=3,
                timeout=30.0,
                return_json=True,
                use_rate_limit=True,
            )
            return data or {}

    @staticmethod
    def _parse_symbol_filters(sym_info: Dict[str, Any]) -> Dict[str, Any]:
        """解析 filters -> tick/step/min/min_notional"""
        out: Dict[str, Any] = {}
        for filter_item in sym_info.get("filters", []) or []:
            filter_type = filter_item.get("filterType", "")
            if filter_type == "PRICE_FILTER":
                tick = filter_item.get("tickSize")
                if tick is not None:
                    out["tick_size"] = float(tick)
            elif filter_type == "LOT_SIZE":
                step = filter_item.get("stepSize")
                min_qty = filter_item.get("minQty")
                if step is not None:
                    out["step_size"] = float(step)
                if min_qty is not None:
                    out["min_qty"] = float(min_qty)
            elif filter_type == "MIN_NOTIONAL":
                # Binance Futures 里一般用 notional 字段
                notional = filter_item.get("notional")
                if notional is None:
                    notional = filter_item.get("minNotional")
                if notional is not None:
                    out["min_notional"] = float(notional)
        return out

    def _load_existing_table(self) -> Dict[str, Any]:
        if not self.table_path.exists():
            return {}
        try:
            raw = self.table_path.read_text(encoding="utf-8")
            if not raw.strip():
                return {}
            data = json.loads(raw)
            return data if isinstance(data, dict) else {}
        except Exception as e:
            logger.warning(f"Failed to load existing symbol basic info table: {e}")
            return {}

    def _atomic_write_table(self, data: Dict[str, Any]) -> None:
        tmp_path = self.table_path.with_suffix(self.table_path.suffix + f".{id(self)}.tmp")
        try:
            tmp_path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
            # Windows 上也能用替换来实现原子性
            import os

            os.replace(str(tmp_path), str(self.table_path))
        finally:
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass

    async def update_once(self) -> None:
        """拉取 exchangeInfo 并覆盖/合并落盘"""
        try:
            exchange_info = await self.fetch_exchange_info()
            symbols = exchange_info.get("symbols", []) or []

            existing = self._load_existing_table()
            merged: Dict[str, Any] = dict(existing)

            for sym_info in symbols:
                sym = format_symbol(sym_info.get("symbol", ""))
                if not sym:
                    continue
                parsed = self._parse_symbol_filters(sym_info)
                if not parsed:
                    continue
                # 返回的符号：全量覆盖（只覆盖本次交易所返回到的字段/条目）
                merged[sym] = parsed

            self._atomic_write_table(merged)
            logger.info(
                f"Updated symbol basic info table: {len(symbols)} exchange symbols, total persisted symbols={len(merged)}"
            )
        except Exception as e:
            log_network_error(
                "update_symbol_basic_info_table",
                e,
                context={"api_base": self.api_base, "table_path": str(self.table_path)},
            )
            raise

    async def _scheduled_update(self) -> None:
        """定时更新：默认 1 小时一次"""
        # 先立刻更新一次，保证 table 可用
        while self.running:
            try:
                await self.update_once()
            except Exception as e:
                logger.error(f"Symbol basic info update failed: {e}", exc_info=True)
                # 出错后稍等再试，避免死循环打爆接口
                await asyncio.sleep(min(600.0, self.update_interval_seconds))

            # 正常周期等待
            await asyncio.sleep(self.update_interval_seconds)

    async def start(self) -> None:
        """启动定时维护任务"""
        if self.running:
            return
        self.running = True
        self._task = asyncio.create_task(self._scheduled_update())
        logger.info(
            f"SymbolBasicInfoManager started, interval={self.update_interval_seconds}s, table={self.table_path}"
        )

    async def stop(self) -> None:
        """停止定时任务"""
        self.running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("SymbolBasicInfoManager stopped")


_symbol_basic_info_manager: Optional[SymbolBasicInfoManager] = None


def get_symbol_basic_info_manager() -> SymbolBasicInfoManager:
    global _symbol_basic_info_manager
    if _symbol_basic_info_manager is None:
        _symbol_basic_info_manager = SymbolBasicInfoManager()
    return _symbol_basic_info_manager

