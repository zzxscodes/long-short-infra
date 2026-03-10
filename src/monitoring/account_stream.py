"""
监控用账户/持仓 WebSocket 流
通过 User Data Stream 订阅 ACCOUNT_UPDATE 事件，实时维护账户与持仓缓存，
替代 REST get_account_info / get_positions 轮询，减少 API 请求、避免限流。

启动时用 REST 做全量快照，之后由 WebSocket 增量更新。
"""
import json
import time
import asyncio
import random
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException
from typing import Dict, List, Optional, Any, TYPE_CHECKING

from ..common.config import config
from ..common.logger import get_logger
from ..common.utils import format_symbol
from ..common.network_utils import log_network_error, ConnectionTimeoutError

if TYPE_CHECKING:
    from ..execution.binance_client import BinanceClient

logger = get_logger('account_stream')


class AccountStream:
    """
    监控用账户/持仓 WebSocket 流

    订阅 Binance Futures User Data Stream，解析 ACCOUNT_UPDATE 事件，
    维护与 REST 相同结构的 account_info 和 positions 缓存。
    启动时用 REST 全量拉取，之后由 WebSocket 增量更新。
    """

    def __init__(self, account_id: str, client: "BinanceClient", mark_price_cache: Optional[Any] = None):
        """
        Args:
            account_id: 账户 ID（用于日志）
            client: BinanceClient（用于 listenKey、REST 快照）
            mark_price_cache: MarkPriceCache 实例，用于补充持仓的 markPrice（可选）
        """
        self.account_id = account_id
        self.client = client
        self.mark_price_cache = mark_price_cache

        self.ws_base = config.get_binance_ws_base()
        self.reconnect_delay = 5
        self.keepalive_interval = int(config.get("monitoring.account_stream_keepalive_minutes", 30)) * 60
        self.ping_interval = int(config.get("monitoring.account_stream_ping_interval_seconds", 20))
        self.ping_timeout = int(config.get("monitoring.account_stream_ping_timeout_seconds", 30))
        self.open_timeout = int(config.get("monitoring.account_stream_open_timeout_seconds", 30))

        self._account_info: Dict[str, Any] = {}
        self._positions: List[Dict] = []
        self._positions_by_key: Dict[str, Dict] = {}  # (symbol, ps) -> pos
        self._listen_key: Optional[str] = None
        self._ws_task: Optional[asyncio.Task] = None
        self.running = False
        self._connected = False
        self._last_rest_snapshot_ts: float = 0.0

        self.stats = {
            'messages_received': 0,
            'reconnect_count': 0,
            'rest_snapshots': 0,
            'start_time': time.time(),
        }

    def get_account_info(self) -> Dict[str, Any]:
        """获取缓存的账户信息（与 REST /fapi/v2/account 结构一致）"""
        return dict(self._account_info)

    def get_positions(self) -> List[Dict]:
        """获取缓存的持仓列表（与 REST /fapi/v2/positionRisk 结构一致，仅非零持仓）"""
        return [p for p in self._positions if abs(float(p.get('positionAmt', 0))) > 1e-8]

    def is_ready(self) -> bool:
        """是否有有效缓存（至少做过一次 REST 快照）"""
        return len(self._account_info) > 0 or self._last_rest_snapshot_ts > 0

    def last_snapshot_time(self) -> float:
        """最后一次 REST 快照时间"""
        return self._last_rest_snapshot_ts

    async def _rest_snapshot(self) -> bool:
        """REST 全量快照"""
        try:
            account_info = await self.client.get_account_info()
            positions = await self.client.get_positions()
            self._apply_rest_snapshot(account_info, positions)
            self._last_rest_snapshot_ts = time.time()
            self.stats['rest_snapshots'] += 1
            logger.info(f"[{self.account_id}] Account stream REST snapshot ok, {len(positions)} positions")
            return True
        except Exception as e:
            logger.error(f"[{self.account_id}] Account stream REST snapshot failed: {e}", exc_info=True)
            return False

    def _apply_rest_snapshot(self, account_info: Dict, positions: List[Dict]):
        """应用 REST 快照到缓存"""
        self._account_info = dict(account_info)
        self._positions = []
        self._positions_by_key = {}
        for pos in positions:
            pos_copy = dict(pos)
            self._positions.append(pos_copy)
            symbol = format_symbol(pos.get('symbol', ''))
            ps = pos.get('positionSide', 'BOTH')
            self._positions_by_key[(symbol, ps)] = pos_copy

    def _merge_account_update(self, event: Dict):
        """
        合并 ACCOUNT_UPDATE 事件到缓存
        格式: {e: "ACCOUNT_UPDATE", a: {m, B: [...], P: [...]}}
        """
        data = event.get('a', {})
        if not data:
            return

        # 合并余额 B
        for b in data.get('B', []):
            asset = b.get('a', '')
            wb = b.get('wb', '0')
            cw = b.get('cw', wb)
            # 更新 assets 中的对应资产
            assets = self._account_info.get('assets', [])
            found = False
            for a in assets:
                if a.get('asset') == asset:
                    a['walletBalance'] = wb
                    a['crossWalletBalance'] = cw
                    a['availableBalance'] = cw  # 简化
                    found = True
                    break
            if not found:
                assets.append({
                    'asset': asset,
                    'walletBalance': wb,
                    'crossWalletBalance': cw,
                    'availableBalance': cw,
                    'unrealizedProfit': '0',
                    'marginBalance': cw,
                })
            self._account_info['assets'] = assets

            # 更新顶层 totals（USDS-margined 单资产时）
            if asset == 'USDT':
                self._account_info['totalWalletBalance'] = wb
                self._account_info['availableBalance'] = cw

        # 合并持仓 P
        for p in data.get('P', []):
            symbol = format_symbol(p.get('s', ''))
            ps = p.get('ps', 'BOTH')
            pa = p.get('pa', '0')
            ep = p.get('ep', '0')
            up = p.get('up', '0')
            try:
                pa_f = float(pa)
            except (ValueError, TypeError):
                pa_f = 0.0

            key = (symbol, ps)
            if abs(pa_f) < 1e-8:
                self._positions_by_key.pop(key, None)
            else:
                pos = self._positions_by_key.get(key)
                if pos is None:
                    pos = {
                        'symbol': symbol,
                        'positionAmt': pa,
                        'entryPrice': ep,
                        'unrealizedProfit': up,
                        'positionSide': ps,
                        'marginType': p.get('mt', 'cross'),
                    }
                    self._positions_by_key[key] = pos
                else:
                    pos['positionAmt'] = pa
                    pos['entryPrice'] = ep
                    pos['unrealizedProfit'] = up

                # 补充 markPrice（从 mark_price_cache）
                if self.mark_price_cache:
                    mp = self.mark_price_cache.get_price(symbol)
                    if mp is not None:
                        pos['markPrice'] = str(mp)

        # 重建 _positions 列表并更新 totalUnrealizedProfit
        self._positions = list(self._positions_by_key.values())
        total_up = sum(float(p.get('unrealizedProfit', 0)) for p in self._positions)
        self._account_info['totalUnrealizedProfit'] = str(total_up)
        twb = float(self._account_info.get('totalWalletBalance', 0))
        self._account_info['totalMarginBalance'] = str(twb + total_up)

    async def start(self) -> bool:
        """启动：先 REST 快照，再建立 WebSocket"""
        if not await self._rest_snapshot():
            return False
        self.running = True
        logger.info(f"[{self.account_id}] Starting account stream WebSocket")
        self._ws_task = asyncio.create_task(self._websocket_handler())
        return True

    async def stop(self):
        """停止"""
        self.running = False
        self._connected = False
        self._listen_key = None
        if self._ws_task and not self._ws_task.done():
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
        self._ws_task = None
        logger.info(f"[{self.account_id}] Account stream stopped")

    async def _websocket_handler(self):
        """WebSocket 主循环"""
        reconnect_count = 0
        last_keepalive = time.time()

        while self.running:
            try:
                if not self._listen_key:
                    self._listen_key = await self.client.create_listen_key()
                ws_url = f"{self.ws_base}/ws/{self._listen_key}"

                ping_jitter = random.uniform(-2.0, 2.0)
                eff_ping = max(5.0, float(self.ping_interval) + ping_jitter)
                eff_timeout = max(float(self.ping_timeout), eff_ping + 5.0)

                logger.info(
                    f"[{self.account_id}] Account stream connecting "
                    f"(ping={eff_ping:.1f}/{eff_timeout:.1f}s)"
                )

                async with websockets.connect(
                    ws_url,
                    ping_interval=eff_ping,
                    ping_timeout=eff_timeout,
                    close_timeout=10,
                    open_timeout=self.open_timeout,
                ) as websocket:
                    self._connected = True
                    logger.info(f"[{self.account_id}] Account stream WebSocket connected")
                    reconnect_count = 0

                    while self.running:
                        try:
                            msg_str = await asyncio.wait_for(websocket.recv(), timeout=eff_timeout + 5)
                            data = json.loads(msg_str)
                            self._process_message(data)

                            # Keepalive listenKey
                            if time.time() - last_keepalive >= self.keepalive_interval:
                                await self.client.keepalive_listen_key()
                                last_keepalive = time.time()
                                logger.debug(f"[{self.account_id}] ListenKey keepalive ok")

                        except asyncio.TimeoutError:
                            continue
                        except ConnectionClosed as e:
                            log_network_error(f"[{self.account_id}] Account stream recv", e)
                            logger.info(f"[{self.account_id}] Account stream closed, reconnecting...")
                            break
                        except Exception as e:
                            log_network_error(f"[{self.account_id}] Account stream message", e)
                            break

                    self._connected = False
                    self._listen_key = None

                if self.running:
                    stagger = random.uniform(2.0, 5.0)
                    logger.info(f"[{self.account_id}] Account stream waiting {stagger:.1f}s before reconnect")
                    await asyncio.sleep(stagger)

            except WebSocketException as e:
                if not self.running:
                    break
                reconnect_count += 1
                self.stats['reconnect_count'] += 1
                self._listen_key = None
                delay = min(self.reconnect_delay * (1.5 ** min(reconnect_count - 1, 3)), 30)
                log_network_error(f"[{self.account_id}] Account stream connect", e)
                logger.info(f"[{self.account_id}] Account stream reconnecting in {delay:.1f}s (#{reconnect_count})")
                await asyncio.sleep(delay)

            except (ConnectionTimeoutError, asyncio.TimeoutError) as e:
                if not self.running:
                    break
                reconnect_count += 1
                self.stats['reconnect_count'] += 1
                self._listen_key = None
                delay = min(self.reconnect_delay * (1.5 ** min(reconnect_count - 1, 3)), 30)
                log_network_error(f"[{self.account_id}] Account stream timeout", e)
                logger.info(f"[{self.account_id}] Account stream timeout, reconnecting in {delay:.1f}s")
                await asyncio.sleep(delay)

            except Exception as e:
                if not self.running:
                    break
                import socket
                reconnect_count += 1
                self.stats['reconnect_count'] += 1
                self._listen_key = None
                if isinstance(e, (socket.gaierror, OSError)) and "getaddrinfo" in str(e):
                    delay = min(self.reconnect_delay * (1.2 ** min(reconnect_count - 1, 2)), 10)
                else:
                    delay = min(self.reconnect_delay * (1.5 ** min(reconnect_count - 1, 3)), 30)
                    log_network_error(f"[{self.account_id}] Account stream error", e)
                logger.info(f"[{self.account_id}] Account stream error, reconnecting in {delay:.1f}s")
                await asyncio.sleep(delay)

    def _process_message(self, msg: dict):
        """处理 WebSocket 消息"""
        if not isinstance(msg, dict):
            return
        event_type = msg.get('e', '')
        if event_type == 'ACCOUNT_UPDATE':
            self.stats['messages_received'] += 1
            self._merge_account_update(msg)
