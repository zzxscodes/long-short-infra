"""
监控用标记价格缓存
通过 WebSocket 订阅 !markPrice@arr 流，实时接收全市场标记价格，
替代 REST get_all_ticker_prices，减少 API 请求、避免限流。

与 funding_market_collector 采用相同的 WebSocket 架构。
"""
import json
import time
import asyncio
import random
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException
from typing import Dict, Optional

from ..common.config import config
from ..common.logger import get_logger
from ..common.utils import format_symbol
from ..common.network_utils import log_network_error, ConnectionTimeoutError

logger = get_logger('mark_price_cache')


class MarkPriceCache:
    """
    监控用标记价格缓存

    订阅 Binance Futures !markPrice@arr 流，维护 {symbol: mark_price} 缓存。
    用于 _convert_weights_to_quantities 中的价格转换，替代 REST get_all_ticker_prices。
    """

    def __init__(self):
        self.ws_base = config.get_binance_ws_base()
        self.reconnect_delay = 5
        self.ping_interval = int(config.get("monitoring.mark_price_ping_interval_seconds", 20))
        self.ping_timeout = int(config.get("monitoring.mark_price_ping_timeout_seconds", 30))
        self.open_timeout = int(config.get("monitoring.mark_price_open_timeout_seconds", 30))

        self._price_cache: Dict[str, float] = {}
        self._last_update_ts: float = 0.0
        self.running = False
        self._connected = False

        self.stats = {
            'messages_received': 0,
            'reconnect_count': 0,
            'start_time': time.time(),
        }

    def get_prices(self) -> Dict[str, float]:
        """
        获取当前所有 symbol 的标记价格快照

        Returns:
            Dict[symbol, price]，缓存为空时返回空字典
        """
        return dict(self._price_cache)

    def get_price(self, symbol: str) -> Optional[float]:
        """获取单个 symbol 的标记价格"""
        return self._price_cache.get(format_symbol(symbol))

    def is_ready(self) -> bool:
        """缓存是否已有数据（连接成功且至少收到过一条推送）"""
        return self._connected and len(self._price_cache) > 0

    def last_update_time(self) -> float:
        """最后一次更新的时间戳"""
        return self._last_update_ts

    async def start(self):
        """启动 WebSocket 连接"""
        self.running = True
        logger.info("Starting mark price cache via WebSocket !markPrice@arr")
        await self._websocket_handler()

    async def stop(self):
        """停止 WebSocket 连接"""
        self.running = False
        self._connected = False
        logger.info("Mark price cache stopped")

    async def _websocket_handler(self):
        """
        订阅 !markPrice@arr 组合流
        格式: wss://fstream.binance.com/stream?streams=!markPrice@arr
        """
        update_speed = config.get("monitoring.mark_price_update_speed", "3s")
        speed_suffix = "@1s" if update_speed == "1s" else ""
        stream_name = f"!markPrice@arr{speed_suffix}"
        ws_url = f"{self.ws_base}/stream?streams={stream_name}"
        reconnect_count = 0

        while self.running:
            try:
                ping_jitter = random.uniform(-2.0, 2.0)
                eff_ping = max(5.0, float(self.ping_interval) + ping_jitter)
                eff_timeout = max(float(self.ping_timeout), eff_ping + 5.0)

                logger.info(
                    f"Mark price cache connecting to {stream_name} "
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
                    logger.info("Mark price cache WebSocket connected")
                    reconnect_count = 0

                    while self.running:
                        try:
                            msg_str = await websocket.recv()
                            data = json.loads(msg_str)
                            self._process_message(data)
                        except ConnectionClosed as e:
                            log_network_error("Mark price cache WebSocket recv", e)
                            logger.info("Mark price cache WebSocket closed, reconnecting...")
                            break
                        except Exception as e:
                            log_network_error("Mark price cache message processing", e)
                            break

                    self._connected = False

                if self.running:
                    stagger = random.uniform(2.0, 5.0)
                    logger.info(f"Mark price cache waiting {stagger:.1f}s before reconnect")
                    await asyncio.sleep(stagger)

            except WebSocketException as e:
                if not self.running:
                    break
                reconnect_count += 1
                self.stats['reconnect_count'] += 1
                delay = min(self.reconnect_delay * (1.5 ** min(reconnect_count - 1, 3)), 30)
                log_network_error("Mark price cache WebSocket connect", e)
                logger.info(f"Mark price cache reconnecting in {delay:.1f}s (#{reconnect_count})")
                await asyncio.sleep(delay)

            except (ConnectionTimeoutError, asyncio.TimeoutError) as e:
                if not self.running:
                    break
                reconnect_count += 1
                self.stats['reconnect_count'] += 1
                delay = min(self.reconnect_delay * (1.5 ** min(reconnect_count - 1, 3)), 30)
                log_network_error("Mark price cache WebSocket timeout", e)
                logger.info(f"Mark price cache timeout, reconnecting in {delay:.1f}s")
                await asyncio.sleep(delay)

            except Exception as e:
                if not self.running:
                    break
                import socket
                reconnect_count += 1
                self.stats['reconnect_count'] += 1
                if isinstance(e, (socket.gaierror, OSError)) and "getaddrinfo" in str(e):
                    delay = min(self.reconnect_delay * (1.2 ** min(reconnect_count - 1, 2)), 10)
                    logger.info(f"Mark price cache DNS error, reconnecting in {delay:.1f}s")
                else:
                    delay = min(self.reconnect_delay * (1.5 ** min(reconnect_count - 1, 3)), 30)
                    log_network_error("Mark price cache WebSocket error", e)
                    logger.info(f"Mark price cache error, reconnecting in {delay:.1f}s")
                await asyncio.sleep(delay)

    def _process_message(self, msg: dict):
        """
        处理组合流消息
        格式: {"stream": "!markPrice@arr", "data": [{e, s, p, ...}, ...]}
        """
        if not isinstance(msg, dict):
            return

        data = msg.get('data')
        if not isinstance(data, list):
            return
        stream_name = msg.get('stream', '')
        if 'markPrice' not in stream_name:
            return

        for item in data:
            if isinstance(item, dict):
                symbol = item.get('s')
                price_str = item.get('p')
                if symbol and price_str:
                    try:
                        price = float(price_str)
                        if price > 0:
                            self._price_cache[format_symbol(symbol)] = price
                    except (ValueError, TypeError):
                        pass
        self.stats['messages_received'] += 1
        self._last_update_ts = time.time()
