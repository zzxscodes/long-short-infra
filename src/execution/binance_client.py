"""
Binance API客户端
用于订单执行和账户查询
"""
import time
import hmac
import hashlib
import aiohttp
from typing import Dict, List, Optional, Any
from urllib.parse import urlencode

from ..common.config import config
from ..common.logger import get_logger
from ..common.utils import round_price, round_qty, format_symbol
from ..common.network_utils import safe_http_request, log_network_error, IPBannedError

logger = get_logger('binance_client')


class BinanceClient:
    """Binance API客户端"""
    
    def __init__(self, api_key: str, api_secret: str, api_base: Optional[str] = None, dry_run: bool = False):
        """
        初始化Binance客户端
        
        Args:
            api_key: API Key
            api_secret: API Secret
            api_base: API base url（可选，用于testnet或自定义endpoint）
            dry_run: 是否使用dry-run模式（使用test_order endpoint而不是place_order）
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_base = api_base or config.get('binance.api_base', 'https://fapi.binance.com')
        self.dry_run = dry_run
        self.session: Optional[aiohttp.ClientSession] = None

        # 服务器对时（生产级）：使用 monotonic 作为时间基准，避免本机 wall clock 跳变导致 -1021。
        # 只在必要时（首次/过期/出现 -1021）才请求 /fapi/v1/time，同步后以 monotonic 推进估算 server time。
        self._server_time_base_ms: Optional[int] = None
        self._server_time_base_mono_s: Optional[float] = None
        self._server_time_sync_wall_s: Optional[float] = None
        self._server_time_sync_ttl_s: float = float(
            config.get("execution.binance_client.server_time_sync_ttl_seconds", 6 * 3600)
        )
        self._recv_window_ms: int = int(
            config.get("execution.binance_client.recv_window_ms", 60000)
        )
        
        # 从配置读取合约设置（灵活适配配置）
        contract_settings = config.get('execution.contract_settings', {})
        
        # 目标保证金模式（CROSSED 或 ISOLATED）
        margin_type = contract_settings.get('margin_type', 'CROSSED')
        if isinstance(margin_type, str):
            margin_type = margin_type.upper()
        if margin_type not in ['CROSSED', 'ISOLATED']:
            logger.warning(f"Invalid margin_type in config: {margin_type}, using default CROSSED")
            margin_type = 'CROSSED'
        self._target_margin_type = margin_type
        
        # 目标杠杆倍数（1-125）
        leverage = contract_settings.get('leverage', 20)
        try:
            leverage = int(leverage)
            if leverage < 1 or leverage > 125:
                logger.warning(f"Invalid leverage in config: {leverage}, using default 20")
                leverage = 20
        except (ValueError, TypeError):
            logger.warning(f"Invalid leverage type in config: {leverage}, using default 20")
            leverage = 20
        self._target_leverage = leverage
        
        # 合约配置缓存：记录每个symbol的margin_type和leverage
        # 格式: {symbol: {'margin_type': 'CROSSED', 'leverage': 20, 'verified': True, 'timestamp': time.time()}}
        self._contract_settings_cache: Dict[str, Dict[str, Any]] = {}
        self._contract_settings_cache_max_size = config.get('execution.binance_client.contract_settings_cache_max_size', 500)
        self._contract_settings_cache_ttl = config.get('execution.binance_client.contract_settings_cache_ttl', 3600)  # 1小时过期
        
        if self.dry_run:
            logger.info("BinanceClient initialized in DRY-RUN mode (using test_order endpoint)")
        logger.debug(f"BinanceClient target contract settings: margin_type={self._target_margin_type}, leverage={self._target_leverage}x")
    
    def _cleanup_contract_settings_cache(self):
        """清理过期的合约设置缓存"""
        import time
        current_time = time.time()
        
        # 删除过期的缓存
        expired_symbols = [
            sym for sym, cache in self._contract_settings_cache.items()
            if current_time - cache.get('timestamp', 0) > self._contract_settings_cache_ttl
        ]
        for sym in expired_symbols:
            self._contract_settings_cache.pop(sym, None)
        
        # 如果缓存大小超过限制，删除最旧的直到满足限制
        if len(self._contract_settings_cache) >= self._contract_settings_cache_max_size:
            sorted_items = sorted(
                self._contract_settings_cache.items(),
                key=lambda x: x[1].get('timestamp', 0)
            )
            # 删除最旧的，直到满足限制
            to_remove = len(sorted_items) - self._contract_settings_cache_max_size + 1  # +1确保删除后小于限制
            for sym, _ in sorted_items[:to_remove]:
                self._contract_settings_cache.pop(sym, None)
    
    async def _sync_server_time(self, *, force: bool = False) -> bool:
        """同步Binance服务器时间（生产级，考虑网络延迟与 wall clock 跳变）。

        - 使用 `time.monotonic()` 计算 RTT（不受系统时间调整影响）
        - 同步成功后，以 (server_time_at_midpoint, monotonic_midpoint) 为基准推进估算 server time
        - 仅在首次/过期/force 或 -1021 自愈时触发，不增加常态请求频率
        """
        try:
            if (
                not force
                and self._server_time_sync_wall_s is not None
                and (time.time() - self._server_time_sync_wall_s) < self._server_time_sync_ttl_s
                and self._server_time_base_ms is not None
                and self._server_time_base_mono_s is not None
            ):
                return True

            url = f"{self.api_base}/fapi/v1/time"
            session = await self._get_session()
            
            # 多次测量取平均值，减少网络延迟影响
            samples = []  # (server_mid_ms, mono_mid_s, rtt_ms)
            for _ in range(3):
                try:
                    t0_mono = time.monotonic()
                    # 使用safe_http_request，自动处理限流和错误
                    data = await safe_http_request(
                        session,
                        'GET',
                        url,
                        max_retries=2,
                        timeout=10.0,
                        return_json=True,
                        use_rate_limit=True  # 启用请求限流
                    )
                    t1_mono = time.monotonic()
                    
                    if data:
                        server_time = data.get('serverTime', 0)
                        # RTT（毫秒，基于 monotonic）
                        rtt_ms = (t1_mono - t0_mono) * 1000.0
                        mono_mid_s = (t0_mono + t1_mono) / 2.0

                        # 估计 server time 在“请求往返中点”的时间
                        # serverTime 近似对应服务端产生响应时刻（靠近 t1_mono），回推半个 RTT 得到中点估计。
                        server_mid_ms = int(float(server_time) - (rtt_ms / 2.0))
                        samples.append((server_mid_ms, mono_mid_s, rtt_ms))
                except Exception as e:
                    logger.debug(f"Time sync attempt failed: {e}")
                    continue
            
            if not samples:
                logger.warning("All time sync attempts failed")
                return False

            # 使用 server_mid_ms 的中位数样本，更抗异常 RTT
            samples.sort(key=lambda x: x[0])
            server_mid_ms, mono_mid_s, rtt_ms = samples[len(samples) // 2]

            self._server_time_base_ms = int(server_mid_ms)
            self._server_time_base_mono_s = float(mono_mid_s)
            self._server_time_sync_wall_s = time.time()

            logger.debug(
                f"Server time synced (monotonic-based), "
                f"base_ms={self._server_time_base_ms}, rtt_ms~{rtt_ms:.1f}, samples={len(samples)}"
            )
            return True
        except Exception as e:
            logger.warning(f"Failed to sync server time: {e}")
            return False
    
    def _is_server_time_sync_fresh(self) -> bool:
        """对时是否在 TTL 内（用于决定是否主动刷新；即使过期也可继续用 monotonic 推进估算）。"""
        if self._server_time_sync_wall_s is None:
            return False
        return (time.time() - self._server_time_sync_wall_s) < self._server_time_sync_ttl_s
    
    def _get_server_timestamp_ms(self) -> int:
        """获取“估算的 server time”（毫秒）。优先 monotonic 基准，避免 wall clock 跳变。"""
        if self._server_time_base_ms is not None and self._server_time_base_mono_s is not None:
            elapsed_ms = (time.monotonic() - self._server_time_base_mono_s) * 1000.0
            return int(self._server_time_base_ms + elapsed_ms)
        # 兜底：未成功对时，只能用本机 wall clock
        return int(time.time() * 1000)
    
    async def _get_session(self) -> aiohttp.ClientSession:
        """获取或创建HTTP会话"""
        if self.session is None or self.session.closed:
            # 如果旧session存在且未关闭，先关闭它
            if self.session and not self.session.closed:
                try:
                    await self.session.close()
                except Exception as e:
                    logger.debug(f"Error closing old session: {e}")
            self.session = aiohttp.ClientSession()
        return self.session
    
    def _generate_signature(self, params: Dict) -> str:
        """生成API签名"""
        query_string = urlencode(params)
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        return signature
    
    def _get_headers(self) -> Dict[str, str]:
        """获取请求头"""
        return {
            'X-MBX-APIKEY': self.api_key
        }
    
    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        signed: bool = False
    ) -> Dict:
        """
        发送HTTP请求
        
        Args:
            method: HTTP方法（GET, POST, DELETE等）
            endpoint: API端点
            params: 请求参数
            signed: 是否需要签名
        
        Returns:
            响应数据
        """
        if params is None:
            params = {}
        
        # 添加时间戳和recvWindow（Binance API要求）
        if signed:
            # 首次请求或 TTL 过期时尝试对时；失败则继续使用已有 monotonic 基准（若有）。
            if self._server_time_base_ms is None or self._server_time_base_mono_s is None or (not self._is_server_time_sync_fresh()):
                sync_success = await self._sync_server_time()
                if not sync_success and (self._server_time_base_ms is None or self._server_time_base_mono_s is None):
                    logger.warning("Time sync failed and no previous baseline, using local time (may cause -1021)")

            # 使用“估算的 server timestamp”
            params["timestamp"] = self._get_server_timestamp_ms()
            params["recvWindow"] = int(self._recv_window_ms)
            # 注意：签名必须在添加所有参数后生成
            params['signature'] = self._generate_signature(params)
        
        url = f"{self.api_base}{endpoint}"
        
        session = await self._get_session()
        
        try:
            # 准备请求参数
            request_kwargs = {
                'headers': self._get_headers(),
            }
            
            if method == 'GET':
                request_kwargs['params'] = params
            else:
                request_kwargs['data'] = params
            
            # 使用安全的HTTP请求（带重试和限流）
            try:
                data = await safe_http_request(
                    session,
                    method,
                    url,
                    max_retries=3,
                    timeout=30.0,
                    return_json=True,
                    use_rate_limit=True,  # 启用请求限流
                    **request_kwargs
                )
            except IPBannedError as e:
                # IP被封禁，直接抛出（safe_http_request已经处理了等待逻辑）
                logger.error(f"IP被封禁，无法继续请求: {e}")
                raise
            except Exception as e:
                # 捕获NetworkError，检查是否是时间戳错误
                from ..common.network_utils import NetworkError
                if isinstance(e, NetworkError) and signed:
                    error_str = str(e)
                    if '-1021' in error_str or 'Timestamp' in error_str or 'recvWindow' in error_str:
                        # 时间戳错误，重新同步并重试
                        logger.warning(f"Timestamp error detected, re-syncing server time and retrying: {error_str}")
                        # 重新同步服务器时间（强制同步）
                        sync_success = await self._sync_server_time(force=True)
                        if not sync_success:
                            logger.error("Time re-sync failed, retry may still fail")
                            # 如果没有任何对时基线，重试没有意义（会再次 -1021），直接抛出
                            if self._server_time_base_ms is None or self._server_time_base_mono_s is None:
                                raise
                        
                        # 重新生成时间戳和签名
                        params["timestamp"] = self._get_server_timestamp_ms()
                        # 重新生成签名（需要先移除旧的signature）
                        params.pop('signature', None)
                        params['signature'] = self._generate_signature(params)
                        
                        # 更新请求参数
                        if method == 'GET':
                            request_kwargs['params'] = params
                        else:
                            request_kwargs['data'] = params
                        
                        # 重试请求
                        try:
                            data = await safe_http_request(
                                session,
                                method,
                                url,
                                max_retries=1,  # 只重试一次
                                timeout=30.0,
                                return_json=True,
                                use_rate_limit=True,
                                **request_kwargs
                            )
                            # 重试成功，返回数据
                            return data
                        except Exception as retry_e:
                            # 重试也失败，检查是否仍然是时间戳错误
                            retry_error_str = str(retry_e)
                            if '-1021' in retry_error_str or 'Timestamp' in retry_error_str or 'recvWindow' in retry_error_str:
                                logger.error(f"Timestamp error persists after re-sync: {retry_error_str}")
                            # 抛出重试错误
                            raise retry_e
                    else:
                        # 其他NetworkError，直接抛出
                        raise
                else:
                    # 其他异常，直接抛出
                    raise
            
            # 检查Binance错误码
            if 'code' in data and data['code'] != 200:
                error_code = data['code']
                error_msg = data.get('msg', 'Unknown error')
                
                # 如果是时间戳错误（-1021），重新同步时间并重试一次
                if error_code == -1021 and signed:
                    logger.warning(f"Timestamp error detected, re-syncing server time and retrying: {error_msg}")
                    # 重新同步服务器时间（强制同步）
                    sync_success = await self._sync_server_time(force=True)
                    if not sync_success:
                        logger.error("Time re-sync failed, retry may still fail")
                        if self._server_time_base_ms is None or self._server_time_base_mono_s is None:
                            raise
                    
                    # 重新生成时间戳和签名
                    params["timestamp"] = self._get_server_timestamp_ms()
                    # 重新生成签名（需要先移除旧的signature）
                    params.pop('signature', None)
                    params['signature'] = self._generate_signature(params)
                    
                    # 更新请求参数
                    if method == 'GET':
                        request_kwargs['params'] = params
                    else:
                        request_kwargs['data'] = params
                    
                    # 重试请求
                    try:
                        data = await safe_http_request(
                            session,
                            method,
                            url,
                            max_retries=1,  # 只重试一次
                            timeout=30.0,
                            return_json=True,
                            use_rate_limit=True,
                            **request_kwargs
                        )
                        
                        # 检查重试后的结果
                        if 'code' in data and data['code'] != 200:
                            error_code = data['code']
                            error_msg = data.get('msg', 'Unknown error')
                            # 如果仍然是时间戳错误，记录详细日志
                            if error_code == -1021:
                                logger.error(f"Timestamp error persists after re-sync: {error_msg}")
                        else:
                            # 重试成功，返回数据
                            return data
                    except Exception as retry_e:
                        # 重试请求本身失败，检查是否是时间戳错误
                        retry_error_str = str(retry_e)
                        if '-1021' in retry_error_str or 'Timestamp' in retry_error_str or 'recvWindow' in retry_error_str:
                            logger.error(f"Timestamp error persists after re-sync: {retry_error_str}")
                        # 继续处理，让后续的错误处理逻辑处理
                        raise
                
                # 检查是否是"无需更改"的错误（这些应该被视为成功）
                if error_code == -4059 or 'No need to change position side' in error_msg:
                    logger.debug("Position mode already set to target, treating as success")
                    return {'code': 200, 'msg': 'Already set to target position mode'}
                if error_code == -4046 or 'No need to change margin type' in error_msg:
                    logger.debug("Margin type already set to target, treating as success")
                    return {'code': 200, 'msg': 'Already set to target margin type'}
                
                # 其他错误记录并抛出
                log_network_error(
                    f"Binance API调用",
                    Exception(f"API错误码: {error_code}"),
                    context={
                        "method": method,
                        "endpoint": endpoint,
                        "code": error_code,
                        "msg": error_msg
                    }
                )
                raise Exception(f"Binance API error: code={error_code}, msg={error_msg}")
            
            return data
                
        except Exception as e:
            # 检查NetworkError中的错误码
            error_str = str(e)
            if '-4059' in error_str or 'No need to change position side' in error_str:
                logger.debug("Position mode already set to target, treating as success")
                return {'code': 200, 'msg': 'Already set to target position mode'}
            if '-4046' in error_str or 'No need to change margin type' in error_str:
                logger.debug("Margin type already set to target, treating as success")
                return {'code': 200, 'msg': 'Already set to target margin type'}
            
            log_network_error(
                f"Binance API请求",
                e,
                context={
                    "method": method,
                    "endpoint": endpoint,
                    "url": url
                }
            )
            raise
    
    async def get_account_info(self) -> Dict:
        """获取账户信息"""
        try:
            data = await self._request('GET', '/fapi/v2/account', signed=True)
            return data
        except Exception as e:
            logger.error(f"Failed to get account info: {e}", exc_info=True)
            raise
    
    async def get_positions(self) -> List[Dict]:
        """获取当前持仓"""
        try:
            data = await self._request('GET', '/fapi/v2/positionRisk', signed=True)
            # 只返回持仓量不为0的
            positions = [pos for pos in data if float(pos.get('positionAmt', 0)) != 0]
            return positions
        except Exception as e:
            logger.error(f"Failed to get positions: {e}", exc_info=True)
            raise
    
    async def get_position_risk(self, symbol: Optional[str] = None) -> List[Dict]:
        """
        获取持仓风险信息（包括所有交易对，即使持仓为0）
        可用于验证合约设置
        
        Args:
            symbol: 交易对（可选，如果指定则只返回该交易对的信息）
        
        Returns:
            持仓风险信息列表
        """
        try:
            params = {}
            if symbol:
                symbol = format_symbol(symbol)
                params['symbol'] = symbol
            
            data = await self._request('GET', '/fapi/v2/positionRisk', params=params, signed=True)
            return data
        except Exception as e:
            logger.error(f"Failed to get position risk: {e}", exc_info=True)
            raise
    
    async def verify_contract_settings(self, symbol: str, expected_margin_type: str, expected_leverage: int) -> Dict[str, bool]:
        """
        验证交易对的合约设置是否符合要求
        
        Args:
            symbol: 交易对
            expected_margin_type: 期望的保证金模式（ISOLATED或CROSSED）
            expected_leverage: 期望的杠杆倍数
        
        Returns:
            验证结果字典，包含 'margin_type_ok' 和 'leverage_ok'
        """
        try:
            symbol = format_symbol(symbol)
            position_risk = await self.get_position_risk(symbol)
            
            if not position_risk:
                return {'margin_type_ok': False, 'leverage_ok': False, 'error': 'No position risk data found'}
            
            # 找到对应的交易对
            pos_info = None
            for pos in position_risk:
                if format_symbol(pos.get('symbol', '')) == symbol:
                    pos_info = pos
                    break
            
            if not pos_info:
                return {'margin_type_ok': False, 'leverage_ok': False, 'error': 'Symbol not found in position risk'}
            
            # 验证保证金模式
            current_margin_type = pos_info.get('marginType', '').upper()
            expected_margin_type_upper = expected_margin_type.upper()
            # Binance API返回的marginType可能是"CROSS"而不是"CROSSED"，需要兼容处理
            # CROSSED和CROSS都表示全仓模式
            margin_type_ok = False  # 默认值
            if expected_margin_type_upper == 'CROSSED':
                margin_type_ok = (current_margin_type == 'CROSSED' or current_margin_type == 'CROSS')
            elif expected_margin_type_upper == 'ISOLATED':
                margin_type_ok = (current_margin_type == 'ISOLATED' or current_margin_type == 'ISOLATE')
            else:
                margin_type_ok = (current_margin_type == expected_margin_type_upper)
            
            # 验证杠杆倍数
            current_leverage = int(pos_info.get('leverage', 0))
            leverage_ok = (current_leverage == expected_leverage)
            
            return {
                'margin_type_ok': margin_type_ok,
                'leverage_ok': leverage_ok,
                'current_margin_type': current_margin_type,
                'current_leverage': current_leverage
            }
        except Exception as e:
            logger.warning(f"Failed to verify contract settings for {symbol}: {e}")
            return {'margin_type_ok': False, 'leverage_ok': False, 'error': str(e)}
    
    async def get_exchange_info(self) -> Dict:
        """获取交易所信息（获取symbol的精度等信息）"""
        try:
            data = await self._request('GET', '/fapi/v1/exchangeInfo', signed=False)
            return data
        except Exception as e:
            logger.error(f"Failed to get exchange info: {e}", exc_info=True)
            raise
    
    async def get_all_ticker_prices(self) -> Dict[str, float]:
        """
        一次请求获取所有交易对价格（避免多次 get_symbol_price 触发限流）
        
        Returns:
            Dict[symbol, price]，失败返回空字典
        """
        try:
            data = await self._request('GET', '/fapi/v1/ticker/price', params={}, signed=False)
            if isinstance(data, list):
                return {
                    format_symbol(item.get('symbol', '')): float(item.get('price', 0))
                    for item in data
                    if item.get('symbol') and float(item.get('price', 0)) > 0
                }
            return {}
        except Exception as e:
            logger.debug(f"Failed to get all ticker prices: {e}")
            return {}

    async def get_symbol_price(self, symbol: str) -> Optional[float]:
        """
        获取交易对的当前标记价格（用于计算订单金额）
        
        Args:
            symbol: 交易对
        
        Returns:
            当前价格，如果获取失败返回None
        """
        try:
            symbol = format_symbol(symbol)
            params = {'symbol': symbol}
            data = await self._request('GET', '/fapi/v1/ticker/price', params=params, signed=False)
            price = float(data.get('price', 0))
            if price > 0:
                return price
            return None
        except Exception as e:
            logger.debug(f"Failed to get price for {symbol}: {e}")
            return None
    
    async def get_orderbook(self, symbol: str, limit: int = 5) -> Optional[Dict]:
        """
        获取订单簿（深度）
        
        Args:
            symbol: 交易对
            limit: 深度限制（5, 10, 20, 50, 100, 500, 1000）
        
        Returns:
            订单簿数据，包含 bids 和 asks，如果获取失败返回 None
            {
                'bids': [[price, quantity], ...],  # 买单，价格从高到低
                'asks': [[price, quantity], ...],  # 卖单，价格从低到高
            }
        """
        try:
            symbol = format_symbol(symbol)
            params = {'symbol': symbol, 'limit': limit}
            data = await self._request('GET', '/fapi/v1/depth', params=params, signed=False)
            
            if data:
                return {
                    'bids': [[float(b[0]), float(b[1])] for b in data.get('bids', [])],
                    'asks': [[float(a[0]), float(a[1])] for a in data.get('asks', [])],
                }
            return None
        except Exception as e:
            logger.debug(f"Failed to get orderbook for {symbol}: {e}")
            return None
    
    async def get_current_funding_rate(self, symbol: str) -> Optional[float]:
        """
        获取交易对的当前资金费率（最后一次资金费率）
        
        Args:
            symbol: 交易对
        
        Returns:
            当前资金费率，如果获取失败则返回None
        """
        try:
            symbol = format_symbol(symbol)
            # 使用 /fapi/v1/fundingRate 获取最新的资金费率记录
            params = {'symbol': symbol, 'limit': 1}
            data = await self._request('GET', '/fapi/v1/fundingRate', params=params, signed=False)
            if data and isinstance(data, list) and len(data) > 0:
                funding_rate = data[0].get('fundingRate')
                if funding_rate is not None:
                    return float(funding_rate)
            return None
        except Exception as e:
            logger.debug(f"Failed to get current funding rate for {symbol}: {e}")
            return None
    
    def _get_max_leverage_from_exchange_info(self, exchange_info: Dict, symbol: str) -> Optional[int]:
        """
        从交易所信息中获取指定交易对支持的最大杠杆
        
        Args:
            exchange_info: 交易所信息（从get_exchange_info获取）
            symbol: 交易对（如'BTCUSDT'）
        
        Returns:
            最大杠杆倍数，如果未找到则返回None
        """
        try:
            symbol = format_symbol(symbol)
            symbols = exchange_info.get('symbols', [])
            for s in symbols:
                if s.get('symbol') == symbol:
                    # Binance的exchangeInfo中，leverageBrackets字段包含杠杆信息
                    # 但更简单的方式是直接尝试设置，如果失败则降低杠杆
                    # 这里先返回一个合理的默认值，实际会在设置时处理
                    filters = s.get('filters', [])
                    for f in filters:
                        if f.get('filterType') == 'LEVERAGE':
                            # 如果有LEVERAGE filter，返回最大杠杆
                            max_leverage = f.get('maxLeverage')
                            if max_leverage:
                                return int(max_leverage)
                    # 如果没有找到，返回None（表示使用默认值）
                    return None
            return None
        except Exception as e:
            logger.warning(f"Failed to get max leverage for {symbol} from exchange info: {e}")
            return None
    
    async def _ensure_contract_settings(
        self, 
        symbol: str, 
        margin_type: Optional[str] = None, 
        leverage: Optional[int] = None
    ) -> bool:
        """
        确保合约配置正确（对策略透明）
        
        在订单执行前自动检查并设置保证金模式和杠杆倍数
        如果已设置且正确，则跳过；否则自动设置
        
        Args:
            symbol: 交易对
            margin_type: 保证金模式（可选，如果不提供则使用默认值）
            leverage: 杠杆倍数（可选，如果不提供则使用默认值）
        
        Returns:
            True表示配置已确保，False表示设置失败
        """
        try:
            symbol = format_symbol(symbol)
            
            # 使用提供的参数或从配置读取（灵活适配配置）
            if margin_type is not None:
                target_margin_type = margin_type.upper() if isinstance(margin_type, str) else margin_type
            else:
                # 从配置读取
                contract_settings = config.get('execution.contract_settings', {})
                target_margin_type = contract_settings.get('margin_type', 'CROSSED')
                if isinstance(target_margin_type, str):
                    target_margin_type = target_margin_type.upper()
                if target_margin_type not in ['CROSSED', 'ISOLATED']:
                    target_margin_type = 'CROSSED'
            
            if leverage is not None:
                try:
                    target_leverage = int(leverage)
                except (ValueError, TypeError):
                    target_leverage = 20
            else:
                # 从配置读取
                contract_settings = config.get('execution.contract_settings', {})
                target_leverage = contract_settings.get('leverage', 20)
                try:
                    target_leverage = int(target_leverage)
                    if target_leverage < 1 or target_leverage > 125:
                        target_leverage = 20
                except (ValueError, TypeError):
                    target_leverage = 20
            
            # 清理过期缓存
            self._cleanup_contract_settings_cache()
            
            # 检查缓存，如果已验证过且正确，则跳过
            cached = self._contract_settings_cache.get(symbol)
            if cached and cached.get('verified', False):
                # 检查缓存是否过期
                import time
                cache_time = cached.get('timestamp', 0)
                if time.time() - cache_time > self._contract_settings_cache_ttl:
                    # 缓存过期，删除
                    self._contract_settings_cache.pop(symbol, None)
                    cached = None
                else:
                    # 检查缓存中的配置是否与目标配置一致
                    cached_margin_type = cached.get('margin_type')
                    cached_leverage = cached.get('leverage')
                    
                    if cached_margin_type == target_margin_type and cached_leverage == target_leverage:
                        # 验证当前设置是否仍然正确
                        verification = await self.verify_contract_settings(
                            symbol, target_margin_type, target_leverage
                        )
                        if verification.get('margin_type_ok') and verification.get('leverage_ok'):
                            # 配置仍然正确，无需重新设置
                            return True
                        else:
                            # 配置已改变，需要重新设置
                            logger.debug(f"{symbol}: Contract settings changed, re-applying...")
                            cached['verified'] = False
                    else:
                        # 目标配置已改变，需要重新设置
                        logger.debug(f"{symbol}: Target contract settings changed, re-applying...")
                        cached['verified'] = False
            
            # 如果缓存中没有或未验证，则设置合约配置
            if not cached or not cached.get('verified', False):
                try:
                    # 设置保证金模式
                    await self.change_margin_type(symbol, target_margin_type)
                    
                    # 设置杠杆倍数
                    await self.change_leverage(symbol, target_leverage)
                    
                    # 验证设置是否成功
                    verification = await self.verify_contract_settings(
                        symbol, target_margin_type, target_leverage
                    )
                    
                    # 获取当前设置
                    current_margin = verification.get('current_margin_type', '').upper()
                    current_leverage = verification.get('current_leverage', 0)
                    expected_margin_upper = target_margin_type.upper()
                    
                    # 检查保证金模式是否匹配（考虑 CROSS 和 CROSSED 的兼容性）
                    is_margin_ok = False
                    if expected_margin_upper == 'CROSSED':
                        is_margin_ok = (current_margin == 'CROSSED' or current_margin == 'CROSS')
                    elif expected_margin_upper == 'ISOLATED':
                        is_margin_ok = (current_margin == 'ISOLATED' or current_margin == 'ISOLATE')
                    else:
                        is_margin_ok = (current_margin == expected_margin_upper)
                    
                    # 检查杠杆倍数
                    leverage_ok = (current_leverage == target_leverage)
                    
                    if is_margin_ok and leverage_ok:
                        # 设置成功，更新缓存
                        import time
                        # 检查缓存大小，如果超过限制则清理最旧的
                        if len(self._contract_settings_cache) >= self._contract_settings_cache_max_size:
                            self._cleanup_contract_settings_cache()
                        
                        self._contract_settings_cache[symbol] = {
                            'margin_type': target_margin_type,
                            'leverage': target_leverage,
                            'verified': True,
                            'timestamp': time.time(),
                        }
                        # 如果 API 返回的是 CROSS 而不是 CROSSED，记录为 debug 级别（不是警告）
                        if expected_margin_upper == 'CROSSED' and current_margin == 'CROSS':
                            logger.debug(
                                f"{symbol}: Contract settings ensured automatically "
                                f"(margin_type={target_margin_type} [API returns CROSS], leverage={target_leverage}x)"
                            )
                        else:
                            logger.debug(
                                f"{symbol}: Contract settings ensured automatically "
                                f"(margin_type={target_margin_type}, leverage={target_leverage}x)"
                            )
                        return True
                    else:
                        # 只有真正不匹配时才记录警告
                        logger.warning(
                            f"{symbol}: Failed to verify contract settings after applying. "
                            f"Expected: margin_type={target_margin_type}, leverage={target_leverage}x. "
                            f"Current: margin_type={current_margin}, leverage={current_leverage}x"
                        )
                        return False
                        
                except Exception as e:
                    error_msg = str(e)
                    # 如果返回"已经设置"，视为成功
                    if '-4046' in error_msg or 'No need to change margin type' in error_msg:
                        # 保证金模式已设置，继续设置杠杆
                        try:
                            await self.change_leverage(symbol, target_leverage)
                            # 验证设置
                            verification = await self.verify_contract_settings(
                                symbol, target_margin_type, target_leverage
                            )
                            
                            # 获取当前设置并检查兼容性
                            current_margin = verification.get('current_margin_type', '').upper()
                            current_leverage = verification.get('current_leverage', 0)
                            expected_margin_upper = target_margin_type.upper()
                            
                            # 检查保证金模式是否匹配（考虑 CROSS 和 CROSSED 的兼容性）
                            is_margin_ok = False
                            if expected_margin_upper == 'CROSSED':
                                is_margin_ok = (current_margin == 'CROSSED' or current_margin == 'CROSS')
                            elif expected_margin_upper == 'ISOLATED':
                                is_margin_ok = (current_margin == 'ISOLATED' or current_margin == 'ISOLATE')
                            else:
                                is_margin_ok = (current_margin == expected_margin_upper)
                            
                            leverage_ok = (current_leverage == target_leverage)
                            
                            if is_margin_ok and leverage_ok:
                                import time
                                # 检查缓存大小，如果超过限制则清理最旧的
                                if len(self._contract_settings_cache) >= self._contract_settings_cache_max_size:
                                    self._cleanup_contract_settings_cache()
                                
                                self._contract_settings_cache[symbol] = {
                                    'margin_type': target_margin_type,
                                    'leverage': target_leverage,
                                    'verified': True,
                                    'timestamp': time.time(),
                                }
                                return True
                        except Exception as lev_e:
                            logger.warning(f"{symbol}: Failed to set leverage: {lev_e}")
                            return False
                    else:
                        logger.warning(f"{symbol}: Failed to ensure contract settings: {e}")
                        return False
            
            return False
            
        except Exception as e:
            logger.warning(f"{symbol}: Error ensuring contract settings: {e}")
            return False
    
    async def place_order(
        self,
        symbol: str,
        side: str,  # BUY or SELL
        order_type: str,  # MARKET, LIMIT等
        quantity: Optional[float] = None,
        price: Optional[float] = None,
        position_side: str = 'BOTH',  # LONG, SHORT, BOTH
        reduce_only: bool = False,
        margin_type: Optional[str] = None,  # 可选的保证金模式（如果提供则在下单前设置）
        leverage: Optional[int] = None  # 可选的杠杆倍数（如果提供则在下单前设置）
    ) -> Dict:
        """
        下单
        
        Args:
            symbol: 交易对
            side: 方向（BUY/SELL）
            order_type: 订单类型（MARKET/LIMIT）
            quantity: 数量
            price: 价格（限价单需要）
            position_side: 持仓方向（LONG/SHORT/BOTH）
            reduce_only: 是否只减仓
            margin_type: 可选的保证金模式（CROSSED/ISOLATED），如果提供则在下单前自动设置
            leverage: 可选的杠杆倍数（1-125），如果提供则在下单前自动设置
        
        Returns:
            订单信息
        
        注意：
            - margin_type和leverage不能作为订单参数传递给Binance API
            - 如果提供这些参数，系统会在下单前自动设置合约配置
            - 如果不提供，则使用配置文件中的默认值（对策略透明）
        """
        try:
            symbol = format_symbol(symbol)
            
            # 自动确保合约配置（对策略透明）
            # 在订单执行前，自动检查并设置保证金模式和杠杆倍数
            if not self.dry_run:
                # _ensure_contract_settings 会从配置读取默认值（灵活适配配置）
                await self._ensure_contract_settings(symbol, margin_type, leverage)
            
            # 获取当前持仓模式（用于正确处理positionSide和reduceOnly）
            dual_side_position = False
            if not self.dry_run:
                try:
                    dual_side_position = await self.get_position_mode()
                except Exception as e:
                    logger.warning(f"Failed to get position mode for {symbol}, assuming one-way mode: {e}")
                    dual_side_position = False
            
            # 根据持仓模式处理positionSide和reduceOnly
            params = {
                'symbol': symbol,
                'side': side,
                'type': order_type,
            }
            
            # 双向持仓模式：必须指定positionSide（LONG/SHORT），不支持reduceOnly
            # 单向持仓模式：使用BOTH或不传递positionSide，支持reduceOnly
            if dual_side_position:
                # 双向持仓模式
                if position_side not in ['LONG', 'SHORT']:
                    # 如果未指定或为BOTH，根据目标持仓方向推断
                    # 注意：这里需要根据实际持仓方向设置，如果无法推断则使用LONG作为默认
                    if reduce_only:
                        # 平仓时，需要根据当前持仓方向设置
                        # 如果无法确定，使用LONG（更保守）
                        logger.warning(f"reduceOnly in dual-side mode for {symbol}, position_side not specified, using LONG")
                        position_side = 'LONG'
                    else:
                        # 开仓时，BUY对应LONG，SELL对应SHORT
                        position_side = 'LONG' if side == 'BUY' else 'SHORT'
                
                params['positionSide'] = position_side
                
                # 双向持仓模式下，reduceOnly参数不被支持，需要使用closePosition
                if reduce_only:
                    # 使用closePosition代替reduceOnly
                    # 注意：使用closePosition时，不能传递quantity参数
                    params['closePosition'] = 'true'
                    # 移除quantity参数（如果存在）
                    if 'quantity' in params:
                        del params['quantity']
                    logger.debug(f"Dual-side mode: using closePosition instead of reduceOnly for {symbol}")
                # 如果不使用closePosition，正常传递quantity
            else:
                # 单向持仓模式
                params['positionSide'] = position_side  # 通常是'BOTH'
                if reduce_only:
                    params['reduceOnly'] = reduce_only
            
            # 注意：在双向持仓模式下，如果使用closePosition，不能传递quantity
            if quantity is not None and 'closePosition' not in params:
                # 确保数量格式化为字符串，避免浮点数精度问题
                # Binance API要求数量必须符合step_size的精度
                # 使用Decimal确保精度，然后转换为字符串
                from decimal import Decimal
                qty_decimal = Decimal(str(quantity))
                # 转换为字符串，移除科学计数法，保留必要的小数位
                # 使用normalize()移除尾随的0，但保留小数点
                qty_str = format(qty_decimal.normalize(), 'f')
                # 如果结果是整数，添加.0以确保是浮点数格式
                if '.' not in qty_str:
                    qty_str = qty_str + '.0'
                params['quantity'] = qty_str
            elif quantity is not None and 'closePosition' in params:
                # 双向持仓模式下使用closePosition时，不能传递quantity
                logger.warning(f"Cannot use quantity with closePosition in dual-side mode for {symbol}, quantity will be ignored")
            
            if price is not None:
                params['price'] = price
            
            if order_type == 'LIMIT':
                params['timeInForce'] = 'GTC'  # Good Till Cancel
            
            # 如果启用dry-run模式，使用test_order endpoint
            if self.dry_run:
                endpoint = '/fapi/v1/order/test'
                logger.info(f"DRY-RUN mode: Using test_order endpoint for {symbol}")
            else:
                endpoint = '/fapi/v1/order'
            
            data = await self._request('POST', endpoint, params=params, signed=True)
            
            # test_order endpoint可能返回空dict，需要构造模拟响应
            if self.dry_run and not data.get('orderId'):
                # 生成模拟订单ID（仅用于dry-run展示）
                import random
                mock_order_id = random.randint(1000000, 9999999)
                data = {
                    'orderId': mock_order_id,
                    'symbol': symbol,
                    'status': 'NEW',
                    'side': side,
                    'type': order_type,
                    'origQty': quantity,
                    'price': price,
                }
            
            logger.info(
                f"{'DRY-RUN: ' if self.dry_run else ''}Order placed: {symbol} {side} {order_type} "
                f"qty={quantity} price={price}, orderId={data.get('orderId')}"
            )
            
            return data
            
        except Exception as e:
            logger.error(f"Failed to place order: {e}", exc_info=True)
            raise

    async def test_order(
        self,
        symbol: str,
        side: str,  # BUY or SELL
        order_type: str,  # MARKET, LIMIT等
        quantity: Optional[float] = None,
        price: Optional[float] = None,
        position_side: str = 'BOTH',  # LONG, SHORT, BOTH
        reduce_only: bool = False
    ) -> Dict:
        """
        下单测试（dry-run，不会产生真实订单）

        使用Binance Futures的 test order endpoint：
        - POST /fapi/v1/order/test

        Returns:
            Binance返回的响应（通常为空 dict 或包含 msg/code）
        """
        try:
            symbol = format_symbol(symbol)

            params = {
                'symbol': symbol,
                'side': side,
                'type': order_type,
                'positionSide': position_side,
                'reduceOnly': reduce_only,
            }

            if quantity is not None:
                params['quantity'] = quantity
            if price is not None:
                params['price'] = price
            if order_type == 'LIMIT':
                params['timeInForce'] = 'GTC'

            data = await self._request('POST', '/fapi/v1/order/test', params=params, signed=True)
            logger.info(f"Order test (dry-run) ok: {symbol} {side} {order_type} qty={quantity} price={price}")
            return data
        except Exception as e:
            logger.error(f"Failed to test order: {e}", exc_info=True)
            raise
    
    async def cancel_order(self, symbol: str, order_id: int) -> Dict:
        """取消订单"""
        try:
            symbol = format_symbol(symbol)
            params = {
                'symbol': symbol,
                'orderId': order_id,
            }
            
            data = await self._request('DELETE', '/fapi/v1/order', params=params, signed=True)
            return data
            
        except Exception as e:
            logger.error(f"Failed to cancel order: {e}", exc_info=True)
            raise
    
    async def get_order_status(self, symbol: str, order_id: int) -> Dict:
        """查询订单状态"""
        try:
            symbol = format_symbol(symbol)
            params = {
                'symbol': symbol,
                'orderId': order_id,
            }
            
            data = await self._request('GET', '/fapi/v1/order', params=params, signed=True)
            return data
            
        except Exception as e:
            logger.error(f"Failed to get order status: {e}", exc_info=True)
            raise
    
    async def get_open_orders(self, symbol: Optional[str] = None) -> List[Dict]:
        """
        查询活跃订单
        
        Args:
            symbol: 交易对，如果不指定则查询所有交易对的活跃订单
        
        Returns:
            活跃订单列表
        """
        try:
            params = {}
            if symbol:
                params['symbol'] = format_symbol(symbol)
            
            data = await self._request('GET', '/fapi/v1/openOrders', params=params, signed=True)
            return data if isinstance(data, list) else []
            
        except Exception as e:
            logger.error(f"Failed to get open orders: {e}", exc_info=True)
            raise
    
    async def cancel_all_orders(self, symbol: str) -> List[Dict]:
        """
        取消指定交易对的所有活跃订单
        
        Args:
            symbol: 交易对
        
        Returns:
            取消结果列表
        """
        try:
            symbol = format_symbol(symbol)
            params = {
                'symbol': symbol,
            }
            
            data = await self._request('DELETE', '/fapi/v1/allOpenOrders', params=params, signed=True)
            return data if isinstance(data, list) else []
            
        except Exception as e:
            logger.error(f"Failed to cancel all orders: {e}", exc_info=True)
            raise
    
    async def change_position_mode(self, dual_side_position: bool) -> Dict:
        """
        设置合约持仓模式
        
        Args:
            dual_side_position: True表示双向持仓模式，False表示单向持仓模式
        
        Returns:
            设置结果
        """
        try:
            params = {
                'dualSidePosition': 'true' if dual_side_position else 'false',
            }
            
            data = await self._request('POST', '/fapi/v1/positionSide/dual', params=params, signed=True)
            return data
            
        except Exception as e:
            logger.error(f"Failed to change position mode: {e}", exc_info=True)
            raise

    async def get_position_mode(self) -> bool:
        """
        获取持仓模式。

        Returns:
            True: 双向持仓模式
            False: 单向持仓模式
        """
        try:
            data = await self._request('GET', '/fapi/v1/positionSide/dual', signed=True)
            # Binance返回: {'dualSidePosition': true/false} 或字符串
            v = data.get('dualSidePosition', False)
            if isinstance(v, str):
                return v.lower() == 'true'
            return bool(v)
        except Exception as e:
            logger.error(f"Failed to get position mode: {e}", exc_info=True)
            raise
    
    async def change_leverage(self, symbol: str, leverage: int) -> Dict:
        """
        设置合约杠杆倍数
        
        Args:
            symbol: 交易对
            leverage: 杠杆倍数（1-125）
        
        Returns:
            设置结果
        """
        try:
            symbol = format_symbol(symbol)
            params = {
                'symbol': symbol,
                'leverage': leverage,
            }
            
            data = await self._request('POST', '/fapi/v1/leverage', params=params, signed=True)
            return data
            
        except Exception as e:
            logger.error(f"Failed to change leverage: {e}", exc_info=True)
            raise
    
    async def change_margin_type(self, symbol: str, margin_type: str) -> Dict:
        """
        设置合约保证金模式
        
        Args:
            symbol: 交易对
            margin_type: 保证金模式（ISOLATED或CROSSED）
        
        Returns:
            设置结果
        """
        try:
            symbol = format_symbol(symbol)
            params = {
                'symbol': symbol,
                'marginType': margin_type.upper(),
            }
            
            data = await self._request('POST', '/fapi/v1/marginType', params=params, signed=True)
            return data
            
        except Exception as e:
            logger.error(f"Failed to change margin type: {e}", exc_info=True)
            raise
    
    async def close(self):
        """关闭HTTP会话"""
        if self.session and not self.session.closed:
            await self.session.close()
            self.session = None
