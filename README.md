# Binance永续合约多空策略量化交易系统

基于Binance USDT本位永续合约的多空量化交易系统。采用多进程架构，实时接收归集逐笔成交数据，自行聚合生成多周期K线，支持多账户交易和多种订单执行策略。

## 核心特性

- **实时数据采集**: WebSocket实时接收归集逐笔成交数据（@aggTrade），自行聚合生成多周期K线
- **多周期支持**: 支持5min, 1h, 4h, 8h, 12h, 24h多周期K线聚合
- **多进程架构**: 5个独立进程，通过IPC和文件信号通信
- **多账户支持**: 支持配置多个交易账户，每个账户独立执行进程
- **Web监控界面**: 提供实时Web监控面板（http://localhost:8080），支持进程状态、账户信息、系统监控
- **多种订单策略**: 支持Market订单、TWAP（时间加权平均价格）、VWAP（成交量加权平均价格）
- **自动合约设置**: 自动应用保证金模式、持仓模式、杠杆倍数
- **Universe管理**: 每天23:55（北京时间）自动更新可交易资产列表，支持版本管理（v1, v2等）
- **溢价指数K线**: 自动采集和存储溢价指数K线数据
- **历史资金费率**: 自动采集和存储历史资金费率数据
- **每日官方K线对比**: 定时对比 Binance 官方 5m K 线与自聚合结果，通过则同步到回测机；不通过则重拉 aggTrade、重聚合后同步
- **回测数据同步**: 支持两种模式：(1) 实盘机主动推送；(2) **回测机主动拉取**（推荐），从实盘机器拉取数据、对比官方、自动修复不一致
- **防重复触发**: 策略计算防重复触发机制，确保计算完整性
- **数据完整性检查**: 支持严格完整性模式（覆盖率与最小点数阈值），确保策略触发时机准确
- **无成交K线处理**: 自动生成无成交时段的K线数据
- **监控告警**: 实时监控持仓偏差、敞口、账户健康度
- **净值曲线追踪**: 自动记录账户净值曲线，支持历史数据查看和分析
- **持仓历史记录**: 自动记录所有开仓历史，用于策略分析和回测
- **策略报告生成**: 自动生成策略执行报告，包括交易统计、策略磨损分析等
- **统一策略API**: StrategyAPI封装数据层、系统层和执行层，简化策略开发
- **多因子架构**: 支持多研究员独立开发calculators（因子），系统自动发现、并发执行并求和
- **因子并发执行**: 默认使用线程池并发执行所有calculators，提升计算性能

## 系统架构

```
事件驱动协调进程 (Process 1) - IPC Server
           ↕ IPC通信
数据层进程 (Process 2) - Trade Collector + Kline Aggregator
           ↕ 文件信号
策略进程 (Process 3) - Position Calculator
           ↕ 文件信号
订单执行进程 (Process 4) - Order Manager (多实例，每账户一个)
           ↕ IPC通信
监控进程 (Process 5) - Metrics & Alerts + Web API
           ↕ HTTP API
Web监控界面 (http://localhost:8080)
```

**进程说明**：
1. **事件驱动协调进程**: 协调各进程间通信，作为IPC服务器
2. **数据层进程**: 实时接收归集逐笔成交（@aggTrade），聚合生成多周期K线，管理Universe，采集资金费率和溢价指数K线
3. **策略进程**: 执行策略计算，使用Alpha引擎并发运行多个calculators（因子），生成目标持仓文件，支持防重复触发机制
4. **订单执行进程**: 执行交易订单，支持Market/TWAP/VWAP多种订单策略，支持多账户（每个账户一个进程实例），自动记录持仓历史
5. **监控进程**: 监控系统状态、账户信息、持仓偏差、敞口等指标，记录净值曲线，生成策略报告，提供HTTP API和Web监控界面

## 快速开始

### 安装

**Windows**:
```cmd
setup_windows.bat
quant\Scripts\activate.bat
```

**Linux/macOS**:
```bash
sed -i 's/\r$//' setup_linux.sh
chmod +x setup_linux.sh
./setup_linux.sh
source quant/bin/activate
```

### 配置

编辑 `config/default.yaml`:

```yaml
strategy:
  history_days: 30           # 策略计算使用的历史数据天数
  calculation_interval: 5m   # 计算间隔
  
  # Alpha引擎并发配置
  alpha:
    concurrency: thread      # thread（线程池，默认）| process（进程池）| none（串行）
    max_workers: null         # 最大并发数，null表示自动设置为calculators数量
  
  # Calculator（因子）配置
  calculators:
    enabled:                  # 指定要加载的calculators，不配置则自动发现所有
      # - "mean_buy_dolvol4_over_dolvol_rank"
      # - "researcher1_momentum_factor"

execution:
  mode: testnet  # testnet (测试网), mock (模拟), live (实盘)
  dry_run: false  # 所有模式都可以配置dry-run开关
  
  contract_settings:
    margin_type: CROSSED      # CROSSED (全仓) 或 ISOLATED (逐仓)
    position_mode: one_way    # one_way (单向) 或 dual_side (双向)
    leverage: 20              # 杠杆倍数: 1-125
  
  testnet:
    accounts:
      - account_id: account1
        api_key: your_testnet_key
        api_secret: your_testnet_secret
```

常用的数据层参数（`data.*`）：

```yaml
data:
  # K线聚合与内存控制
  kline_aggregator_max_klines: 8
  kline_aggregator_max_pending_windows: 8
  kline_aggregator_max_trades_per_window: 0
  kline_close_grace_windows: 6
  kline_snapshot_max_windows_per_symbol: 2

  # pending逐笔压缩（按窗口新旧分层）
  kline_aggregator_pending_compact_old_window_threshold: 120
  kline_aggregator_pending_compact_old_window_keep_tail: 20
  kline_aggregator_pending_compact_current_window_threshold: 260
  kline_aggregator_pending_compact_current_window_keep_tail: 40
  kline_aggregator_pending_compact_trades_soft_limit: 2000000

  # 内存清理
  memory_cleanup_interval: 60
  memory_force_rebuild_interval: 60
  memory_aggressive_cleanup_threshold_mb: 900

  # 网络抖动下的补缺与限流
  collector_gap_recover_enabled: true
  collector_gap_recover_max_trades: 3000
  collector_gap_recover_global_budget_per_minute: 15000
```

**配置优先级**: 环境变量 > 配置文件
- Testnet: `{ACCOUNT_ID}_TESTNET_API_KEY` / `{ACCOUNT_ID}_TESTNET_API_SECRET`
- Live: `{ACCOUNT_ID}_API_KEY` / `{ACCOUNT_ID}_API_SECRET`

**回测数据同步**（`data.*`）：

```yaml
data:
  backtest_klines_dest: data/backtest_klines
  backtest_funding_rates_dest: data/backtest_funding_rates
  backtest_premium_index_dest: data/backtest_premium_index
  daily_compare_use_data_vision: true  # 使用 data.binance.vision，避免占用 fapi 限流
```

### 启动/停止

```bash
# 启动系统
python start_all.py

# 停止系统
python stop_all.py

# 访问Web监控界面
# http://localhost:8080
```

## 执行模式

### Testnet模式（推荐用于策略验证）
- 使用Binance测试网环境
- 需要Testnet API密钥
- 不会产生真实订单

### Mock模式（推荐用于开发测试）
- 完全离线模拟，不需要API密钥
- 所有订单都是模拟的
- 适合策略开发、测试和调试

### Live模式（实盘交易）
- **会产生真实订单和真实交易**
- 需要真实API密钥
- 建议先使用 `dry_run: true` 进行测试

**Dry-Run开关**: 所有模式都可以配置 `dry_run: true`，即使使用真实API也不会产生真实订单。

## Web监控界面

系统提供实时Web监控面板，访问 `http://localhost:8080` 即可查看：

- **系统进程状态**: 实时显示所有进程运行状态，支持多账户执行进程监控
- **系统信息**: CPU核心数、内存使用率等系统资源信息
- **账户监控**: 账户余额、持仓、敞口、偏差等实时指标
- **告警信息**: 持仓偏差、敞口、账户健康度告警
- **净值曲线**: 实时显示账户净值曲线，支持历史数据查看
- **策略报告**: 显示策略执行报告，包括交易对、开仓记录、策略磨损分析等
- **自动刷新**: 支持5秒自动刷新，实时更新监控数据

## 策略开发

### 多因子架构（Calculators）

系统采用多因子架构，支持多研究员独立开发calculators（因子），系统自动发现、并发执行并求和。

#### 快速开始

1. **创建新的Calculator文件**

```bash
# 复制模板文件
cp src/strategy/calculators/template.py src/strategy/calculators/your_name_factor_name.py
```

2. **实现Calculator类**

```python
from ..calculator import AlphaCalculatorBase, AlphaDataView
from typing import Dict

class YourFactorCalculator(AlphaCalculatorBase):
    def __init__(self):
        self.name = "your_name_factor_name"  # 必须唯一
        self.mutates_inputs = False  # 推荐False，性能更好

    def run(self, view: AlphaDataView) -> Dict[str, float]:
        """实现你的因子逻辑"""
        weights = {}
        for sym in view.iter_symbols():
            # 获取数据（从view中已有的数据）
            bar_df = view.get_bar(sym, tail=100)  # 最近100根K线
            tran_df = view.get_tran_stats(sym, tail=100)
            
            # 或者通过view访问数据API（获取指定时间范围的数据）
            # bar_data = view.get_bar_between('2025-01-01-000', '2025-01-02-000', mode='5min')
            # funding_rates = view.get_funding_rate_between('2025-01-01-000', '2025-01-02-000')
            # universe = view.get_universe()  # 获取可交易资产列表
            
            # 计算因子
            score = your_calculation(bar_df, tran_df)
            weights[sym] = score
        
        return weights

# 创建实例（系统会自动发现）
CALCULATOR_INSTANCE = YourFactorCalculator()
```

3. **系统自动加载**

系统会在启动时自动发现并加载你的Calculator，无需修改核心代码。

#### 并发执行

- **默认行为**: 当有多个calculators（>1）时，自动使用线程池并发执行
- **性能优势**: 2个calculators在0.73秒内完成，相比串行执行提升约2倍
- **数据安全**: 每个calculator获得独立的数据视图，互不影响
- **配置选项**: 可在`config/default.yaml`中配置并发模式（thread/process/none）

详细开发指南请参考：`src/strategy/calculators/README.md`

#### AlphaDataView数据访问接口

`AlphaDataView`提供了与`StrategyAPI`相同的数据访问接口，Calculator可以通过`view`参数直接访问：

```python
def run(self, view: AlphaDataView) -> Dict[str, float]:
    # 访问已加载的快照数据
    bar_df = view.get_bar(sym, tail=100)
    tran_df = view.get_tran_stats(sym, tail=100)
    
    # 或者通过view访问数据API（获取指定时间范围的数据）
    # 获取多周期bar数据
    bars_5min = view.get_bar_between('2025-01-01-000', '2025-01-02-000', mode='5min')
    bars_1h = view.get_bar_between('2025-01-01-000', '2025-01-02-000', mode='1h')
    
    # 获取多周期tran_stats数据
    tran_stats_5min = view.get_tran_stats_between('2025-01-01-000', '2025-01-02-000', mode='5min')
    
    # 获取溢价指数K线
    premium_index = view.get_premium_index_bar_between('2025-01-01-000', '2025-01-02-000', mode='5min')
    
    # 获取资金费率
    funding_rates = view.get_funding_rate_between('2025-01-01-000', '2025-01-02-000')
    
    # 获取Universe（支持版本）
    universe = view.get_last_universe(version='v1')
    universe_by_date = view.get_universe(date='2025-01-01', version='v1')
    
    return weights
```

**接口说明**：
- 时间标签格式：`'YYYY-MM-DD-HHH'`（例如：`'2025-12-22-004'`），其中HHH为1-288（每天288个5分钟窗口）
- 周期模式：`mode='5min'`（默认）、`'1h'`、`'4h'`、`'8h'`、`'12h'`、`'24h'`
- Universe版本：`version='v1'`（默认）、`'v2'`等
- 返回格式：`{'btc-usdt': DataFrame, 'eth-usdt': DataFrame, ...}`

这些接口与`StrategyAPI`完全一致，确保Calculator可以访问所有需要的数据。

### 使用StrategyAPI（已废弃，推荐使用Calculators架构）

修改 `src/strategy/calculator.py` 中的 `_execute_strategy` 方法实现自定义策略。

```python
from src.api.strategy_api import get_strategy_api

strategy_api = get_strategy_api()

# 获取多周期bar数据
bars_5min = strategy_api.get_bar_between(begin_label, end_label, mode='5min')
bars_1h = strategy_api.get_bar_between(begin_label, end_label, mode='1h')

# 获取多周期tran_stats数据
tran_stats_5min = strategy_api.get_tran_stats_between(begin_label, end_label, mode='5min')

# 获取溢价指数K线
premium_index = strategy_api.get_premium_index_bar_between(begin_label, end_label, mode='5min')

# 获取资金费率
funding_rates = strategy_api.get_funding_rate_between(begin_label, end_label)

# 获取Universe（支持版本）
universe = strategy_api.get_last_universe(version='v1')
```

**参数格式**: 
- 时间标签：`'YYYY-MM-DD-HHH'`（例如：`'2025-12-22-004'`），其中HHH为1-288（每天288个5分钟窗口）
- 周期模式：`mode='5min'`（默认）、`'1h'`、`'4h'`、`'8h'`、`'12h'`、`'24h'`
- Universe版本：`version='v1'`（默认）、`'v2'`等

**返回格式**: `{'btc-usdt': DataFrame, 'eth-usdt': DataFrame, ...}`

### 策略特性

- **防重复触发**: 策略计算过程中自动防止新的data_complete触发，确保计算完整性
- **数据完整性**: 支持严格完整性校验（5m K线 24h=288、funding 最近3天>=9、premium 最近3天>=864）后再触发策略
- **Universe自动更新**: 每天23:55自动更新Universe，WebSocket自动重新订阅新的交易对
- **测试策略**: testnet非dry-run模式下使用测试策略（按成交额排序，大的做空，小的做多）

## 系统特性

### 订单执行策略

系统支持多种订单执行策略：
- **Market订单**: 立即以市价执行
- **TWAP订单**: 时间加权平均价格，将订单按时间均匀分配到多个时间段
- **VWAP订单**: 成交量加权平均价格，根据历史成交量分布分配订单

### 数据完整性保障

- **严格覆盖检查**: `strict_data_completeness: true` 时，币种覆盖需与 universe 一致
- **最小点数检查**: 5m K线最近24h至少288条；funding最近3天至少9条；premium最近3天至少864条
- **窗口关闭判断**: 通过窗口时间判断，确保时间段内的所有交易都已收到
- **无成交处理**: 自动生成无成交时段的K线，确保数据连续性
- **防重复触发**: 策略计算过程中防止新的data_complete触发

### 数据聚合

系统从归集逐笔成交数据（@aggTrade）自行聚合生成多周期K线：
- **数据源**: 使用Binance归集逐笔（@aggTrade），减少数据量，提高处理效率
- **支持周期**: 5min（基础），1h, 4h, 8h, 12h, 24h（从5min聚合）
- **基础字段**: `open`, `high`, `low`, `close`, `volume`, `quote_volume`, `trade_count`
- **扩展字段**: `vwap`, `dolvol`, `buydolvol`, `selldolvol`, `buyvolume`, `sellvolume`
- **时间字段**: `open_time`, `close_time`, `time_lable`（范围1-288，每天288个5分钟窗口）
- **无成交处理**: 无成交时段自动生成K线，ohlc=上一K线close，vwap=nan，成交额为0
- **Transtats阈值**: 人民币阈值自动转换为USD（除以7.0）

使用Polars进行高性能向量化计算，支持bar表和tran_stats表两种数据格式。

## 聚合准确性校验（临时脚本）

项目提供了临时校验脚本：`scripts/temp_validate_data_layer_vs_binance.py`，用于对比本地聚合结果与 Binance 官方 `fapi/v1/klines` 的重合字段一致性。

- 对比字段：`open/high/low/close/volume/quote_volume/trade_count`
- 对比口径：同一 `symbol + 5m window`
- 本地路径：复用聚合器实时路径（`add_trade -> _aggregate_window`）

示例：

```bash
python scripts/temp_validate_data_layer_vs_binance.py \
  --symbols 40 \
  --window-count 2 \
  --skip-recent-windows 2 \
  --concurrency 4 \
  --max-pages 30
```

说明：
- `skip-recent-windows` 建议 >=2，避免比较到仍在收敛的近端窗口；
- Binance 可能返回 `429`，可降低 `--concurrency` 或缩小 `--symbols` 重试；
- 脚本退出码为 0 表示本轮样本全部通过。

## 每日官方K线对比与回测数据同步

系统支持两种数据同步模式，均使用 `data.binance.vision` 官方数据进行对比校验：

| 模式 | 运行环境 | 说明 |
|------|----------|------|
| **实盘机推送** | 实盘机 | 实盘机主动对比、同步数据至回测机 |
| **回测机拉取**（推荐） | 回测机 | 回测机从实盘机拉取数据，本地对比校验，自动修复不一致 |

建议 cron 时间：**UTC 10:30**（`30 10 * * *`，配合 `CRON_TZ=UTC`），此时 data.binance.vision 当日文件通常已可用。

---

### 模式一：实盘机推送

脚本 `scripts/daily_official_5m_compare.py` 在**实盘机**上运行：

1. **对比** Binance 官方 5m K 线与本地自聚合 K 线
2. **K 线同步**：对比通过则同步当日 K 线至回测机；不通过则重拉 aggTrade、重聚合、覆盖本地后再同步
3. **funding_rates / premium_index 同步**：不做对比，直接将当日数据同步至回测机

**安装定时任务**：

```bash
# Linux/macOS
./scripts/setup_daily_compare_cron.sh
# 或
python scripts/setup_daily_compare_scheduler.py --time 10:30

# Windows
scripts\setup_daily_compare.bat
# 或
python scripts/setup_daily_compare_scheduler.py --time 10:30
```

**配置项（config/default.yaml）**：

```yaml
data:
  backtest_klines_dest: data/backtest_klines        # K 线同步目标（本地路径或 rsync user@host:/path）
  backtest_funding_rates_dest: data/backtest_funding_rates   # funding_rates 同步目标
  backtest_premium_index_dest: data/backtest_premium_index   # premium_index 同步目标
  daily_compare_use_data_vision: true               # 使用 data.binance.vision（推荐，不占 fapi 限流）
```

**脚本参数示例**：

```bash
# 仅对比（不同步）
python scripts/daily_official_5m_compare.py --compare-only --day 2026-03-10

# 指定交易对
python scripts/daily_official_5m_compare.py --symbols BTCUSDT,ETHUSDT

# 指定同步目标
python scripts/daily_official_5m_compare.py --backtest-dest user@host:/data/backtest_klines
```

---

### 模式二：回测机拉取（推荐）

脚本 `scripts/backtest_pull_compare.py` 在**回测机**上运行：

1. **拉取数据**：通过 SSH/SCP 从实盘机器拉取当日 klines/funding_rates/premium_index 数据
2. **对比校验**：下载 Binance 官方 5m K 线（data.binance.vision），与拉取的数据对比
3. **自动修复**：对比不通过则从 data.binance.vision 下载历史 aggTrade，聚合覆盖本地数据

**前置条件**：
- 配置 SSH 免密登录到实盘机器（`ssh-keygen` + `ssh-copy-id`）
- 在 `config/default.yaml` 中配置 `backtest_pull` 相关参数

**安装定时任务**：

```bash
# Linux/macOS
python scripts/setup_backtest_compare_scheduler.py --time 10:30

# Windows（默认 18:30 本地时间，对应 UTC+8 的 UTC 10:30）
scripts\setup_backtest_compare.bat
# 或
python scripts/setup_backtest_compare_scheduler.py --time 18:30
```

**配置项（config/default.yaml）**：

```yaml
data:
  # 回测机拉取模式配置
  backtest_pull_live_host: "192.168.1.100"          # 实盘机器 IP/hostname
  backtest_pull_live_user: "quant"                  # SSH 用户名
  backtest_pull_live_klines_dir: "/home/quant/long-short-infra/data/klines"
  backtest_pull_live_funding_rates_dir: "/home/quant/long-short-infra/data/funding_rates"
  backtest_pull_live_premium_index_dir: "/home/quant/long-short-infra/data/premium_index"
  backtest_pull_live_universe_dir: "/home/quant/long-short-infra/data/universe"  # Universe 目录
```

**自动拉取 Universe**：如果本地没有 universe 文件，脚本会自动从实盘机器拉取。

**脚本参数示例**：

```bash
# 完整流程（拉取 + 对比 + 修复）
python scripts/backtest_pull_compare.py --day 2026-03-10

# 仅对比（跳过拉取，使用本地已有数据）
python scripts/backtest_pull_compare.py --compare-only --day 2026-03-10

# 指定实盘机器参数
python scripts/backtest_pull_compare.py --live-host 192.168.1.100 --live-user quant \
  --live-klines-dir /home/quant/data/klines
```

---

### 通用说明

- Symbol 来源：不传 `--symbols` 时从 universe（`data/universe/{date}/v1/universe.csv`）加载
- 回测机需将 `data.klines_directory`、`data.funding_rates_directory`、`data.premium_index_directory` 分别指向对应数据目录，布局为 `{dir}/{SYMBOL}/{YYYY-MM-DD}.parquet`

## 数据存储结构

```
data/
├── universe/                    # Universe文件（支持版本）
│   └── YYYY-MM-DD/
│       ├── v1/universe.csv
│       └── v2/universe.csv
├── klines/                      # K线数据（Parquet格式）
│   └── {SYMBOL}/YYYY-MM-DD.parquet
├── trades/                      # 归集逐笔成交数据
│   └── {SYMBOL}/YYYY-MM-DD-HHh.parquet
├── funding_rates/              # 资金费率数据（Parquet格式）
│   └── {SYMBOL}/YYYY-MM-DD.parquet
├── premium_index/              # 溢价指数K线数据（Parquet格式）
│   └── {SYMBOL}/YYYY-MM-DD.parquet
├── positions/                   # 目标持仓文件
│   └── {account_id}_target_positions_{timestamp}.json
├── signals/                     # 进程间信号文件
│   ├── strategy_trigger.json
│   └── execution_trigger_{account_id}.json
├── equity_curve/                # 净值曲线数据（JSON格式，单个文件覆盖模式）
│   └── {account_id}_equity_curve.json
├── position_history/            # 持仓历史数据（JSON格式，单个文件覆盖模式）
│   └── {account_id}_position_history.json
└── strategy_reports/            # 策略报告数据（JSON格式，单个文件覆盖模式）
    └── {account_id}_strategy_report_latest.json
```

**回测数据同步目标**（由定时任务 `daily_official_5m_compare.py` 写入）：

| 配置项 | 默认路径 | 说明 |
|--------|----------|------|
| `data.backtest_klines_dest` | `data/backtest_klines` | K 线同步目标（对比通过后） |
| `data.backtest_funding_rates_dest` | `data/backtest_funding_rates` | 资金费率同步目标 |
| `data.backtest_premium_index_dest` | `data/backtest_premium_index` | 溢价指数同步目标 |

布局均为 `{dest}/{SYMBOL}/{YYYY-MM-DD}.parquet`，支持本地路径或 rsync 目标（如 `user@host:/path`）

## 项目结构

```
long-short-infra/
├── src/
│   ├── api/                  # 策略API：统一封装数据层、系统层、执行层
│   │   └── strategy_api.py
│   ├── data/                 # 数据层：采集、聚合、存储
│   │   ├── collector.py      # 归集逐笔成交采集器（@aggTrade）
│   │   ├── kline_aggregator.py
│   │   ├── multi_interval_aggregator.py
│   │   ├── funding_rate_collector.py
│   │   ├── funding_market_collector.py  # WebSocket 资金费率/标记价格
│   │   ├── premium_index_collector.py
│   │   ├── daily_official_compare.py   # 官方5m对比与回测数据同步
│   │   ├── universe_manager.py
│   │   ├── storage.py
│   │   └── api.py
│   ├── strategy/             # 策略层：Alpha引擎、Calculators、持仓生成
│   ├── execution/            # 执行层：订单管理、Binance 客户端
│   ├── monitoring/           # 监控层：指标、告警、策略报告、Web API
│   ├── common/               # 公共模块：配置、日志、IPC、网络工具
│   ├── processes/            # 进程入口
│   └── backtest/             # 回测模块（独立于实盘流程）
├── scripts/
│   ├── daily_official_5m_compare.py    # 实盘机：每日官方K线对比与回测数据同步
│   ├── setup_daily_compare_scheduler.py # 实盘机：定时任务安装（cron/schtasks）
│   ├── setup_daily_compare_cron.sh
│   ├── setup_daily_compare.bat
│   ├── daily_compare_launcher.bat
│   ├── backtest_pull_compare.py        # 回测机：主动拉取数据并对比修复
│   ├── setup_backtest_compare_scheduler.py # 回测机：定时任务安装（cron/schtasks）
│   ├── setup_backtest_compare.bat
│   ├── backtest_pull_launcher.sh
│   ├── backtest_pull_launcher.bat
│   ├── temp_validate_data_layer_vs_binance.py  # 聚合准确性校验
│   └── data_layer_memory_stress.py     # 数据层内存压力测试
├── web/
│   └── monitor.html          # 监控面板前端
├── config/
│   └── default.yaml
├── data/                     # 数据存储
├── logs/
├── start_all.py
└── stop_all.py
```

## 注意事项

1. **账户配置**: 如果配置了 `account_id`，必须填写完整配置；占位 API Key/Secret 会被启动校验自动跳过
2. **API密钥安全**: 建议使用环境变量配置API密钥
3. **测试顺序**: 建议按 mock → testnet → live 的顺序进行测试
4. **实盘交易**: Live模式下会产生真实订单，请确保策略已经充分测试
5. **日志查看**: 系统日志保存在 `logs/` 目录，建议定期检查
6. **时间标签范围**: time_label范围为1-288（每天288个5分钟窗口）
7. **Universe更新**: 每天23:55自动更新Universe，WebSocket会自动重新订阅新的交易对
8. **数据完整性**: 可开启严格模式，按 universe 覆盖率与 288/9/864 阈值进行触发前校验
9. **策略开发**: 推荐使用Calculators架构进行因子开发，支持多研究员协作和并发执行
10. **Web监控**: 访问 `http://localhost:8080` 查看实时监控界面，支持多账户执行进程状态监控
11. **Calculators并发**: 默认使用线程池并发执行所有calculators，可通过配置调整为进程池或串行模式
12. **因子开发**: 在`src/strategy/calculators/`目录下创建因子文件，系统会自动发现并加载
13. **回测数据同步**: 支持两种模式：(1) 实盘机推送（`scripts/daily_official_5m_compare.py`）；(2) 回测机拉取（`scripts/backtest_pull_compare.py`，推荐）。回测机须将 `data.klines_directory`、`data.funding_rates_directory`、`data.premium_index_directory` 指向对应数据目录

## 版本更新

### 最新功能（当前版本）

- ✅ **严格数据完整性模式**: 支持 `strict_data_completeness: true`，按 universe 严格检查覆盖率；5m K 线按 24h `288` 条校验，funding/premium 按 3 天最小点数校验
- ✅ **K线补缺闭环**: 当检测到 5m K 线短缺时，自动触发补空窗口逻辑，确保窗口连续性
- ✅ **Funding/Premium 分批 WebSocket**: `funding_market_collector` 支持按 `mark_price_max_symbols_per_connection` 分批连接，降低单连接 stream 过载导致的超时/重连风险
- ✅ **实盘启动前账户校验**: `start_all.py` 启动时会校验 live/testnet 账户配置完整性，自动跳过占位密钥账户并给出明确提示
- ✅ **数据目录分层存储**: 当前默认目录结构为 `data/universe/{date}/v*/universe.csv` 与 `{type}/{SYMBOL}/{YYYY-MM-DD}.parquet`，便于按日审计与回放
- ✅ **每日官方K线对比与回测同步**: 支持实盘机推送与回测机拉取两种模式，使用 data.binance.vision 做官方对比与修复
- ✅ **多因子并发执行**: calculators 自动发现并支持线程池并发；`AlphaDataView` 提供统一数据访问接口