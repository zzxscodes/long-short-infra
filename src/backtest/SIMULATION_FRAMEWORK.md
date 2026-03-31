# 回测框架：实盘（Live）与仿真（Simulation）对齐说明

本文档对应「回测框架沟通调整」中的 4.1–4.5，与代码位置 `src/backtest/`、`src/strategy/calculator.py` 对齐，便于实现与评审。

---

## 1. 术语

| 概念 | 含义 |
|------|------|
| **Live** | 真实行情/数据流到达后，由引擎按事件驱动调用策略链路。 |
| **Simulation** | 使用历史 K 线（或等价快照），**按时间顺序**推进，不依赖「新数据到达」事件。 |
| **Alpha** | 多因子组合层：对若干 `AlphaCalculator` 的权重做聚合（求和、归一化等），见 `AlphaEngine`。 |
| **Calculator** | 实现 `AlphaCalculatorBase.run(view) -> Dict[symbol, weight]` 的单因子计算单元。 |

---

## 2. 4.2 & 4.4 — Calculator 写法统一、仿真粒度

**约定：Simulation 与 Live 使用同一套 Calculator 实现**（同一类、同一 `run(AlphaDataView)` 签名），禁止维护「仅回测用」的另一套类。

- 基类：`AlphaCalculatorBase`（`src/strategy/calculator.py`）。
- 数据入口：统一为 `AlphaDataView`（`bar_data` / `tran_stats` 等），由**外层驱动**决定 `view` 里装的是「当前 bar 窗口」还是「历史到当前的切片」。

**一次 Simulation 运行的最小业务单元**（4.4）：

- **1 个 Alpha 定义（组合规则） + 1 个 Calculator** 作为常见回测单元；多因子时在该 Alpha 内再挂多个 Calculator，仍由 Alpha 聚合，而不是在回测引擎里写另一套合并逻辑。
- 代码侧已有两种入口，本质一致：
  - **K 线顺序驱动 + 多 calculator 求和**：`MultiFactorBacktestEngine.run()`（`backtest.py`）按 `timestamps` 循环构造 `AlphaDataView`。
  - **重放引擎 + 单 calculator**：`run_backtest()`（`executor.py`）在 `strategy_wrapper` 里对 `klines` 构造 `AlphaDataView` 并调用 `calculator.run(view)`。

---

## 3. 4.3 — 驱动方式：K 线顺序 vs Live 事件

| 维度 | Live | Simulation |
|------|------|------------|
| 驱动 | 新数据到位 / 定时 tick → 回调 | **全局排序后的 K 线时间戳**逐步前进 |
| 时序 | 真实异步 | **for ts in sorted_timestamps**（见 `backtest.py` 中 `timestamps` 循环） |
| `AlphaDataView` | 引擎在「当前时刻」注入窗口 | 重放模块在「该 ts」注入窗口（`rolling_window_bars` 等） |

**要求**：仿真侧不得再实现一套「伪事件总线」去模仿 Live；**唯一推进变量是 K 线时间轴**（可多标的合并时间戳，与现有 `replay` / `backtest.run` 一致）。

---

## 4. 4.1 — Live Calculator 增加「数据落地」

**目的**：实盘在计算权重的同时，把可复现的中间结果或输出落盘（或写入消息队列），使 Simulation 可对照、可审计，并减轻重复计算。

**建议形态（实现时二选一或组合）**：

1. **引擎侧统一落地（推荐）**  
   在 Live 的 `AlphaEngine`（或等价调度点）在每次 `calculator.run(view)` 之后，由框架写入：
   - `run_id / strategy_id / calculator_name`
   - `ts`（bar open_time）
   - `weights`（或哈希）
   - 可选：`view` 元信息（universe 版本、interval）  
   格式：Parquet 追加分区、或按天滚动文件，避免每 tick 大量小文件（见 4.5）。

2. **Calculator 可选钩子（补充）**  
   在 `AlphaCalculatorBase` 上增加可选方法，例如 `on_after_run(self, view, weights, ctx)`，默认空；仅当策略需要落自定义特征时再实现。**业务计算仍只在 `run()` 中**，避免两套逻辑。

**与 Simulation 的关系**：Simulation 以历史数据重放为主；若需与 Live 逐 bar 对比，应以**同一套 `run(view)` 输出**为准，落地文件仅作校验与排障。

---

## 5. 4.5 — 多研究员、多任务并发下的 IO 压力

场景：多名研究员不定时各自运行若干次「Alpha + 1 个 Calculator」全历史回测，**读历史 K 线 + 写结果/日志** 叠加，易打满磁盘 IOPS 与元数据。

**架构层**

- **任务队列 + 工作池**：提交到队列（Celery/RQ/内部队列），限制全局并发 worker 数；避免 shell 并发扫满磁盘。
- **按 data snapshot / 版本号复用**：同一 `universe` + `interval` + 日期区间的输入数据只准备一份（共享只读目录或内容寻址存储），任务只读该路径。

**数据层**

- **读**：进程内 / 节点内 **LRU 缓存**已解析的 DataFrame 或 mmap；多任务同机时共享只读 mmap（Parquet/Feather）。
- **写**：**批量、按分区**写结果（例如每 N 根 K 线或每 1 分钟 flush），禁止每 bar 单独 `open/write/close`。
- **合并小文件**：日终或任务结束做 compaction（单 Parquet 或按 `(date, calculator_id)` 目录）。

**部署层**

- 仿真集群与 **实盘写入路径隔离**（不同盘或不同存储池）；大任务用本地 NVMe scratch，结束后再上传到共享存储。
- **配额与优先级**：按用户/项目限流写带宽与并发任务数。

**产品层**

- **去重**：相同参数 + 相同代码版本的任务返回缓存结果 ID，避免重复全量跑。

---

## 6. 与现有代码的映射（便于改代码时对照）

| 能力 | 主要位置 |
|------|----------|
| K 线顺序、多 calculator、`AlphaDataView` | `src/backtest/backtest.py` — `MultiFactorBacktestEngine.run` |
| 重放引擎、按存储加载 K 线 | `src/backtest/replay.py` — `DataReplayEngine` |
| 单 calculator + Executor | `src/backtest/executor.py` — `run_backtest`、`strategy_wrapper` |
| Calculator 协议 | `src/strategy/calculator.py` — `AlphaCalculatorBase`, `AlphaDataView` |
| 增量 / alpha 快照（减重复算） | `backtest.py` — `save_alpha`、`run_mode=increment`、`_alpha_snapshot_path` |

---

## 7. 后续实现 checklist（供开发拆分）

- [ ] Live：在 Alpha 调度路径增加可配置的「权重/元数据」落地（格式与分区策略）。
- [ ] 统一文档与类型：明确 Simulation **仅**使用 K 线时间序驱动（已在 `backtest.run` / `run_backtest` 体现，需在 PR 描述中写清）。
- [ ] 运维：仿真任务队列、并发上限、共享数据目录规范、结果目录按 `run_id` 命名空间隔离。
