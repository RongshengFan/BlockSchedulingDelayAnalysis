# BlockSchedulingDelayAnalysis

这是一个基于 Neutrino 的 GPU Block 调度行为分析项目。项目围绕 block 级 trace 采集、结构化解析、指标计算与结果展示组织，目标是把原始 trace 转换为可直接用于分析的 CSV、统计表、静态图和本地 Dashboard，并以 `sched` 为主指标刻画同一 SM 上 block replacement 过程中的等待间隔。

当前默认覆盖五类代表性 workload：

- `compute`
- `memory`
- `mixed`
- `sparse`
- `vgg16`

## 项目概览

项目主线按数据流转组织为三个核心阶段：

1. `数据采集`
   使用 Neutrino 探针采集 block 级 trace。
2. `数据解析与指标计算`
   将原始二进制 trace 转换为结构化表，并计算调度、负载均衡和执行时长对照指标。
3. `结果展示`
   输出指标表、静态图，并通过本地 Dashboard 进行浏览。

`cli`、`scenario_runner`、`validators`、`reporting` 等脚本属于辅助层，用于批量执行、结果整理或额外检查，不构成主分析链路。

## 系统架构

从工程实现角度看，项目可以拆成六个层次：

1. `采集定义层`
   `probe/` 负责定义 Neutrino 探针，决定 block 级 trace 记录哪些字段。
2. `负载执行层`
   `workloads/` 负责构造不同 workload 与 batch 组合，并驱动 CUDA kernel 实际运行。
3. `原始数据层`
   `traces/` 保存 Neutrino 采集得到的原始二进制 trace，是后续一切分析的输入来源。
4. `分析计算层`
   `analysis/` 负责解析 trace、重建 `Sched`、计算 load / elapsed 对照指标，并输出汇总表。
5. `结果产出层`
   `output/data/` 保存 block 级结构化数据，`output/chart/metrics/` 保存汇总表，`output/chart/sched`、`output/chart/load`、`output/chart/correlation`、`output/chart/overview` 保存静态图。
6. `展示与验证层`
   `dashboard/` 提供只读浏览界面，`tests/` 和 `analysis/validators.py` / `analysis/metrics_guard.py` 负责一致性检查与结果守卫。

如果按“输入 -> 处理中间层 -> 输出”的角度来看，完整架构可以概括为：

```text
probe/ + workloads/
-> traces/
-> analysis/parse_to_csv.py
-> output/data/
-> analysis/recompute_sched_metrics.py + analysis/analyze_and_plot.py
-> output/chart/metrics/
-> output/chart/sched + output/chart/load + output/chart/correlation + output/chart/overview
-> dashboard/
```

其中：

- `configs/` 提供 pipeline 与场景参数。
- `neutrino/` 提供项目内置的 Neutrino 运行基础。
- `tests/` 保证解析、指标重建、报告生成与结果校验逻辑不会随改动漂移。

## 核心链路

```text
probe + workloads
-> traces/
-> output/data/
-> output/chart/metrics/base/
-> output/chart/
-> dashboard/
```

对应到脚本层面，主流程可概括为：

```text
neutrino 安装
-> workloads/collect_workloads.sh
-> analysis/parse_to_csv.py
-> analysis/recompute_sched_metrics.py
-> analysis/analyze_and_plot.py
-> dashboard/server.py
```

### 1. 数据采集

- `probe/probe.py`
  定义 Neutrino 探针，记录 block 的起始时刻、执行时长与所在 SM。
- `workloads/main.py`
  负责按 workload / batch 启动目标 CUDA 负载。
- `workloads/collect_workloads.sh`
  编译 workload 扩展，并按 workload-batch 组合批量运行 Neutrino 采集。

### 2. 数据解析与指标计算

- `analysis/parse_to_csv.py`
  解析 `traces/*/bs*/trace/*/result/*.bin`，生成统一格式的 block 级 CSV。
- `analysis/recompute_sched_metrics.py`
  基于同一 SM 上 block replacement 的接续过程，重算 Neutrino 风格的 `Sched` 汇总指标。
- `analysis/analyze_and_plot.py`
  汇总 sched、load、elapsed 等指标，并输出统计表与静态图。

### 3. 结果展示

- `output/`
  保存结构化数据、指标汇总结果与图表产物。
- `dashboard/server.py`
  提供只读本地网页，用于查看 `output/` 下已生成的分析结果。

## 图表家族概览

当前结果图按用途分成四组：

| 图组 | 路径 | 关注重点 |
| --- | --- | --- |
| `sched` | `output/chart/sched/` | 观察 `Sched` 的整体水平、尾部规模、事件覆盖率与 workload-batch 分层 |
| `load` | `output/chart/load/` | 观察 SM 间 block 分布、累计执行时长分布及负载均衡性 |
| `correlation` | `output/chart/correlation/` | 直接观察 `Sched` 与负载均衡指标的关联关系 |
| `overview` | `output/chart/overview/` | 观察 block 级执行时长分布，为 `Sched` 现象提供执行代价对照 |

如果要写论文正文，可以把它们分别对应为：

- `4.2 总览分析`：以 `sched` 图组为主，辅以风险排序表。
- `4.3 Sched 指标分析`：继续使用 `sched` 图组中的 P95、ECDF、分位线与热力图。
- `4.4 负载均衡与 Sched 的关联分析`：以 `correlation` 图组为主，必要时再结合 `load` 图组做局部解释。
- `4.5 与执行时长的区分`：以 `overview` 图组作为执行代价参照。

## 原始观测字段

当前主分析链路依赖以下四个原始字段：

| 字段 | 含义 | 用途 |
| --- | --- | --- |
| `start_clock` | block 在 SM 本地时钟域上的启动周期 | 用于计算同一 SM 内的接续关系与 `sched` |
| `start_ts` | block 的全局启动时间戳 | 用于跨 SM 对齐、总览排序和可视化 |
| `elapsed` | block 的执行时长，单位为 cycle | 用于构造 `end_clock`，并作为执行时长对照 |
| `sm` | block 所在 SM 编号 | 用于按 SM 重建 block replacement 过程 |

由此可以得到：

$$
end\_clock = start\_clock + elapsed
$$

`end_clock` 不作为单独输出指标，但它是识别 `Sched` 事件的中间量。

## 核心指标说明

本项目采用“`Sched` 为主、`load` 为辅、`elapsed` 对照”的指标体系。这里的字段 `sched` 对应论文中的 `Sched`。主分析目标不是描述 block 的绝对启动先后，而是统一刻画同一 SM 上 block replacement 过程中的 `Sched`。

### 1. Sched 主指标

对同一 kernel run 内、同一 SM 上按 `start_clock` 排序的 block 序列，维护当前活动 block 集合；若新 block 启动前存在某个已结束 block 满足 `end_clock <= start_clock`，则该 block 的 `Sched` 定义为：

$$
Sched_i = start\_clock_i - end\_clock_j
$$

其中 `j` 为当前可被替换、且结束时间最接近 `start_clock_i` 的 block。若不存在满足条件的前驱 block，则当前 block 不构成有效 `Sched` 事件，记：

$$
Sched_i = 0
$$

这个定义与简单的“相邻 block 时间差”不同，它本质上是对同一 SM 上 resident block 槽位接替过程的重建估计，更接近 Neutrino 论文附录 E 的 block dispatch simulation 思路。

基于 block 级 `sched`，项目进一步计算以下汇总指标：

| 指标 | 含义 |
| --- | --- |
| `sched_event_count` | 有效 `Sched` 事件数，即 `sched > 0` 的事件总数 |
| `sched_mean_cycles` | 所有有效 `Sched` 事件的均值 |
| `sched_p95_cycles` | 所有有效 `Sched` 事件的 P95 |
| `sched_max_cycles` | 所有有效 `Sched` 事件的最大值 |
| `sched_cycles_total` | 单个 SM 在一个 kernel run 内累计的 sched 总周期 |
| `sched_cycles_per_sm_mean` | workload-batch 级别按 SM 聚合后的平均 sched 总周期 |
| `sched_cycles_per_sm_p95` | workload-batch 级别按 SM 聚合后的 sched 总周期 P95 |
| `sched_event_ratio` | 有效 replacement 事件数占观测 block 总数的比例 |

其中，`sched_*_cycles` 用于看 `Sched` 的分布与规模，`sched_cycles_per_sm_*` 用于看每个 SM 上累计的 `Sched` 代价，`sched_event_ratio` 用于判断当前实验条件下 replacement 现象是否已经充分展开。

### 2. load 类指标

load 类指标用于解释不同 workload 的调度现象，但不替代 `Sched` 主指标。它们回答的问题是：不同 SM 上的 block 和执行时长分布是否均衡，以及这种均衡性是否可能影响 replacement 节奏。

| 指标 | 含义 |
| --- | --- |
| `block_imbalance_ratio` | SM 间 block 数不均衡程度，定义为 `(block_max - block_min) / block_mean` |
| `block_cv` | SM 间 block 数的变异系数，用于补充不均衡程度 |
| `elapsed_sum_cv` | 各 SM 上累计执行时长之和的变异系数 |
| `jain_block_fairness` | 基于各 SM block 数计算的 Jain 公平性指数，越接近 1 表示越均衡 |
| `sm_count` | 当前 workload-batch 组合下实际观测到的 SM 数量 |

这些指标主要承担解释作用，例如：某个 workload 的 `Sched` 很高时，可以结合 `block_imbalance_ratio`、`elapsed_sum_cv` 和 `jain_block_fairness` 判断其是否伴随明显的 SM 负载不均。

### 3. elapsed 类对照指标

elapsed 类指标用于区分“执行时间长”与“Sched 高”这两件事，避免把宏观执行代价误当成 `Sched` 本身。

| 指标 | 含义 |
| --- | --- |
| `work_cycles_total` | 单个 SM 在一个 kernel run 内所有 block 的累计执行周期 |
| `work_cycles_per_sm_mean` | workload-batch 级别按 SM 聚合后的平均执行周期 |
| `block_elapsed_mean_cycles` | block 级执行时长均值 |
| `block_elapsed_p95_cycles` | block 级执行时长 P95 |

这些指标的意义在于提供参照。某个 workload 即使 `elapsed` 很高，也不一定意味着 `Sched` 更高；反之，较高的 `Sched` 也可能出现在并不极端的 `elapsed` 条件下。

### 4. 风险排序与汇总指标

在仪表板和自动报告中，项目还会把多类指标汇总成 workload 级风险排序，用于快速定位“更值得优先分析的 workload”。该排序不是物理硬件指标，而是分析层的综合比较结果。

当前排序逻辑以以下四个维度做等权 rank-sum：

- `sched_p95_mean`
- `imbalance_mean`
- `elapsed_cv_mean`
- `jain_mean`（低者更差）

其用途是辅助摘要和看板展示，而不是替代 `Sched` 的物理定义。

## 指标边界与使用原则

为避免概念混淆，项目统一采用以下口径：

1. `Sched` 只描述同一 SM 内 block replacement 过程，不跨 SM 计算。
2. `Sched` 采用 GPU cycle 作为单位，不主动转换为 ns，以保持与原始 trace 一致。
3. `sched` 字段是从 `(start_clock, elapsed, sm)` 重建得到的估计指标，不是硬件直接提供的 scheduler counter。
4. `load` 类指标只用于解释现象，不作为调度延迟的替代量。
5. `elapsed` 类指标只作为执行代价对照，不应直接等价为调度等待。


## 项目结构

| 路径 | 说明 |
| --- | --- |
| `probe/` | Neutrino 探针定义。 |
| `workloads/` | workload 入口、CUDA 扩展与采集脚本。 |
| `analysis/` | trace 解析、指标计算、作图与辅助脚本。 |
| `dashboard/` | 只读本地展示层。 |
| `configs/` | 默认 pipeline 与场景配置。 |
| `traces/` | 原始 Neutrino trace 目录。 |
| `output/` | 结构化 CSV、指标表与静态图输出目录。 |
| `tests/` | analysis 层相关测试。 |
| `neutrino/` | 项目内置的 Neutrino 源码。 |

## 环境

当前仓库中的已有结果产物来自如下实验环境：

- GPU：NVIDIA GeForce RTX 4090
- OS：Ubuntu 22.04
- Driver：550.xx
- CUDA：12.2
- Python：3.10+
- PyTorch：与本机 CUDA 兼容的版本
- Neutrino：使用仓库内置源码或者下载官方代码

如需使用 `analysis/config_loader.py` 和 `analysis/scenario_runner.py`，建议使用 Python 3.11 及以上版本，以便直接使用标准库 `tomllib`。

## 安装与准备

安装分析脚本依赖：

```bash
python3 -m pip install --upgrade pip
python3 -m pip install pandas numpy matplotlib seaborn
```

安装与本机 CUDA 匹配的 PyTorch 版本后，再编译 workload 扩展。

安装仓库内置的 Neutrino：

```bash
cd neutrino
python3 setup.py install
cd ..
```

编译 workload 扩展：

```bash
cd workloads
python3 setup.py build_ext --inplace
cd ..
```

## 运行方式

### 按核心链路逐步执行

在项目根目录下：

```bash
bash workloads/collect_workloads.sh
python3 analysis/parse_to_csv.py
python3 analysis/recompute_sched_metrics.py
python3 analysis/analyze_and_plot.py
python3 dashboard/server.py
```

如需在采集阶段覆盖默认参数，可显式传入环境变量：

```bash
PYTHON=python3 \
BATCHES="16 32 64 128" \
WORKLOADS="compute memory mixed sparse vgg16" \
ITERS=8 \
ITERS_VGG16=1 \
bash workloads/collect_workloads.sh
```

### 使用包装脚本执行

`analysis/cli.py` 可以串联执行同一条流程：

```bash
python3 analysis/cli.py all --python python3 --batches 16 32 64 128 --workloads compute memory mixed sparse vgg16
```

也可以直接使用默认配置文件：

```bash
python3 analysis/cli.py all --config configs/pipeline.default.toml
```

批量场景矩阵可通过以下方式预览或执行：

```bash
python3 analysis/scenario_runner.py --matrix configs/scenarios.default.toml --python python3 --dry-run
```

## 输出内容

### 结构化数据

- `output/data/<workload>.csv`
  每类 workload 对应一份 block 级明细表。

当前主分析链路依赖的典型字段如下：

```text
workload, batch, block_id, sm, start_clock, start_ts, elapsed, sched
```

若个别中间结果仍包含 `launch_offset`，应将其视为时间对齐辅助量，而非主分析字段。

### 指标汇总

`output/chart/metrics/base/` 下的核心结果包括：

- `sched_summary_by_workload_batch.csv`
- `sched_detail_by_sm.csv`
- `sched_events.csv`
- `load_summary_by_workload_batch.csv`
- `load_per_sm.csv`
- `sched_block_detail.csv`
- `sched_block_summary_by_workload_batch.csv`

### 静态图表

- `output/chart/sched/`
  sched 趋势图、分布图、ECDF、heatmap 等。
- `output/chart/load/`
  负载不均衡、CV、Jain fairness 与 SM 分布相关图表。
- `output/chart/overview/`
  全局 elapsed 分布概览图。

### 本地 Dashboard

在 `output/` 生成完成后，可启动本地展示页面：

```bash
python3 dashboard/server.py --output-dir output --host 127.0.0.1 --port 8765
```

默认访问地址：

```text
http://127.0.0.1:8765
```

更多说明可见 `dashboard/README.md`。

## 辅助脚本

以下脚本不属于核心三段链路，但在批量实验、结果整理或附加分析时会用到：

- `analysis/validators.py`
  对解析结果做基础合法性检查。
- `analysis/reporting.py`
  输出结构化 JSON / Markdown 摘要。
- `analysis/metrics_guard.py`
  对不同指标表做基础一致性核对。
- `analysis/ablation.py`
  基于已有汇总结果做 workload exclusion 分析。
- `analysis/config_loader.py`
  为包装脚本读取配置文件。
- `analysis/scenario_runner.py`
  负责场景矩阵的批量执行。

## 测试

在项目根目录下运行：

```bash
python3 -m unittest discover -s tests
```
