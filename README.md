# BlockSchedulingDelayAnalysis

这是一个基于 Neutrino 的 GPU Block 调度分析项目。项目围绕 block 级 trace 采集、结构化解析、指标计算与结果展示组织，目标是把原始 trace 转换为可直接用于分析的 CSV、统计表、静态图和本地 Dashboard。

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
   将原始二进制 trace 转换为结构化表，并计算调度与负载均衡指标。
3. `结果展示`
   输出指标表、静态图，并通过本地 Dashboard 进行浏览。

`cli`、`scenario_runner`、`validators`、`reporting` 等脚本属于辅助层，用于批量执行、结果整理或额外检查，不构成主分析链路。

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
  基于同一 SM 上相邻 block 的开始/结束关系，重算 Neutrino 风格的 `sched` / dispatch-gap 指标。
- `analysis/analyze_and_plot.py`
  汇总 sched、load、elapsed 等指标，并输出统计表与静态图。

### 3. 结果展示

- `output/`
  保存结构化数据、指标汇总结果与图表产物。
- `dashboard/server.py`
  提供只读本地网页，用于查看 `output/` 下已生成的分析结果。

## 核心指标

当前主分析路径以 `sched` 与负载均衡指标为核心。

| 指标 | 含义 |
| --- | --- |
| `sched` | 同一 SM 上，下一个 block 的实际开始时间减去上一个 block 的实际结束时间。 |
| `dispatch_gap_p95_cycles` | workload-batch 级别非零调度间隙事件的 P95。 |
| `dispatch_gap_event_ratio` | 调度间隙事件数占观测 block 总数的比例。 |
| `block_imbalance_ratio` | SM 间 block 数分布的不均衡程度，定义为 `(block_max - block_min) / block_mean`。 |
| `elapsed_sum_cv` | 各 SM 上累计执行时长之和的变异系数。 |
| `jain_block_fairness` | 基于各 SM block 数计算的 Jain 公平性指数。 |


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
- Neutrino：使用仓库内置源码

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
BATCHES="8 16 32 64 128" \
WORKLOADS="compute memory mixed sparse vgg16" \
ITERS=8 \
ITERS_VGG16=1 \
bash workloads/collect_workloads.sh
```

### 使用包装脚本执行

`analysis/cli.py` 可以串联执行同一条流程：

```bash
python3 analysis/cli.py all --python python3 --batches 8 16 32 64 --workloads compute memory mixed sparse vgg16
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

典型字段如下：

```text
workload, batch, block_id, sm, launch_anchor_ts, start_ts, launch_offset, elapsed, sched
```

### 指标汇总

`output/chart/metrics/base/` 下的核心结果包括：

- `sched_summary_by_workload_batch.csv`
- `sched_detail_by_sm.csv`
- `dispatch_gap_events.csv`
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
