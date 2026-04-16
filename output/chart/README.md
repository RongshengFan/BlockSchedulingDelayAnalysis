# 图表目录说明与第四章写作指引

本文件面向两个使用场景：

1. 作为 `output/chart/` 目录的图表说明，说明当前图集的构成、指标口径与适用范围。
2. 作为论文第四章的配套写作指引，说明各组图表能够支撑的分析任务、适合落入的章节位置，以及可据以形成的主要结论。

本文档重点回答以下问题：

1. 当前图表目录由哪些子模块构成，各自承担什么分析任务。
2. 项目中涉及的核心指标有哪些，其物理含义和分析价值分别是什么。
3. 每张图表主要表达什么信息，适合支撑何种结论。
4. 在第四章中，应如何组织“总体调度表现”“批次影响”“负载均衡关联”“执行时长对照”四条分析主线。

## 一、目录概览

当前 `output/chart/` 目录下的图表可划分为四个子模块：

| 子目录 | 主题 | 对应章节 | 主要作用 |
| --- | --- | --- | --- |
| `sched/` | `Sched` 的均值、尾部、分布与分层 | `4.2`、`4.3` | 呈现调度表现，是第四章的主图组 |
| `load/` | SM 侧 block 分布、执行代价分布与负载均衡 | `4.2`、`4.4` | 用于解释 `Sched` 差异的可能来源 |
| `correlation/` | `Sched` 与负载均衡指标的对应关系 | `4.4` | 用于支撑“存在关联，但不宜直接推定强因果”的论证 |
| `overview/` | block 执行时长的整体分布 | `4.5` | 用于区分“执行时间长”与“调度等待大” |

建议的阅读顺序如下：

1. 先阅读 `sched/`，识别不同 workload 的 `Sched` 分层及其在 batch 维度上的变化。
2. 再结合 `load/` 与 `correlation/`，分析负载均衡特征是否能够解释调度差异。
3. 最后参考 `overview/`，将调度差异与执行时长差异区分开。

## 二、核心指标体系

### 1. `Sched` 主指标

对同一 kernel run、同一 SM 上的 block，若当前 block 接替了一个已经结束的 block 槽位，则该 block 的 `Sched` 定义为：

$$
\mathrm{Sched}_i = s_i - e_j
$$

若不存在满足接替条件的已结束前驱 block，则定义为：

$$
\mathrm{Sched}_i = 0
$$

其中， $s_i$ 表示当前 block 的 `start_clock`， $e_j$ 表示其被替换前驱 block 的 `end_clock`。`Sched` 对应论文中的核心调度指标，用于刻画同一 SM 上 block replacement 过程中产生的等待周期。该指标不是 host 侧 launch offset，也不是硬件直接暴露的 scheduler counter，而是根据 `(start_clock, elapsed, sm)` 重建得到的估计量。

与 `Sched` 相关的主要聚合指标如下：

| 指标 | 含义 | 主要用途 |
| --- | --- | --- |
| `sched_mean_cycles` | 有效 `Sched` 事件的均值 | 观察整体平均调度水平 |
| `sched_p95_cycles` | 有效 `Sched` 事件的 P95 | 观察尾部调度压力，是正文主指标 |
| `sched_max_cycles` | 有效 `Sched` 事件的最大值 | 观察极端尾部异常 |
| `sched_event_count` | 有效 `Sched` 事件数 | 观察 replacement 现象出现频次 |
| `sched_event_ratio` | 有效 `Sched` 事件数占 block 总数的比例 | 观察调度现象是否在当前 batch 下充分展开 |
| `sched_cycles_per_sm_mean` | 以 SM 为单位累计 `Sched` 周期后的均值 | 衡量每个 SM 上的平均累计调度代价 |
| `sched_cycles_per_sm_p95` | 以 SM 为单位累计 `Sched` 周期后的 P95 | 衡量高压力 SM 上的累计调度代价 |

在第四章中，建议将 `sched_p95_cycles` 作为主指标，`sched_mean_cycles` 用于补充整体水平，`sched_max_cycles` 仅用于说明极端尾部事件，`sched_event_ratio` 用于解释不同 batch 下调度现象的展开程度。

### 2. `load` 类指标

`load` 类指标不直接表示调度延迟，而是用于刻画 SM 级负载分布是否均衡。其主要分析问题包括：

1. SM 间 block 数量是否均衡。
2. SM 间累计执行代价是否均衡。
3. 均衡性变化是否可能影响 `Sched` 的观测结果。

核心指标定义如下：

$$
\mathrm{Imbalance} = \frac{B_{\max} - B_{\min}}{\overline{B}}
$$

$$
\mathrm{CV}_{\mathrm{elapsed}} = \frac{\sigma(x)}{\mu(x)}
$$

$$
\mathrm{Jain} = \frac{\left(\sum_{i=1}^{n} x_i\right)^2}{n \cdot \sum_{i=1}^{n} x_i^2}
$$

对应指标及用途如下：

| 指标 | 含义 | 主要用途 |
| --- | --- | --- |
| `block_imbalance_ratio` | SM 间 block 数分布的不均衡程度，数值越大越不均衡 | 观察 block 分配偏斜 |
| `block_cv` | SM 间 block 数的变异系数 | 补充刻画 block 分布离散程度 |
| `elapsed_sum_cv` | SM 间累计执行时长之和的变异系数 | 观察执行代价是否集中于少数 SM |
| `jain_block_fairness` | 基于 block 数计算的 Jain 公平性指数，越接近 1 越均衡 | 从公平性角度刻画均衡程度 |
| `sm_count` | 实际被观测到的 SM 数量 | 用于确认实验覆盖范围 |

在第四章中，`load` 类指标主要承担解释作用。它们可以支持“存在对应关系”或“存在解释线索”的论述，但不宜单独作为 `Sched` 的替代指标，也不宜据此直接推导强因果结论。

### 3. `elapsed` / 执行时长对照指标

`overview/` 图组围绕 block 执行时长展开，其作用在于区分“执行时间长”与“调度等待大”这两类现象。

| 指标 | 含义 | 主要用途 |
| --- | --- | --- |
| `block_elapsed_mean_cycles` | block 执行时长均值 | 观察平均执行代价 |
| `block_elapsed_p95_cycles` | block 执行时长 P95 | 观察执行时长尾部 |
| `work_cycles_per_sm_mean` | 每个 SM 上累计执行周期均值 | 观察 workload 自身执行负担 |

这些指标在正文中的意义主要体现在：

1. 若某 workload 的 `elapsed` 很高但 `Sched` 不高，则其主要特征更可能来自执行本身，而非调度等待。
2. 若某 workload 的 `Sched` 很高而 `elapsed` 并不极端，则其差异更可能来自 block 接续过程。

### 4. 综合排序指标

仪表板与自动报告中还使用了 workload 级综合排序，用于形成总览视角下的关注优先级。当前排序使用以下维度：

1. `sched_p95_mean`
2. `imbalance_mean`
3. `elapsed_cv_mean`
4. `jain_mean`（数值越低表示越差）

当前综合风险排序为：

1. `memory`
2. `mixed`
3. `compute`
4. `sparse`
5. `vgg16`

需要强调的是，该排序属于分析层综合摘要，而不是物理硬件指标。它适合用于 `4.2` 的总览部分，帮助说明“优先关注哪些 workload”，但不宜替代单项核心指标的分析。

## 三、各子目录分述

### 1. `sched/` 子目录

`sched/` 是第四章的主图来源，主要用于回答以下问题：

1. 不同 workload 的 `Sched` 谁高谁低。
2. 不同 workload 的差异主要体现为均值差异、尾部差异，还是分布形态差异。
3. 这种差异在 batch 维度上是否稳定。

| 图 | 内容 | 能支撑的结论 | 建议使用位置 |
| --- | --- | --- | --- |
| `01_sched_mean_by_batch.png` | 各 workload 的 `Sched Mean` 随 batch 变化 | 观察整体平均水平是否同步变化 | `4.3` 辅助图 |
| `02_sched_p95_by_batch.png` | 各 workload 的 `Sched P95` 随 batch 变化 | 识别尾部调度压力最高的 workload 及其 batch 响应 | `4.2`、`4.3` 主图 |
| `03_sched_event_ratio_by_batch.png` | 各 workload 的 `sched_event_ratio` 随 batch 变化 | 观察 replacement 事件覆盖是否充分展开 | `4.2`、`4.3` 辅助图 |
| `04_sched_max_by_batch.png` | 各 workload 的 `Sched Max` 随 batch 变化 | 观察极端尾部异常是否集中出现 | `4.3` 补充图 |
| `05_sched_p95_heatmap_workload_batch.png` | workload-batch 二维热力图 | 识别高值 workload-batch 区域与重点案例 | `4.2`、`4.4` 主图 |
| `06_sched_ecdf_by_workload.png` | 非零 `Sched` 的经验分布函数 | 区分整体右移与少量长尾抬升 | `4.3` 主图 |
| `07_sched_quantile_ladders_by_workload.png` | P50 / P95 / P99 随 batch 的变化 | 分析不同 workload 的分位扩张形态 | `4.3` 主图 |
| `08_sched_p95_grouped_bar_by_batch.png` | 同一 batch 下 workload 横向比较 `Sched P95` | 强化同批次内的分层对比 | `4.2`、`4.3` 备选图 |
| `09_sched_mean_grouped_bar_by_batch.png` | 同一 batch 下 workload 横向比较 `Sched Mean` | 强化均值层面的横向对比 | `4.3` 备选图 |

建议的图表组合如下：

1. `4.2` 可优先使用 `02 + 05`，必要时补充 `03`。
2. `4.3` 可优先使用 `02 + 06 + 07`，必要时以 `01` 或 `04` 作补充。

### 2. `load/` 子目录

`load/` 主要用于解释调度表现差异的可能来源，而不是用于证明“谁的 `Sched` 更高”。其关注点是 SM 级的资源分布、执行代价分布及均衡性差异。

| 图 | 内容 | 能支撑的结论 | 建议使用位置 |
| --- | --- | --- | --- |
| `01_block_imbalance_ratio_by_batch.png` | `block_imbalance_ratio` 随 batch 变化 | 观察 block 分配偏斜程度 | `4.2`、`4.4` |
| `02_elapsed_sum_cv_by_batch.png` | `elapsed_sum_cv` 随 batch 变化 | 观察执行代价分布是否不均 | `4.4` |
| `03_jain_fairness_by_batch.png` | `jain_block_fairness` 随 batch 变化 | 观察整体公平性差异 | `4.4` |
| `04_sm_block_heatmap_<workload>.png` | 特定 workload 的 SM block 热力图 | 提供 block 分布偏斜的局部证据 | `4.4` 局部图 |
| `05_sm_elapsed_heatmap_<workload>.png` | 特定 workload 的 SM elapsed 热力图 | 提供执行代价集中分布的局部证据 | `4.4` 局部图 |
| `06_block_imbalance_ratio_grouped_bar_by_batch.png` | 同一 batch 下 workload 的不均衡横向比较 | 强化同批次下的 workload 对比 | `4.4` 备选图 |
| `07_elapsed_sum_cv_grouped_bar_by_batch.png` | 同一 batch 下 workload 的执行代价偏斜比较 | 强化同批次下的 workload 对比 | `4.4` 备选图 |

使用时应遵循以下原则：

1. `01~03` 用于给出总体趋势。
2. `04~05` 用于提供局部证据，不宜直接承担全局排序任务。
3. `load` 图支撑的是解释性分析，而不是 `Sched` 主结论本身。

### 3. `correlation/` 子目录

`correlation/` 是 `4.4` 的关键补充。其作用在于将 `Sched` 与 `load` 指标放在同一张图中观察，从而避免仅依赖“两张图并列对照”的分析方式。

| 图 | 内容 | 能支撑的结论 | 建议使用位置 |
| --- | --- | --- | --- |
| `01_sched_p95_vs_load_metrics.png` | `Sched P95` 与三类 `load` 指标的三联散点图 | 观察 `Sched P95` 与负载均衡指标的总体关系及偏离点 | `4.4` 主图 |
| `02_sched_mean_vs_load_metrics.png` | `Sched Mean` 与三类 `load` 指标的关系 | 观察均值层面是否存在对应关系 | `4.4` 辅助图 |
| `03_sched_max_vs_load_metrics.png` | `Sched Max` 与三类 `load` 指标的关系 | 观察极端尾部异常是否伴随负载不均衡 | `4.4` 补充图 |
| `04_sched_event_ratio_vs_load_metrics.png` | `sched_event_ratio` 与三类 `load` 指标的关系 | 观察 replacement 事件覆盖度与负载分布的关系 | `4.4` 机制解释图 |

该组图的解释原则如下：

1. 散点图展示的是关联强弱与偏离样本，不宜直接解释为因果关系。
2. 三联子图共用同一纵轴，目的是便于横向比较哪一种 `load` 指标与当前 `Sched` 聚合量更为对应。
3. 趋势线表示总体趋势，不代表某个特定 workload。

### 4. `overview/` 子目录

`overview/` 用于回答一个关键问题：当前 workload 的主要特征究竟来自调度，还是来自执行。

| 图 | 内容 | 能支撑的结论 | 建议使用位置 |
| --- | --- | --- | --- |
| `01_elapsed_distribution_by_workload.png` | block 执行时长总体分布 | 识别哪些 workload 的 block 本身更长 | `4.5` 主图 |
| `02_elapsed_mean_grouped_bar_by_batch.png` | 各 batch 下 workload 的平均执行时长 | 观察执行代价随 batch 的变化 | `4.5` 辅助图 |

其使用方式为：

1. 先观察某 workload 的 `Sched` 是否较高。
2. 再观察其 `elapsed` 是否同步偏高。
3. 若两者不同步，则可更有把握地区分“调度差异”与“执行差异”。

## 四、总体调度表现与负载特征概览

本节对应第四章 `4.2`。其目标不是展开所有细节，而是先给出 workload 之间的总体分层、典型特征与关注重点。

### 1. 建议的写作顺序

建议按照以下顺序展开：

1. 说明五类 workload 在 `Sched P95` 上存在稳定分层。
2. 说明这种分层在不同 batch 下是否持续存在。
3. 补充综合风险排序，指出仅看 `Sched` 与综合考虑 `load` 后的关注优先级并不完全一致。
4. 对五类 workload 的典型特征进行概括。

推荐主图组合：

1. `sched/02_sched_p95_by_batch.png`
2. `sched/05_sched_p95_heatmap_workload_batch.png`
3. `sched/03_sched_event_ratio_by_batch.png`

若本节仅保留两张图，则建议优先使用：

1. `sched/02_sched_p95_by_batch.png`
2. `sched/05_sched_p95_heatmap_workload_batch.png`

### 2. 五类 workload 的概括方式

结合当前结果，可作如下归纳：

- `mixed`：`Sched` 尾部最重，是当前数据中最典型的高 `Sched` workload。其在 `batch=16/32/64/128` 上均给出了最高的 `Sched P95`，说明高延迟特征具有较强稳定性。
- `vgg16`：`Sched` 同样偏高，但负载均衡较好。这说明较高的 `Sched` 不一定总是伴随显著的 SM 不均衡，卷积类 workload 的 block 接续模式可能同样重要。
- `memory`：`Sched P95` 并非最高，但负载不均衡最明显，综合风险最高。其更适合作为“负载偏斜显著”的代表性 workload。
- `compute`：整体 `Sched` 较低，但执行代价较高，更接近“执行重、调度相对轻”的 workload。
- `sparse`：整体 `Sched` 最低，可作为低调度延迟 workload 的参考基线。

### 3. 本节可支撑的主要结论

本节适合支撑以下结论：

1. 不同 workload 的 `Sched` 表现存在稳定分层。
2. 高 `Sched` workload 具有不同类型，有的以尾部极重为特征，有的则在均衡性较好条件下仍表现出较高 `Sched`。
3. 单独考察 `Sched` 与综合考察 `Sched + load`，得到的关注优先级并不完全一致。

## 五、批次大小对 `Sched` 的影响分析

本节对应第四章 `4.3`。分析重点在于 batch 增大后，`Sched` 现象如何展开，以及 workload 间差异如何被放大。

### 1. 建议的写作顺序

建议按如下逻辑展开：

1. 说明 batch 增大后，`sched_event_ratio` 快速上升，表明 `Sched` 现象逐步充分展开。
2. 说明 `Sched P95` 如何随 batch 变化，以及不同 workload 的变化幅度是否一致。
3. 区分“batch 使整体更差”与“batch 放大 workload 间差异”这两个层面。

推荐图表：

1. `sched/03_sched_event_ratio_by_batch.png`
2. `sched/02_sched_p95_by_batch.png`
3. `sched/07_sched_quantile_ladders_by_workload.png`
4. `sched/06_sched_ecdf_by_workload.png`

### 2. 当前结果下的主要观察

当前结果支持以下观察：

1. 从 `batch=16` 到 `batch=64/128`，五类 workload 的 `sched_event_ratio` 均由约 `0.23~0.25` 提升至 `0.77~0.91` 左右，说明小 batch 下 replacement 现象尚未充分展开，而较大 batch 更适合作为正文主分析区间。
2. `mixed` 的 `Sched P95` 在所有主 batch 上均居于最高位置，并在 `batch=32` 达到最突出峰值，表明 batch 放大了其调度尾部特征，而非仅带来线性增长。
3. `compute` 与 `sparse` 虽然也会随着 batch 增大出现更多 `Sched` 事件，但其 `Sched P95` 仍维持在较低水平，说明 batch 增大不会自动将所有 workload 推入同等高延迟区间。
4. `vgg16` 呈现出非简单单调的高延迟特征，说明 batch 与 workload 结构具有共同作用。

### 3. 本节可支撑的主要结论

本节适合形成以下结论：

1. batch 增大会使 `Sched` 现象更充分出现。
2. batch 增大的影响不仅表现为整体抬升，更表现为 workload 间差异的放大。
3. 不同 workload 对 batch 的响应方式不同，因此不宜将 batch 效应简单表述为统一的单调上升。

## 六、负载均衡与 `Sched` 的关联分析

本节对应第四章 `4.4`。该部分最重要的写作原则是：讨论关联与解释，而非直接论证强因果关系。

### 1. 建议的写作顺序

推荐按照以下路径展开：

1. 先用 `correlation/01_sched_p95_vs_load_metrics.png` 观察总体关系。
2. 再结合 `sched/05_sched_p95_heatmap_workload_batch.png` 锁定高 `Sched` 场景。
3. 再回到 `load/01~03`，观察这些场景是否同时表现出更差的负载均衡。
4. 最后用 `load/04~05` 提供典型 workload 的局部证据。

推荐图表组合：

1. `correlation/01_sched_p95_vs_load_metrics.png`
2. `sched/05_sched_p95_heatmap_workload_batch.png`
3. `load/01_block_imbalance_ratio_by_batch.png`
4. `load/02_elapsed_sum_cv_by_batch.png`
5. `load/03_jain_fairness_by_batch.png`
6. 典型 workload 的 `load/04` 与 `load/05`

### 2. 当前结果下的主要观察

当前数据呈现出以下特征：

1. `Sched P95` 与三类负载均衡指标的整体相关性均较弱：与 `block_imbalance_ratio` 的 Pearson 约为 `-0.09`，与 `elapsed_sum_cv` 约为 `-0.14`，与 `jain_block_fairness` 约为 `0.10`。这说明仅依赖负载均衡指标，尚不足以充分解释 workload 间的 `Sched P95` 差异。
2. `sched_event_ratio` 与 `load` 指标之间的关系显著更强：其与 `block_imbalance_ratio`、`elapsed_sum_cv` 呈强负相关，与 `jain_block_fairness` 呈强正相关。这表明负载均衡对“replacement 现象是否充分展开”的影响更明显，而对尾部 `Sched P95` 的直接解释力度相对有限。
3. `memory` 是典型的高不均衡 workload，但并非 `Sched P95` 最高的 workload，说明“高不均衡”不必然对应“最高尾部调度延迟”。
4. `vgg16` 的 `Sched` 偏高而公平性仍较好，说明高 `Sched` 不一定依赖显著的负载不均衡，workload 自身的 block 接续模式同样可能是重要因素。

### 3. 本节可支撑的主要结论

本节适合形成以下结论：

1. 负载均衡指标与 `Sched` 存在一定关联，但这种关联不足以单独解释 `Sched P95`。
2. 负载均衡更像是影响调度现象展开程度的条件，而不是尾部 `Sched` 的唯一决定因素。
3. 高 `Sched` workload 中既包括“负载偏斜显著”的类型，也包括“负载均衡较好但 `Sched` 仍偏高”的类型，因此 `Sched` 保留了独立于 `load` 的调度信息。

## 七、执行时长对照分析

本节对应第四章 `4.5`。其核心任务是区分“执行时间长”与“调度等待大”。

建议的使用方式如下：

1. 先根据 `sched/` 图组确定哪些 workload 具有较高 `Sched`。
2. 再使用 `overview/01~02` 检查其执行时长是否同样显著偏高。
3. 若两者不一致，则可更明确地指出：该 workload 的主要差异来自调度过程，而非执行时间本身。

推荐图表：

1. `overview/01_elapsed_distribution_by_workload.png`
2. `overview/02_elapsed_mean_grouped_bar_by_batch.png`

## 八、总结

当前 `output/chart/` 目录下的图集已经能够完整支撑第四章主线，无需为论文正文额外设计新的图型体系。建议采用如下映射关系：

- `4.2 总体调度表现与负载特征概览`
  使用 `sched/02`、`sched/05`，必要时补充 `sched/03`
- `4.3 批次大小对调度延迟的影响分析`
  使用 `sched/02`、`sched/03`、`sched/06`、`sched/07`
- `4.4 负载均衡与调度延迟的关联分析`
  使用 `correlation/01` 作为主图，`load/01~03` 作为解释图，`load/04~05` 作为局部证据
- `4.5 与执行时长的区分`
  使用 `overview/01~02`

若需要将本章的核心判断浓缩为一句话，则可概括为：

> 不同 workload 的 `Sched` 表现存在稳定分层；批次大小会显著放大这种分层；负载均衡能够解释其中一部分现象，但不足以单独解释尾部 `Sched` 差异，因此 `Sched` 仍然是独立且必要的调度分析指标。
