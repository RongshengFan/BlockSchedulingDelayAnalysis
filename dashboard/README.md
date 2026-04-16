# Local Dashboard

这个目录是对现有分析结果的独立展示层，只读取 `output/`，不修改 pipeline、analysis 或 output 结构。

## 功能

- 读取现有 output 目录中的 CSV 与 JSON 指标文件。
- 提供本地网页，用于按 workload / batch 筛选并查看交互式图表。
- 页面以 sched 为主，load 为辅，补充 overview 与风险排序等视角。
- 支持 sched/load 的趋势图、分布图、SM heatmap、风险排序、结构化结论与联表视图。
- 支持展示 output/chart 下已有的静态 PNG 图，并在页面中集中浏览。

## 启动方式

依赖：

```bash
python3 -m pip install --user pandas
```

从项目根目录启动：

```bash
python3 dashboard/server.py --output-dir output --host 127.0.0.1 --port 8765
```

默认地址：

```text
http://127.0.0.1:8765
```

如果 output 目录不在默认位置，可显式指定：

```bash
python3 dashboard/server.py --output-dir /path/to/output --port 8877
```

如果需要让同一局域网内的其他机器访问，可显式监听所有网卡：

```bash
python3 dashboard/server.py --host 0.0.0.0 --port 8765
```

## 数据来源

- output/data/*.csv
- output/chart/metrics/base/*.csv
- output/chart/metrics/report/*.json
- output/chart/metrics/validation/*.json
- output/chart/overview/*.png
- output/chart/sched/*.png
- output/chart/load/*.png

## 页面内容

- 筛选 workload 与 batch 后查看 sched 主指标图。
- 同步查看 load imbalance、Jain fairness、risk scatter、distribution 和 heatmap。
- 查看 analysis_conclusion.json 的结构化结论，原始 JSON 默认折叠。
- 浏览 output/chart 中已有静态图，适合答辩或论文复核时快速查阅。

## 说明

- 这是零侵入新增目录，不会改变现有分析脚本行为。
- 前端不依赖外部 CDN，离线环境可直接打开使用。
- dashboard 只消费已有 output 结果，不负责重新采集 trace 或重跑分析。
