# Sched and Load Analysis Report

## 1) Metric Notes
- sched: Sched is the core metric reconstructed from (start_clock, elapsed, sm). Within the same SM, if a block replaces the most recent finished block slot, its Sched value is start_clock_i - end_clock_j; otherwise Sched is 0.
- sched metric: Core Sched summary follows the Neutrino-style same-SM block replacement reconstruction. For a block observed on an SM, Sched is computed from the block start clock and the end clock of the most recent finished block that it replaces on that SM.

## 2) Validation
- rows: 626944
- passed: True
- errors: 0
- warnings: 0

## 3) Sched Findings
- [high] batch=16 highest Sched P95 workload is mixed (p95=13378.2000)
- [high] batch=32 highest Sched P95 workload is mixed (p95=28343.1500)
- [high] batch=64 highest Sched P95 workload is mixed (p95=20722.2500)
- [high] batch=128 highest Sched P95 workload is mixed (p95=16106.0000)
- [high] overall highest mean Sched P95 workload is mixed (value=19637.4000)
- [info] overall lowest mean Sched P95 workload is sparse (value=1035.5500)

## 4) Load Findings
- [high] highest average block imbalance ratio workload is memory (value=0.2407)
- [info] lowest average block imbalance ratio workload is vgg16 (value=0.0792)
- [high] lowest average Jain fairness workload is compute (value=0.9963)
- [medium] batch=16 largest imbalance observed on memory (ratio=0.4844)
- [medium] batch=32 largest imbalance observed on memory (ratio=0.2188)
- [medium] batch=64 largest imbalance observed on memory (ratio=0.1484)
- [medium] batch=128 largest imbalance observed on memory (ratio=0.1113)

## 5) Correlation Summary
- merged workload-batch rows: 20
- Sched P95 vs block_imbalance_ratio: pearson=-0.0883 spearman=-0.0836, very weak negative correlation
- Sched P95 vs elapsed_sum_cv: pearson=-0.1413 spearman=-0.0361, very weak negative correlation
- Sched P95 vs jain_block_fairness: pearson=0.0980 spearman=0.0241, very weak positive correlation
- sched_event_ratio vs block_imbalance_ratio: pearson=-0.8096 spearman=-0.7110, strong negative correlation
- sched_event_ratio vs elapsed_sum_cv: pearson=-0.8040 spearman=-0.7946, strong negative correlation
- sched_event_ratio vs jain_block_fairness: pearson=0.7653 spearman=0.7138, strong positive correlation

## 6) Workload Composite Ranking
- method: equal_weight_rank_sum
- score definition: overall_risk_score = rank_sched + rank_imbalance + rank_cv + rank_fairness_bad
- explanation: A rank-sum is used instead of directly combining raw values, because the four dimensions have different units and scales. Each dimension is equally weighted.
- excluded batches: 8

- memory: score=17.00, sched_p95_mean=2041.1000, imbalance_mean=0.2407, jain_mean=0.9965
- mixed: score=14.00, sched_p95_mean=19637.4000, imbalance_mean=0.1479, jain_mean=0.9970
- compute: score=13.00, sched_p95_mean=1122.1750, imbalance_mean=0.0938, jain_mean=0.9963
- sparse: score=8.00, sched_p95_mean=1035.5500, imbalance_mean=0.1562, jain_mean=0.9984
- vgg16: score=8.00, sched_p95_mean=7431.4000, imbalance_mean=0.0792, jain_mean=0.9994

## 7) Conclusion
- Highest Sched risk workload: memory (score=17.00)
- Lowest Sched risk workload: vgg16 (score=8.00)
