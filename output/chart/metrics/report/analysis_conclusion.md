# Scheduling Delay and Load Analysis Report

## 1) Metric Notes
- sched: sched denotes the Neutrino-style scheduling gap on the same SM: the actual start time of a newly dispatched block minus the actual end time of the previous block observed on that SM.
- launch_offset: launch_offset is a relative start offset measured from the earliest block start timestamp in the same traced kernel run. It is not the true ready-to-run wait time.
- core metric: Core scheduling summary is based on Neutrino-style dispatch-gap statistics. For blocks observed on the same SM, the sched gap is defined as the actual start time of the next block minus the actual end time of the previous block.

## 2) Validation
- rows: 626944
- passed: True
- errors: 0
- warnings: 0

## 3) Scheduling Findings
- [high] batch=16 highest dispatch gap p95 workload is mixed (p95=13378.2000)
- [high] batch=32 highest dispatch gap p95 workload is mixed (p95=28343.1500)
- [high] batch=64 highest dispatch gap p95 workload is mixed (p95=20722.2500)
- [high] batch=128 highest dispatch gap p95 workload is mixed (p95=16106.0000)
- [high] overall highest mean dispatch-gap p95 scheduling delay workload is mixed (value=19637.4000)
- [info] overall lowest mean dispatch-gap p95 scheduling delay workload is sparse (value=1035.5500)

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
- dispatch_gap_p95_cycles vs block_imbalance_ratio: pearson=-0.0883 spearman=-0.0836, very weak negative correlation
- dispatch_gap_p95_cycles vs elapsed_sum_cv: pearson=-0.1413 spearman=-0.0361, very weak negative correlation
- dispatch_gap_p95_cycles vs jain_block_fairness: pearson=0.0980 spearman=0.0241, very weak positive correlation
- sched_event_ratio vs block_imbalance_ratio: pearson=-0.8096 spearman=-0.7110, strong negative correlation
- sched_event_ratio vs elapsed_sum_cv: pearson=-0.8040 spearman=-0.7946, strong negative correlation
- sched_event_ratio vs jain_block_fairness: pearson=0.7653 spearman=0.7138, strong positive correlation

## 6) Workload Composite Ranking
- method: equal_weight_rank_sum
- score definition: overall_risk_score = rank_sched + rank_imbalance + rank_cv + rank_fairness_bad
- explanation: A rank-sum is used instead of directly combining raw values, because the four dimensions have different units and scales. Each dimension is equally weighted.
- excluded batches: 8

- memory: score=17.00, core_sched_p95_mean=2041.1000, imbalance_mean=0.2407, jain_mean=0.9965
- mixed: score=14.00, core_sched_p95_mean=19637.4000, imbalance_mean=0.1479, jain_mean=0.9970
- compute: score=13.00, core_sched_p95_mean=1122.1750, imbalance_mean=0.0938, jain_mean=0.9963
- sparse: score=8.00, core_sched_p95_mean=1035.5500, imbalance_mean=0.1562, jain_mean=0.9984
- vgg16: score=8.00, core_sched_p95_mean=7431.4000, imbalance_mean=0.0792, jain_mean=0.9994

## 7) Conclusion
- Highest scheduling risk workload: memory (score=17.00)
- Lowest scheduling risk workload: vgg16 (score=8.00)
