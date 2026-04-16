"""Blocked-sched probe for queue-delay oriented analysis.

说明：
- 不修改原 probe；本文件用于新的 blocked_sched 数据链路。
- 记录字段：
  1) start_clock: SM 本地 clock64
  2) start_time:  globaltimer（跨 SM 更可比）
  3) elapsed:     执行时长（clock64 域）
  4) sm:          SM ID
"""
from neutrino import probe, Map
import neutrino.language as nl

@Map(level="warp", type="array", size=32, cap=1)
class BlockedSched:
    start_clock: nl.u64
    start_time: nl.u64
    elapsed: nl.u64
    sm: nl.u64

start_clock: nl.u64 = 0
start_time: nl.u64 = 0
elapsed: nl.u64 = 0

@probe(pos="kernel", level="warp", before=True)
def entry():
    start_clock = nl.clock()
    start_time = nl.time()

@probe(pos="kernel", level="warp")
def exit():
    elapsed = nl.clock() - start_clock
    BlockedSched.save(start_clock, start_time, elapsed, nl.cuid())
