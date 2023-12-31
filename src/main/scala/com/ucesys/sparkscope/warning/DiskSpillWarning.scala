package com.ucesys.sparkscope.warning

import com.ucesys.sparkscope.agg.TaskAggMetrics
import com.ucesys.sparkscope.common.MemorySize

class DiskSpillWarning private(diskBytesSpilled: MemorySize, memoryBytesSpilled: MemorySize) extends Warning {
    override def toString: String = {
        f"Tasks are spilling data from memory to disk. Total size spilled to disk: ${diskBytesSpilled.toMB}MB(in-memory size: ${memoryBytesSpilled.toMB}MB)"
    }
}

object DiskSpillWarning {
    def apply(taskAggMetrics: TaskAggMetrics): Option[DiskSpillWarning] = {
        if(taskAggMetrics.diskBytesSpilled.sum > 0L || taskAggMetrics.memoryBytesSpilled.sum > 0L) {
            Some(new DiskSpillWarning(MemorySize(taskAggMetrics.diskBytesSpilled.sum), MemorySize(taskAggMetrics.memoryBytesSpilled.sum)))
        } else {
            None
        }
    }
}