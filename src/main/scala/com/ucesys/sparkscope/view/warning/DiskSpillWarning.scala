package com.ucesys.sparkscope.view.warning

import com.ucesys.sparkscope.agg.TaskAggMetrics
import com.ucesys.sparkscope.common.MemorySize

case class DiskSpillWarning(diskBytesSpilled: MemorySize, memoryBytesSpilled: MemorySize) extends Warning {
    override def toString: String = {
        f"Data spills from memory to disk occured. Total size spilled to disk: ${diskBytesSpilled.toMB}MB(in-memory size: ${memoryBytesSpilled.toMB}MB)"
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