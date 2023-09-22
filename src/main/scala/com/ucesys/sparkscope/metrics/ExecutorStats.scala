package com.ucesys.sparkscope.metrics

case class ExecutorStats(maxHeap: Long, maxHeapPerc: Double, avgHeap: Long, avgHeapPerc: Double, avgNonHeap: Long, maxNonHeap: Long) {
  override def toString: String = {
    Seq(
      s"\nExecutor stats:",
      f"Max heap memory utilization by executor: ${maxHeap}MB(${maxHeapPerc}%1.2f%%)",
      f"Average heap memory utilization by executor: ${avgHeap}MB(${avgHeapPerc}%1.2f%%)",
      s"Max non-heap memory utilization by executor: ${maxNonHeap}MB",
      f"Average non-heap memory utilization by executor: ${avgNonHeap}MB"
    ).mkString("\n")

  }
}

