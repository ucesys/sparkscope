package com.ucesys.sparkscope.metrics

import com.ucesys.sparkscope.SparkScopeAnalyzer._
import com.ucesys.sparkscope.data.DataFrame

case class ExecutorMemoryStats(heapSize: Long, maxHeap: Long, maxHeapPerc: Double, avgHeap: Long, avgHeapPerc: Double, avgNonHeap: Long, maxNonHeap: Long) {
  override def toString: String = {
    Seq(
      s"\nExecutor stats:",
      f"Executor heap size: ${heapSize}MB",
      f"Max heap memory utilization by executor: ${maxHeap}MB(${maxHeapPerc}%1.2f%%)",
      f"Average heap memory utilization by executor: ${avgHeap}MB(${avgHeapPerc}%1.2f%%)",
      s"Max non-heap memory utilization by executor: ${maxNonHeap}MB",
      f"Average non-heap memory utilization by executor: ${avgNonHeap}MB"
    ).mkString("\n")

  }
}

object ExecutorMemoryStats {
  def apply(allExecutorsMetrics: DataFrame): ExecutorMemoryStats = {
    ExecutorMemoryStats(
      heapSize = allExecutorsMetrics.select(JvmHeapMax).max.toLong / BytesInMB,
      maxHeap = allExecutorsMetrics.select(JvmHeapUsed).max.toLong / BytesInMB,
      maxHeapPerc = allExecutorsMetrics.select(JvmHeapUsage).max * 100,
      maxNonHeap = allExecutorsMetrics.select(JvmNonHeapUsed).max.toLong / BytesInMB,
      avgHeap = allExecutorsMetrics.select(JvmHeapUsed).avg.toLong / BytesInMB,
      avgHeapPerc = allExecutorsMetrics.select(JvmHeapUsage).avg,
      avgNonHeap = allExecutorsMetrics.select(JvmNonHeapUsed).avg.toLong / BytesInMB
    )
  }
}

