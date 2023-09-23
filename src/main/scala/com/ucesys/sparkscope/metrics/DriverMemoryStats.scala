package com.ucesys.sparkscope.metrics

import com.ucesys.sparkscope.SparkScopeAnalyzer._
import com.ucesys.sparkscope.data.DataFrame

case class DriverMemoryStats(heapSize: Long, maxHeap: Long, maxHeapPerc: Double, avgHeap: Long, avgHeapPerc: Double, avgNonHeap: Long, maxNonHeap: Long) {
  override def toString: String = {
    Seq(
      s"\nDriver stats:",
      s"Driver heap size: ${heapSize}",
      f"Max heap memory utilization by driver: ${maxHeap}MB(${maxHeapPerc}%1.2f%%)",
      f"Average heap memory utilization by driver: ${avgHeap}MB(${avgHeapPerc}%1.2f%%)",
      s"Max non-heap memory utilization by driver: ${maxNonHeap}MB",
      f"Average non-heap memory utilization by driver: ${avgNonHeap}MB"
    ).mkString("\n")
  }
}

object DriverMemoryStats {
  def apply(driverMetricsMerged: DataFrame): DriverMemoryStats = {
    DriverMemoryStats(
      heapSize = driverMetricsMerged.select(JvmHeapMax).max.toLong / BytesInMB,
      maxHeap = driverMetricsMerged.select(JvmHeapUsed).max.toLong / BytesInMB,
      maxHeapPerc = driverMetricsMerged.select(JvmHeapUsage).max * 100,
      avgHeap = driverMetricsMerged.select(JvmHeapUsed).avg.toLong / BytesInMB,
      avgHeapPerc = driverMetricsMerged.select(JvmHeapUsage).avg * 100,
      avgNonHeap = driverMetricsMerged.select(JvmNonHeapUsed).avg.toLong / BytesInMB,
      maxNonHeap = driverMetricsMerged.select(JvmNonHeapUsed).max.toLong / BytesInMB
    )
  }
}

