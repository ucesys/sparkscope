package com.ucesys.sparkscope.metrics

import com.ucesys.sparkscope.SparkScopeAnalyzer._
import com.ucesys.sparkscope.data.DataFrame

case class DriverMemoryStats(heapSize: Long, maxHeap: Long, maxHeapPerc: Double, avgHeap: Long, avgHeapPerc: Double, avgNonHeap: Long, maxNonHeap: Long) {
  override def toString: String = {
    Seq(
      s"Driver stats:",
      s"Driver heap size: ${heapSize}",
      f"Max heap memory utilization by driver: ${maxHeap}MB(${maxHeapPerc * 100}%1.2f%%)",
      f"Average heap memory utilization by driver: ${avgHeap}MB(${avgHeapPerc * 100}%1.2f%%)",
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
      maxHeapPerc = f"${driverMetricsMerged.select(JvmHeapUsage).max}%1.5f".toDouble,
      avgHeap = driverMetricsMerged.select(JvmHeapUsed).avg.toLong / BytesInMB,
      avgHeapPerc = f"${driverMetricsMerged.select(JvmHeapUsage).avg}%1.5f".toDouble,
      avgNonHeap = driverMetricsMerged.select(JvmNonHeapUsed).avg.toLong / BytesInMB,
      maxNonHeap = driverMetricsMerged.select(JvmNonHeapUsed).max.toLong / BytesInMB
    )
  }
}

