package com.ucesys.sparkscope.metrics

import com.ucesys.sparkscope.SparkScopeAnalyzer._

case class ClusterMemoryStats(maxHeap: Long,
                              avgHeap: Long,
                              maxHeapPerc: Double,
                              avgHeapPerc: Double,
                              executorTimeSecs: Long,
                              heapGbHoursAllocated: Double,
                              heapGbHoursWasted: Double,
                              executorHeapSizeInGb: Double) {
  override def toString: String = {
    Seq(
      "\nCluster Memory stats: ",
      f"Average Cluster heap memory utilization: ${avgHeapPerc*100}%1.2f%% / ${avgHeap}MB",
      f"Max Cluster heap memory utilization: ${maxHeapPerc * 100}%1.2f%% / ${maxHeap}MB",
      f"heapGbHoursAllocated: ${this.heapGbHoursAllocated}%1.4f",
      s"heapGbHoursAllocated=(executorHeapSizeInGb(${this.executorHeapSizeInGb})*combinedExecutorUptimeInSec(${this.executorTimeSecs}s))/3600",
      f"heapGbHoursWasted: ${this.heapGbHoursWasted}%1.4f",
      f"heapGbHoursWasted=heapGbHoursAllocated(${this.heapGbHoursAllocated}%1.4f)*heapUtilization(${this.avgHeapPerc}%1.4f)\n"
    ).mkString("\n")
  }
}

object ClusterMemoryStats {
  def apply(clusterMetrics: ClusterMemoryMetrics, executorTimeSecs: Long, executorStats: ExecutorMemoryStats): ClusterMemoryStats = {
    val executorHeapSizeInGb = executorStats.heapSize.toDouble / 1024d
    val heapGbHoursAllocated = (executorHeapSizeInGb * executorTimeSecs) / 3600d

    ClusterMemoryStats(
      maxHeap = clusterMetrics.heapUsed.select(JvmHeapUsed).max.toLong / BytesInMB,
      avgHeap = clusterMetrics.heapUsed.select(JvmHeapUsed).avg.toLong / BytesInMB,
      maxHeapPerc = f"${clusterMetrics.heapUsage.select(JvmHeapUsage).max}%1.5f".toDouble,
      avgHeapPerc = f"${executorStats.avgHeapPerc}%1.5f".toDouble,
      executorTimeSecs = executorTimeSecs,
      heapGbHoursAllocated = f"${heapGbHoursAllocated}%1.5f".toDouble,
      heapGbHoursWasted = f"${heapGbHoursAllocated * executorStats.avgHeapPerc}%1.5f".toDouble,
      executorHeapSizeInGb = f"${executorHeapSizeInGb}%1.5f".toDouble
    )
  }
}

