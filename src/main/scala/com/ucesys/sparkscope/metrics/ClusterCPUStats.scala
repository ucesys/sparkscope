package com.ucesys.sparkscope.metrics

import com.ucesys.sparkscope.SparkScopeAnalyzer._

case class ClusterCPUStats(cpuUtil: Double,
                           coreHoursAllocated: Double,
                           coreHoursWasted: Double,
                           executorTimeSecs: Long,
                           executorCores: Int) {
  override def toString: String = {
    Seq(
      "Cluster CPU stats: ",
      f"Total CPU utilization: ${cpuUtil*100}%1.2f%%",
      f"coreHoursAllocated: ${this.coreHoursAllocated}%1.4f",
      s"coreHoursAllocated=(executorCores(${this.executorCores})*combinedExecutorUptimeInSec(${this.executorTimeSecs}s))/3600",
      f"coreHoursWasted: ${this.coreHoursWasted}%1.4f",
      f"coreHoursWasted=coreHoursAllocated(${this.coreHoursAllocated}%1.4f)*cpuUtilization(${this.cpuUtil}%1.4f)"
    ).mkString("\n")
  }
}

object ClusterCPUStats {
  def apply(clusterCpuMetrics: ClusterCPUMetrics, executorCores: Int, executorTimeSecs: Long): ClusterCPUStats = {
    val cpuUtil: Double = clusterCpuMetrics.cpuTimePerExecutor.select(CpuTime).sum / (executorTimeSecs * executorCores * NanoSecondsInSec)
    val coreHoursAllocated = (executorCores * executorTimeSecs).toDouble / 3600d
    val coreHoursWasted = coreHoursAllocated * cpuUtil

    ClusterCPUStats(
      cpuUtil = f"${cpuUtil}%1.5f".toDouble,
      coreHoursAllocated =  f"${coreHoursAllocated}%1.5f".toDouble,
      coreHoursWasted =  f"${coreHoursWasted}%1.5f".toDouble,
      executorCores = executorCores,
      executorTimeSecs = executorTimeSecs
    )
  }
}

