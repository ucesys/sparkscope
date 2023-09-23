package com.ucesys.sparkscope.metrics

import com.ucesys.sparkscope.ExecutorMetricsAnalyzer._
import com.ucesys.sparkscope.data.DataFrame

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
  def apply(clusterCpuTime: DataFrame, executorCores: Int, executorTimeSecs: Long): ClusterCPUStats = {
    val cpuUtil: Double = clusterCpuTime.select(CpuTime).max / (executorTimeSecs * executorCores * NanoSecondsInSec)
    val coreHoursAllocated = (executorCores * executorTimeSecs).toDouble / 3600d
    val coreHoursWasted = coreHoursAllocated * cpuUtil

    ClusterCPUStats(
      cpuUtil = cpuUtil,
      executorCores = executorCores,
      executorTimeSecs = executorTimeSecs,
      coreHoursAllocated = coreHoursAllocated,
      coreHoursWasted = coreHoursWasted
    )
  }
}

