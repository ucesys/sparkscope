package com.ucesys.sparkscope.stats

import com.ucesys.sparkscope.SparkScopeAnalyzer._
import com.ucesys.sparkscope.common.CpuTime
import com.ucesys.sparkscope.metrics.ClusterCPUMetrics

case class ClusterCPUStats(cpuUtil: Double,
                           cpuNotUtil: Double,
                           coreHoursAllocated: Double,
                           coreHoursWasted: Double,
                           executorTimeSecs: Long,
                           executorCores: Int) {
    override def toString: String = {
        Seq(
            "Cluster CPU stats: ",
            f"Total CPU utilized: ${cpuUtil * 100}%1.2f%%",
            f"Total CPU not utilized: ${cpuNotUtil * 100}%1.2f%%",
            f"coreHoursAllocated: ${this.coreHoursAllocated}%1.4f",
            s"coreHoursAllocated=(executorCores(${this.executorCores})*combinedExecutorUptimeInSec(${this.executorTimeSecs}s))/3600",
            f"coreHoursWasted: ${this.coreHoursWasted}%1.4f",
            f"coreHoursWasted=coreHoursAllocated(${this.coreHoursAllocated}%1.4f)*cpuNotUtilized(${this.cpuNotUtil}%1.4f)"
        ).mkString("\n")
    }
}

object ClusterCPUStats {
    def apply(clusterCpuMetrics: ClusterCPUMetrics, executorCores: Int, executorTimeSecs: Long): ClusterCPUStats = {
        val cpuUtil: Double = (clusterCpuMetrics.cpuTimePerExecutor.select(CpuTime.name).sum / (executorTimeSecs * executorCores * NanoSecondsInSec)).min(1.0d)
        val cpuNotUtil: Double = 1 - cpuUtil
        val coreHoursAllocated = (executorCores * executorTimeSecs).toDouble / 3600d
        val coreHoursWasted = coreHoursAllocated * cpuNotUtil

        ClusterCPUStats(
            cpuUtil = f"${cpuUtil}%1.5f".toDouble,
            cpuNotUtil = f"${cpuNotUtil}%1.5f".toDouble,
            coreHoursAllocated = f"${coreHoursAllocated}%1.5f".toDouble,
            coreHoursWasted = f"${coreHoursWasted}%1.5f".toDouble,
            executorCores = executorCores,
            executorTimeSecs = executorTimeSecs
        )
    }
}

