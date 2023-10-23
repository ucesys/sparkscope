package com.ucesys.sparkscope.metrics

import com.ucesys.sparkscope.SparkScopeAnalyzer.{CpuTime, CpuUsage}
import com.ucesys.sparkscope.data.DataFrame

case class ClusterCPUMetrics(clusterCpuTime: DataFrame,
                             clusterCpuUsage: DataFrame,
                             clusterCpuUsageSum: DataFrame,
                             clusterCapacity: DataFrame,
                             cpuTimePerExecutor: DataFrame) {
  override def toString: String = {
    Seq(
      s"\nCluster metrics:",
      clusterCpuTime.toString,
      clusterCpuUsage.toString,
      clusterCpuUsageSum.toString,
      clusterCapacity.toString,
      cpuTimePerExecutor.toString
    ).mkString("\n")
  }
}

object ClusterCPUMetrics {
    def apply(allExecutorsMetrics: DataFrame, executorCores: Int): ClusterCPUMetrics = {
        val clusterCpuTimeDf = allExecutorsMetrics.groupBy("t", CpuTime).sum.sortBy("t")
        val groupedTimeCol = clusterCpuTimeDf.select("t")

        val clusterCpuUsageCol = allExecutorsMetrics
            .groupBy("t", CpuUsage)
            .avg
            .sortBy("t")
            .select(CpuUsage)
            .lt(1.0d)
            .rename(CpuUsage)
        val clusterCpuUsageDf=DataFrame(name = "clusterCpuUsage", columns = Seq(groupedTimeCol, clusterCpuUsageCol))

        val clusterCapacityCol = allExecutorsMetrics.groupBy("t", CpuUsage).count.sortBy("t").select("cnt").mul(executorCores)
        val clusterCapacityDf = DataFrame( name = "clusterCapacity", columns = Seq(groupedTimeCol, clusterCapacityCol.rename("totalCores")))

        val clusterCpuUsageSumCol = allExecutorsMetrics.groupBy("t", "cpuUsageAllCores")
            .sum
            .sortBy("t")
            .select("cpuUsageAllCores")
            .min(clusterCapacityCol)
            .rename("cpuUsageAllCores")
        val clusterCpuUsageSumDf = DataFrame(name = "cpuUsageAllCores", columns = Seq(groupedTimeCol, clusterCpuUsageSumCol))

        ClusterCPUMetrics(
            clusterCpuTime=clusterCpuTimeDf,
            clusterCpuUsage=clusterCpuUsageDf,
            clusterCpuUsageSum=clusterCpuUsageSumDf,
            clusterCapacity=clusterCapacityDf,
            cpuTimePerExecutor=allExecutorsMetrics.groupBy("executorId", CpuTime).max
        )
    }
}
