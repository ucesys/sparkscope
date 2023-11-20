package com.ucesys.sparkscope.metrics

import com.ucesys.sparkscope.SparkScopeAnalyzer.{CpuTime, CpuUsage}
import com.ucesys.sparkscope.data.DataTable

case class ClusterCPUMetrics(clusterCpuTime: DataTable,
                             clusterCpuUsage: DataTable,
                             clusterCpuUsageSum: DataTable,
                             clusterCapacity: DataTable,
                             cpuTimePerExecutor: DataTable,
                             numExecutors: DataTable) {
    override def toString: String = {
        Seq(
            s"\nCluster metrics:",
            clusterCpuTime.toString,
            clusterCpuUsage.toString,
            clusterCpuUsageSum.toString,
            clusterCapacity.toString,
            cpuTimePerExecutor.toString,
            numExecutors.toString,
        ).mkString("\n")
    }
}

object ClusterCPUMetrics {
    def apply(allExecutorsMetrics: DataTable, executorCores: Int): ClusterCPUMetrics = {
        val clusterCpuTimeDf = allExecutorsMetrics.groupBy("t", CpuTime).sum.sortBy("t")
        val groupedTimeCol = clusterCpuTimeDf.select("t")

        val clusterCpuUsageCol = allExecutorsMetrics
          .groupBy("t", CpuUsage)
          .avg
          .sortBy("t")
          .select(CpuUsage)
          .lt(1.0d)
          .rename(CpuUsage)
        val clusterCpuUsageDf = DataTable(name = "clusterCpuUsage", columns = Seq(groupedTimeCol, clusterCpuUsageCol))

        val numExecutors = allExecutorsMetrics.groupBy("t", CpuUsage).count.sortBy("t").select(Seq("t", "cnt"))
        val clusterCapacityCol = numExecutors.select("cnt").mul(executorCores)
        val clusterCapacityDf = DataTable(name = "clusterCapacity", columns = Seq(groupedTimeCol, clusterCapacityCol.rename("totalCores")))

        val clusterCpuUsageSumCol = allExecutorsMetrics.groupBy("t", "cpuUsageAllCores")
          .sum
          .sortBy("t")
          .select("cpuUsageAllCores")
          .min(clusterCapacityCol)
          .rename("cpuUsageAllCores")
        val clusterCpuUsageSumDf = DataTable(name = "cpuUsageAllCores", columns = Seq(groupedTimeCol, clusterCpuUsageSumCol))

        ClusterCPUMetrics(
            clusterCpuTime = clusterCpuTimeDf,
            clusterCpuUsage = clusterCpuUsageDf,
            clusterCpuUsageSum = clusterCpuUsageSumDf,
            clusterCapacity = clusterCapacityDf,
            cpuTimePerExecutor = allExecutorsMetrics.groupBy("executorId", CpuTime).max,
            numExecutors = numExecutors
        )
    }
}
