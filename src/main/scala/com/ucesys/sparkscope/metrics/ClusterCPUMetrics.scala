package com.ucesys.sparkscope.metrics

import com.ucesys.sparkscope.common.CpuTime
import com.ucesys.sparkscope.common.MetricUtils.ColCpuUsage
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
            numExecutors.toString
        ).mkString("\n")
    }
}

object ClusterCPUMetrics {
    def apply(allExecutorsMetrics: DataTable, executorCores: Int): ClusterCPUMetrics = {
        val clusterCpuTimeDf = allExecutorsMetrics.groupBy("t", CpuTime.name).sum.sortBy("t")
        val groupedTimeCol = clusterCpuTimeDf.select("t")

        val clusterCpuUsageCol = allExecutorsMetrics
          .groupBy("t", ColCpuUsage)
          .avg
          .sortBy("t")
          .select(ColCpuUsage)
          .lt(1.0d)
          .rename(ColCpuUsage)
        val clusterCpuUsageDf = DataTable(name = "clusterCpuUsage", columns = Seq(groupedTimeCol, clusterCpuUsageCol))

        val numExecutors = allExecutorsMetrics.groupBy("t", ColCpuUsage).count.sortBy("t").select(Seq("t", "cnt"))
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
            cpuTimePerExecutor = allExecutorsMetrics.groupBy("executorId", CpuTime.name).max,
            numExecutors = numExecutors
        )
    }
}
