package com.ucesys.sparkscope.metrics

import com.ucesys.sparkscope.SparkScopeAnalyzer.{CpuTime, CpuUsage}
import com.ucesys.sparkscope.data.DataFrame

case class ClusterCPUMetrics(clusterCpuTime: DataFrame,
                             clusterCpuUsage: DataFrame,
                             clusterCpuUsageSum: DataFrame,
                             clusterCapacity: DataFrame)

object ClusterCPUMetrics {
    def apply(allExecutorsMetrics: DataFrame, executorCores: Int): ClusterCPUMetrics = {
        val clusterCpuTimeDf = allExecutorsMetrics.groupBy("t", CpuTime).sum.sortBy("t")
        val clusterCpuUsage = allExecutorsMetrics.groupBy("t", CpuUsage).avg.sortBy("t")
        val clusterCpuUsageSum = allExecutorsMetrics.groupBy("t", CpuUsage).sum.sortBy("t")
        val clusterCapacityCol = allExecutorsMetrics.groupBy("t", CpuUsage).count.sortBy("t").select("cnt").mul(executorCores)
        val clusterCapacityDf = DataFrame(
            name = "clusterCapacity",
            columns = Seq(allExecutorsMetrics.select("t"), clusterCapacityCol.rename("totalCores"))
        )
        ClusterCPUMetrics(
            clusterCpuTime=clusterCpuTimeDf,
            clusterCpuUsage=clusterCpuUsage,
            clusterCpuUsageSum=clusterCpuUsageSum,
            clusterCapacity=clusterCapacityDf
        )
    }
}
