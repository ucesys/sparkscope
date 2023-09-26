package com.ucesys.sparkscope.metrics

import com.ucesys.sparkscope.SparkScopeAnalyzer.{CpuTime, CpuUsage}
import com.ucesys.sparkscope.data.DataFrame

case class ClusterCPUMetrics(clusterCpuTime: DataFrame, clusterCpuUsage: DataFrame)

object ClusterCPUMetrics {
    def apply(allExecutorsMetrics: DataFrame): ClusterCPUMetrics = {
        val clusterCpuTimeDf = allExecutorsMetrics.groupBy("t", CpuTime).sum.sortBy("t")
        val clusterCpuUsage = allExecutorsMetrics.groupBy("t", CpuUsage).avg.sortBy("t")
        ClusterCPUMetrics(clusterCpuTime=clusterCpuTimeDf, clusterCpuUsage=clusterCpuUsage)
    }
}
