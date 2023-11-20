package com.ucesys.sparkscope.metrics

import com.ucesys.sparkscope.data.DataTable

case class SparkScopeMetrics(driverMetrics: DataTable,
                             executorMemoryMetrics: ExecutorMemoryMetrics,
                             clusterMemoryMetrics: ClusterMemoryMetrics,
                             clusterCPUMetrics: ClusterCPUMetrics,
                             stageTimeline: DataTable)
