package com.ucesys.sparkscope.metrics

import com.ucesys.sparkscope.data.DataFrame

case class SparkScopeMetrics(driverMetrics: DataFrame,
                             executorMemoryMetrics: ExecutorMemoryMetrics,
                             clusterMemoryMetrics: ClusterMemoryMetrics,
                             clusterCPUMetrics: ClusterCPUMetrics)
