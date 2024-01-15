package com.ucesys.sparkscope.metrics

import com.ucesys.sparkscope.data.DataTable

case class SparkScopeMetrics(driver: DataTable,
                             executor: ExecutorMemoryMetrics,
                             clusterMemory: ClusterMemoryMetrics,
                             clusterCpu: ClusterCPUMetrics,
                             stage: StageMetrics)
