package com.ucesys.sparkscope.metrics

case class SparkScopeStats(driverStats: DriverMemoryStats,
                           executorStats: ExecutorMemoryStats,
                           clusterMemoryStats: ClusterMemoryStats,
                           clusterCPUStats: ClusterCPUStats)
