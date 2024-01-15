package com.ucesys.sparkscope.stats

case class SparkScopeStats(driverStats: DriverMemoryStats,
                           executorStats: ExecutorMemoryStats,
                           clusterMemoryStats: ClusterMemoryStats,
                           clusterCPUStats: ClusterCPUStats)
