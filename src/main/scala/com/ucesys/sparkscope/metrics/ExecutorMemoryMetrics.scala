package com.ucesys.sparkscope.metrics

import com.ucesys.sparkscope.ExecutorMetricsAnalyzer._
import com.ucesys.sparkscope.data.DataFrame

case class ExecutorMemoryMetrics(heapUsedMax: DataFrame,
                           heapUsedMin: DataFrame,
                           heapUsedAvg: DataFrame,
                           heapAllocation: DataFrame,
                           nonHeapUsedMax: DataFrame,
                           nonHeapUsedMin: DataFrame,
                           nonHeapUsedAvg: DataFrame)

object ExecutorMemoryMetrics {
  def apply(allExecutorsMetrics: DataFrame): ExecutorMemoryMetrics = {
    ExecutorMemoryMetrics(
      heapUsedMax = allExecutorsMetrics.groupBy("t", JvmHeapUsed).max.sortBy("t"),
      heapUsedMin = allExecutorsMetrics.groupBy("t", JvmHeapUsed).min.sortBy("t"),
      heapUsedAvg = allExecutorsMetrics.groupBy("t", JvmHeapUsed).avg.sortBy("t"),
      heapAllocation = allExecutorsMetrics.groupBy("t", JvmHeapMax).max.sortBy("t"),
      nonHeapUsedMax = allExecutorsMetrics.groupBy("t", JvmNonHeapUsed).max.sortBy("t"),
      nonHeapUsedMin = allExecutorsMetrics.groupBy("t", JvmNonHeapUsed).min.sortBy("t"),
      nonHeapUsedAvg = allExecutorsMetrics.groupBy("t", JvmNonHeapUsed).avg.sortBy("t")
    )
  }
}
