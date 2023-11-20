package com.ucesys.sparkscope.metrics

import com.ucesys.sparkscope.SparkScopeAnalyzer._
import com.ucesys.sparkscope.data.DataTable

case class ExecutorMemoryMetrics(heapUsedMax: DataTable,
                                 heapUsedMin: DataTable,
                                 heapUsedAvg: DataTable,
                                 heapAllocation: DataTable,
                                 nonHeapUsedMax: DataTable,
                                 nonHeapUsedMin: DataTable,
                                 nonHeapUsedAvg: DataTable,
                                 executorMetricsMap: Map[String, DataTable])

object ExecutorMemoryMetrics {
    def apply(allExecutorsMetrics: DataTable, executorMetricsMap: Map[String, DataTable]): ExecutorMemoryMetrics = {
        ExecutorMemoryMetrics(
            heapUsedMax = allExecutorsMetrics.groupBy("t", JvmHeapUsed).max.sortBy("t"),
            heapUsedMin = allExecutorsMetrics.groupBy("t", JvmHeapUsed).min.sortBy("t"),
            heapUsedAvg = allExecutorsMetrics.groupBy("t", JvmHeapUsed).avg.sortBy("t"),
            heapAllocation = allExecutorsMetrics.groupBy("t", JvmHeapMax).max.sortBy("t"),
            nonHeapUsedMax = allExecutorsMetrics.groupBy("t", JvmNonHeapUsed).max.sortBy("t"),
            nonHeapUsedMin = allExecutorsMetrics.groupBy("t", JvmNonHeapUsed).min.sortBy("t"),
            nonHeapUsedAvg = allExecutorsMetrics.groupBy("t", JvmNonHeapUsed).avg.sortBy("t"),
            executorMetricsMap
        )
    }
}
