package com.ucesys.sparkscope.metrics

import com.ucesys.sparkscope.common.{JvmHeapMax, JvmHeapUsed, JvmNonHeapUsed}
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
            heapUsedMax = allExecutorsMetrics.groupBy("t", JvmHeapUsed.name).max.sortBy("t"),
            heapUsedMin = allExecutorsMetrics.groupBy("t", JvmHeapUsed.name).min.sortBy("t"),
            heapUsedAvg = allExecutorsMetrics.groupBy("t", JvmHeapUsed.name).avg.sortBy("t"),
            heapAllocation = allExecutorsMetrics.groupBy("t", JvmHeapMax.name).max.sortBy("t"),
            nonHeapUsedMax = allExecutorsMetrics.groupBy("t", JvmNonHeapUsed.name).max.sortBy("t"),
            nonHeapUsedMin = allExecutorsMetrics.groupBy("t", JvmNonHeapUsed.name).min.sortBy("t"),
            nonHeapUsedAvg = allExecutorsMetrics.groupBy("t", JvmNonHeapUsed.name).avg.sortBy("t"),
            executorMetricsMap
        )
    }
}
