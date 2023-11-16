package com.ucesys.sparkscope.io.metrics

import com.ucesys.sparkscope.data.DataTable
import com.ucesys.sparkscope.io.MetricType

trait MetricReader {
    def readDriver(metricType: MetricType): DataTable
    def readExecutor(metricType: MetricType, executorId: String): DataTable
}
