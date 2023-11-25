package com.ucesys.sparkscope.io.metrics

import com.ucesys.sparkscope.common.MetricType
import com.ucesys.sparkscope.data.DataTable

trait MetricReader {
    def readDriver(metricType: MetricType): DataTable
    def readExecutor(metricType: MetricType, executorId: String): DataTable
}
