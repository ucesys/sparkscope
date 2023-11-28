package com.ucesys.sparkscope.io.metrics

import com.ucesys.sparkscope.data.DataTable

trait MetricReader {
    def readDriver: DataTable
    def readExecutor(executorId: String): DataTable
}
