package com.ucesys.sparkscope.io.metrics

import com.ucesys.sparkscope.data.DataTable

case class DriverExecutorMetrics(driverMetrics: DataTable, executorMetricsMap: Map[String, DataTable])
