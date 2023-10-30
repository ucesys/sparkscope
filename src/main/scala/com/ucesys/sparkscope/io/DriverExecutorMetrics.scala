package com.ucesys.sparkscope.io

import com.ucesys.sparkscope.data.DataTable

case class DriverExecutorMetrics(driverMetrics: Seq[DataTable],
                                 executorMetricsMap: Map[String, Seq[DataTable]])
