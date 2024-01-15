package com.ucesys.sparkscope.common

import com.ucesys.sparkscope.common.MetricType.{AllMetricsDriver, AllMetricsExecutor}

object MetricUtils {
    val ColCpuUsage = "cpuUsage"
    val ColTs = "t"

    val AllMetricNamesDriver: Seq[String] = AllMetricsDriver.map(_.name)
    val AllMetricNamesExecutor: Seq[String] = AllMetricsExecutor.map(_.name)

    val DriverCsvColumns: Seq[String] = Seq(ColTs) ++ AllMetricNamesDriver
    val ExecutorCsvColumns: Seq[String] = Seq(ColTs) ++ AllMetricNamesExecutor
}
