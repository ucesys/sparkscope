package com.ucesys.sparkscope.common

import org.apache.spark.SparkConf

case class SparkScopeConf(driverMetricsDir: String,
                          executorMetricsDir: String,
                          htmlReportPath: String,
                          logPath: String,
                          appName: Option[String],
                          region: Option[String],
                          sendDiagnostics: Boolean,
                          driverMemOverhead: MemorySize,
                          executorMemOverhead: MemorySize,
                          sparkConf: SparkConf)
