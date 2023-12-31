package com.ucesys.sparkscope.common

import org.apache.spark.SparkConf

case class SparkScopeConf(driverMetricsDir: String,
                          executorMetricsDir: String,
                          htmlReportPath: String,
                          logPath: Option[String],
                          appName: Option[String],
                          region: Option[String],
                          driverMemOverhead: MemorySize,
                          executorMemOverhead: MemorySize,
                          sparkConf: SparkConf)
