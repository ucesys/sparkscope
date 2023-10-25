package com.ucesys.sparkscope

import org.apache.spark.SparkConf

case class SparkScopeConf(driverMetricsDir: String, executorMetricsDir: String, htmlReportPath: String, sparkConf: SparkConf)
