package com.ucesys.sparkscope.metrics

import com.qubole.sparklens.common.ApplicationInfo
import org.apache.spark.SparkConf

case class SparkScopeResult(appInfo: ApplicationInfo,
                            sparkConf: SparkConf,
                            logs: String,
                            stats: SparkScopeStats,
                            metrics: SparkScopeMetrics)
