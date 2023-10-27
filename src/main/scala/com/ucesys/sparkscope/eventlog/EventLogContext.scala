package com.ucesys.sparkscope.eventlog

import com.ucesys.sparkscope.common.SparkScopeContext
import org.apache.spark.SparkConf

case class EventLogContext(sparkConf: SparkConf, appContext: SparkScopeContext)
