package com.ucesys.sparkscope.event

import com.ucesys.sparkscope.common.SparkScopeContext
import org.apache.spark.SparkConf

case class EventLogContext(sparkConf: SparkConf, appContext: SparkScopeContext)
