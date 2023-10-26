package com.ucesys.sparkscope.eventlog

import com.ucesys.sparklens.common.AppContext
import org.apache.spark.SparkConf

case class EventLogContext(sparkConf: SparkConf, appContext: AppContext)
