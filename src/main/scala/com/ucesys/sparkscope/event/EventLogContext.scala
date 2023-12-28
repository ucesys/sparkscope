package com.ucesys.sparkscope.event

import com.ucesys.sparkscope.common.AppContext
import org.apache.spark.SparkConf

case class EventLogContext(sparkConf: SparkConf, appContext: AppContext)
