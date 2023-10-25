package com.ucesys.sparkscope.io

import com.ucesys.sparklens.common.AppContext
import org.apache.spark.SparkConf

case class EventLogContext(sparkConf: SparkConf, appContext: AppContext)

object EventLogContext {
    def load(eventLogPath: String): EventLogContext = ???
}