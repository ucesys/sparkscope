package com.ucesys.sparkscope.common

import com.ucesys.sparklens.common.AppContext

case class SparkScopeContext(appId: String,
                             appStartTime: Long,
                             appEndTime: Option[Long],
                             executorMap: Map[String, ExecutorContext]) {
  def executorCores: Int = executorMap.head._2.cores.toInt
}

object SparkScopeContext {
    def apply(sparkLensContext: AppContext): SparkScopeContext = {

        val executorMap: Map[String, ExecutorContext] = sparkLensContext.executorMap.map{ case(id, timeSpan) =>
          (id, ExecutorContext(timeSpan))
        }.toMap

      SparkScopeContext(
        appId = sparkLensContext.appInfo.applicationID,
        appStartTime = sparkLensContext.appInfo.startTime,
        appEndTime = Some(sparkLensContext.appInfo.endTime),
        executorMap = executorMap
      )
    }
}
