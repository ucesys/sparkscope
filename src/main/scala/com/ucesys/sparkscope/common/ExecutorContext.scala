package com.ucesys.sparkscope.common

import com.ucesys.sparklens.timespan.ExecutorTimeSpan

case class ExecutorContext(executorId: String,
                           cores: Long,
                           addTime: Long,
                           removeTime: Option[Long]) {
    def upTime(lastMetricTimeMs: Long): Long = removeTime.getOrElse(lastMetricTimeMs*1000) - addTime
}

object ExecutorContext {
    def apply(executorTimeSpan: ExecutorTimeSpan): ExecutorContext = {
            val removeTime: Option[Long] = executorTimeSpan.endTime match {
                case 0 => None
                case _ => Some(executorTimeSpan.endTime)
            }
            ExecutorContext(executorTimeSpan.executorID, executorTimeSpan.cores, executorTimeSpan.startTime, removeTime)
    }
}
