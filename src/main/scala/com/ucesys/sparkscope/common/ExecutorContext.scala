package com.ucesys.sparkscope.common

import com.ucesys.sparkscope.timeline.ExecutorTimeline

case class ExecutorContext(executorId: String,
                           cores: Long,
                           addTime: Long,
                           removeTime: Option[Long]) {
    def upTime(lastMetricTimeMs: Long)(implicit logger: SparkScopeLogger): Long = {
        val executorEndTime: Long = removeTime match {
            case Some(time) => time
            case None =>
                logger.info(s"Missing remove time for executorId=${executorId}, using last metric timestamp to calculate uptime")
                lastMetricTimeMs*1000
        }
        executorEndTime - addTime
    }
}

object ExecutorContext {
    def apply(executorTimeSpan: ExecutorTimeline): ExecutorContext = {
        val removeTime: Option[Long] = executorTimeSpan.endTime match {
            case 0 => None
            case _ => Some(executorTimeSpan.endTime)
        }
        ExecutorContext(executorTimeSpan.executorID, executorTimeSpan.cores, executorTimeSpan.startTime, removeTime)
    }
}
