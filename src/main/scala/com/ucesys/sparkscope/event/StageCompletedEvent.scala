package com.ucesys.sparkscope.event

import com.ucesys.sparkscope.common.SparkScopeLogger

case class StageCompletedEvent(stageId: String, completionTime: Long)

object StageCompletedEvent {
    val StageInfo = "Stage Info"
    val StageId = "Stage ID"
    val CompletionTime = "Completion Time"

    def apply(eventMap: Map[String, Any])(implicit logger: SparkScopeLogger): Option[StageCompletedEvent] = {
        try {
            Some(StageCompletedEvent(
                eventMap(StageInfo).asInstanceOf[Map[String, Any]](StageId).asInstanceOf[Double].toLong.toString,
                eventMap(StageInfo).asInstanceOf[Map[String, Any]](CompletionTime).asInstanceOf[Double].toLong / 1000
            ))
        } catch {
            case ex: Exception => logger.warn("Couldn't parse StageCompletedEvent. " + ex); None
        }

    }
}
