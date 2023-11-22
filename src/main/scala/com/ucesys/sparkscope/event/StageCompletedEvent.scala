package com.ucesys.sparkscope.event

case class StageCompletedEvent(stageId: String, completionTime: Long)

object StageCompletedEvent {
    val StageInfo = "Stage Info"
    val StageId = "Stage ID"
    val CompletionTime = "Completion Time"

    def apply(eventMap: Map[String, Any]): StageCompletedEvent = {
        StageCompletedEvent(
            eventMap(StageInfo).asInstanceOf[Map[String, Any]](StageId).asInstanceOf[Double].toLong.toString,
            eventMap(StageInfo).asInstanceOf[Map[String, Any]](CompletionTime).asInstanceOf[Double].toLong/1000
        )
    }
}
