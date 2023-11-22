package com.ucesys.sparkscope.event

case class StageSubmittedEvent(stageId: String, submissionTime: Long, numberOfTasks: Long)

object StageSubmittedEvent {
    val StageInfo = "Stage Info"
    val StageId = "Stage ID"
    val SubmissionTime = "Submission Time"
    val NumberOfTasks = "Number of Tasks"

    def apply(eventMap: Map[String, Any]): StageSubmittedEvent = {
        StageSubmittedEvent(
            eventMap(StageInfo).asInstanceOf[Map[String, Any]](StageId).asInstanceOf[Double].toLong.toString,
            eventMap(StageInfo).asInstanceOf[Map[String, Any]](SubmissionTime).asInstanceOf[Double].toLong/1000,
            eventMap(StageInfo).asInstanceOf[Map[String, Any]](NumberOfTasks).asInstanceOf[Double].toLong
        )
    }
}
