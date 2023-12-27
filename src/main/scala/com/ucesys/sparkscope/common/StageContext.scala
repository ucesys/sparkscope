//package com.ucesys.sparkscope.common
//
//import com.ucesys.sparkscope.event.{StageCompletedEvent, StageSubmittedEvent}
//import com.ucesys.sparkscope.timeline.StageTimeline
//
//case class StageContext(stageId: String, startTime: Long, endTime: Long, numberOfTasks: Long) {
//    def getTimeline: Seq[Long] = Seq(getTimelineStart, (getTimelineStart + getTimelineEnd) / 2, getTimelineEnd)
//    def getTimelineStart: Long = startTime - 1
//    def getTimelineEnd: Long = endTime + 1
//}
//
//object StageContext {
//    def apply(stgTimeSpan: StageTimeline): Option[StageContext] = {
//        stgTimeSpan.getStartTime.flatMap(startTime =>
//            stgTimeSpan.getEndTime.map(endTime =>
//                StageContext(stgTimeSpan.stageID.toString, startTime/1000, endTime/1000, stgTimeSpan.numberOfTasks)
//            )
//        )
//    }
//
//    def apply(submitted: StageSubmittedEvent, completed: StageCompletedEvent): StageContext = {
//        StageContext(submitted.stageId, submitted.submissionTime, completed.completionTime, submitted.numberOfTasks)
//    }
//}
