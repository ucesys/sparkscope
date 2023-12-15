package com.ucesys.sparkscope.common

import com.ucesys.sparkscope.event.{StageCompletedEvent, StageSubmittedEvent}
import com.ucesys.sparkscope.timeline.StageTimeline

case class StageContext(stageId: String, startTime: Long, endTime: Long, numberOfTasks: Long) {
    def getTimeline: Seq[Long] = Seq(getTimelineStart, (getTimelineStart + getTimelineEnd) / 2, getTimelineEnd)
    def getTimelineStart: Long = startTime - 1
    def getTimelineEnd: Long = endTime + 1
}

object StageContext {
    def apply(stgTimeSpan: StageTimeline): Option[StageContext] = {
        if(stgTimeSpan.startTime == 0 ||  stgTimeSpan.endTime == 0) {
            None
        } else {
            Some(StageContext(stgTimeSpan.stageID.toString, stgTimeSpan.startTime/1000, stgTimeSpan.endTime/1000, stgTimeSpan.numberOfTasks))
        }
    }

    def apply(submitted: StageSubmittedEvent, completed: StageCompletedEvent): StageContext = {
        StageContext(submitted.stageId, submitted.submissionTime, completed.completionTime, submitted.numberOfTasks)
    }
}
