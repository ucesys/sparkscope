package com.ucesys.sparkscope.common

import com.ucesys.sparklens.timespan.StageTimeSpan

case class StageContext(stageId: String, startTime: Long, endTime: Long, numberOfTasks: Long)

object StageContext {
    def apply(stgTimeSpan: StageTimeSpan): Option[StageContext] = {
        if(stgTimeSpan.startTime == 0 ||  stgTimeSpan.endTime == 0) {
            None
        } else {
            Some(StageContext(stgTimeSpan.stageID.toString, stgTimeSpan.startTime/1000, stgTimeSpan.endTime/1000, stgTimeSpan.numberOfTasks))
        }
    }
}
