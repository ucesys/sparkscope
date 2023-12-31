package com.ucesys.sparkscope.warning

import com.ucesys.sparkscope.agg.TaskAggMetrics

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration._

class GCTimeWarning private(taskDuration: FiniteDuration, jvmGCTime: FiniteDuration, gcTimeFraction: Double) extends Warning {
    override def toString: String = {
        f"Your application has spent ${gcTimeFraction}%% of total task duration in garbage collection(GC time in secs: ${jvmGCTime.toSeconds}, total task time in secs: ${taskDuration.toSeconds})"
    }
}

object GCTimeWarning {
    val GcTimeFractionThreshold = 0.05

    def apply(taskAggMetrics: TaskAggMetrics): Option[GCTimeWarning] = {
        val gcTimeFraction = taskAggMetrics.jvmGCTime.sum.toDouble / taskAggMetrics.taskDuration.sum.toDouble
        if(gcTimeFraction >= GcTimeFractionThreshold) {
            Some(new GCTimeWarning(taskAggMetrics.taskDuration.sum.seconds, taskAggMetrics.jvmGCTime.sum.seconds, gcTimeFraction))
        } else {
            None
        }
    }
}