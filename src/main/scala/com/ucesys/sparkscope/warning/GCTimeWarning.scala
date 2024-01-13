package com.ucesys.sparkscope.warning

import com.ucesys.sparkscope.agg.TaskAggMetrics
import com.ucesys.sparkscope.view.DurationExtensions.FiniteDurationExtensions

import scala.concurrent.duration.{DurationLong, FiniteDuration}

case class GCTimeWarning(taskDuration: FiniteDuration, jvmGCTime: FiniteDuration, gcTimeFraction: Double) extends Warning {
    override def toString: String = {
        f"${gcTimeFraction*100}%1.2f%% of total task duration was spent in garbage collection. GC time: ${jvmGCTime.durationStr}. Total task time: ${taskDuration.durationStr}."
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