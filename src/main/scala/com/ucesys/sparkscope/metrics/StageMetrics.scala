package com.ucesys.sparkscope.metrics

import com.ucesys.sparkscope.timeline.StageTimeline
import com.ucesys.sparkscope.data.{DataColumn, DataTable}

case class StageMetrics(stageTimeline: DataTable, numberOfTasks: DataColumn) {
    override def toString: String = {
        Seq(
            s"\nStage metrics:",
            stageTimeline.toString
        ).mkString("\n")
    }
}

object StageMetrics {
    def apply(stages: Seq[StageTimeline], allTimestamps: Seq[String]): StageMetrics = {
        val stageColumns: Seq[DataColumn] = stages.map { stageTimeline =>
            val colVals: Seq[String] = allTimestamps.map(_.toLong).map { ts =>
                if (stageTimeline.hasTimePointInside(ts)) {
                    stageTimeline.numberOfTasks.toString
                } else if (stageTimeline.hasEdgeTimePoint(ts)) {
                    "0"
                } else {
                    "null"
                }
            }
            DataColumn(stageTimeline.stageId.toString, colVals)
        }

        val numberOfTasksVals: Seq[String] = allTimestamps.map(_.toLong).map { ts =>
            val numberOfTasksSum: Long = stages.map { stageTimeline =>
                if (stageTimeline.hasTimePoint(ts)) {
                    stageTimeline.numberOfTasks
                } else {
                    0L
                }
            }.sum
            numberOfTasksSum.toString
        }

        StageMetrics(
            stageTimeline = DataTable("stages", Seq(DataColumn("t", allTimestamps)) ++ stageColumns),
            numberOfTasks = DataColumn("numberOfTasks", numberOfTasksVals)
        )
    }
}
