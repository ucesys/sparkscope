package com.ucesys.sparkscope.metrics

import com.ucesys.sparkscope.common.StageContext
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
    def apply(stages: Seq[StageContext], allTimestamps: Seq[String]): StageMetrics = {
        val stageColumns: Seq[DataColumn] = stages.map { stage =>
            val colVals: Seq[String] = allTimestamps.map(_.toLong).map { ts =>
                if (ts > stage.getTimelineStart && ts < stage.getTimelineEnd) {
                    stage.numberOfTasks.toString
                } else if (ts == stage.getTimelineStart || ts == stage.getTimelineEnd) {
                    "0"
                } else {
                    "null"
                }
            }
            DataColumn(stage.stageId, colVals)
        }

        val numberOfTasksVals: Seq[String] = allTimestamps.map(_.toLong).map { ts =>
            val numberOfTasksSum: Long = stages.map { stage =>
                if (ts >= stage.getTimelineStart && ts <= stage.getTimelineEnd) {
                    stage.numberOfTasks
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
