package com.ucesys.sparkscope.metrics

import com.ucesys.sparkscope.common.StageContext
import com.ucesys.sparkscope.data.{DataColumn, DataTable}

case class StageMetrics(stageTimeline: DataTable, clusterCPUCapacity: DataTable) {
    override def toString: String = {
        Seq(
            s"\nStage metrics:",
            stageTimeline.toString
        ).mkString("\n")
    }
}

object StageMetrics {
    def apply(stages: Seq[StageContext], allTimestamps: Seq[String], clusterCPUCapacity: DataTable): StageMetrics = {
        // Stages
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

        val clusterCPUCapacityAligned =  {
            val existingTimestamps = clusterCPUCapacity.select("t").values
            val newTimestamps = allTimestamps.filterNot(existingTimestamps.contains)
            DataTable(
                "heapUsed",
                Seq(
                    DataColumn("t", clusterCPUCapacity.select("t").values ++ newTimestamps),
                    DataColumn("totalCores", clusterCPUCapacity.select("totalCores").values ++ Seq.fill(newTimestamps.length)("null")),
                )
            ).sortBy("t")
        }

        StageMetrics(
            stageTimeline = DataTable("stages", Seq(DataColumn("t", allTimestamps)) ++ stageColumns),
            clusterCPUCapacity = clusterCPUCapacityAligned
        )
    }
}
