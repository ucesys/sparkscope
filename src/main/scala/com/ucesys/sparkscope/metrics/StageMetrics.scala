/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
