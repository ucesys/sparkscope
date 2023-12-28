/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.ucesys.sparkscope.timeline

import org.apache.spark.scheduler.{SparkListenerStageCompleted, SparkListenerStageSubmitted}

case class StageTimeline(stageId: Int, startTime: Option[Long], numberOfTasks: Long, parentStageIds: Seq[Int] = Seq.empty, endTime: Option[Long] = None) {
    def getTimeline: Seq[Long] = Seq(getTimelineStart, getTimelineCentre, getTimelineEnd).flatten
    def getTimelineStart: Option[Long] = startTime.map(start => (start / 1000L) - 1)
    def getTimelineEnd: Option[Long] = endTime.map(end => (end / 1000L) + 1)
    def getTimelineCentre: Option[Long] = getTimelineStart.flatMap(start => getTimelineEnd.map(end => (start + end) / 2))

    def hasTimePoint(ts: Long): Boolean = {
        hasTimePointInside(ts) || hasEdgeTimePoint(ts)
    }

    def hasTimePointInside(ts: Long): Boolean = {
        getTimelineStart.flatMap(start => getTimelineEnd.map(end => ts > start && ts < end)).getOrElse(false)
    }

    def hasEdgeTimePoint(ts: Long): Boolean = {
        getTimelineStart.flatMap(start => getTimelineEnd.map(end => ts == start || ts == end)).getOrElse(false)
    }

    def end(stageCompleted: SparkListenerStageCompleted): StageTimeline = {
        this.copy(
            startTime = Some(this.startTime.getOrElse(stageCompleted.stageInfo.submissionTime.getOrElse(
                throw new IllegalArgumentException("Stage submission time empty!")))
            ),
            endTime = Some(this.endTime.getOrElse(stageCompleted.stageInfo.completionTime.getOrElse(
                throw new IllegalArgumentException("Stage completion time empty!")))
            )
        )
    }
}

object StageTimeline {
    def apply(stageSubmitted: SparkListenerStageSubmitted): StageTimeline = {
        new StageTimeline(
            stageSubmitted.stageInfo.stageId,
            stageSubmitted.stageInfo.submissionTime,
            stageSubmitted.stageInfo.numTasks,
            stageSubmitted.stageInfo.parentIds
        )
    }
}
