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

import com.ucesys.sparkscope.listener.AggregateMetrics
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.TaskInfo

import scala.collection.mutable


/*
* The timeSpan of a Job can seen with respect to other jobs as well
* as driver timeSpans providing a timeLine. The other way to look at
* Job timeline is to dig deep and check how the individual stages are
* doing
*
* @param jobID
*/

class JobTimeline(val jobID: Long) extends Timeline {
    var jobMetrics = new AggregateMetrics()
    var stageMap = new mutable.HashMap[Int, StageTimeline]()

    def addStage(stage: StageTimeline): Unit = {
        stageMap(stage.stageId) = stage
    }

    def updateAggregateTaskMetrics(taskMetrics: TaskMetrics, taskInfo: TaskInfo): Unit = {
        jobMetrics.update(taskMetrics, taskInfo)
    }

    /*
    This function computes the minimum time it would take to run this job.
    The computation takes into account the parallel stages.
     */
    def computeCriticalTimeForJob: Option[Long] = {
        if (stageMap.isEmpty) {
            None
        } else {
            val maxStageID = stageMap.keys.max
            Some(criticalTime(maxStageID))
        }
    }

    private def criticalTime(stageID: Int): Long = {
        val stageTimeline =  stageMap.get(stageID)
        val parentStageIDs = stageTimeline.map(_.parentStageIds).getOrElse(List.empty[Int])
        val maxExecutorRuntime = stageTimeline.map(_.stageMetrics.map(AggregateMetrics.executorRuntime).max).getOrElse(0L)

        val parentStagesCriticalTime = parentStageIDs match {
            case Seq() => 0L
            case _ => parentStageIDs.map(parentStageId => criticalTime(parentStageId)).max
        }
        maxExecutorRuntime + parentStagesCriticalTime
    }

    override def getMap: Map[String, _ <: Any] = {
        Map(
            "jobID" -> jobID,
            "jobMetrics" -> jobMetrics.getMap,
            "stageMap" -> stageMap.map{ case (stageId, stageTimeline) => (stageId.toString, stageTimeline.getMap) }.toMap
        ) ++ super.getStartEndTime
    }
}

object JobTimeline {
    def apply(jobID: Long, startTime: Long): JobTimeline = {
        val timeSpan = new JobTimeline(jobID)
        timeSpan.setStartTime(startTime)
        timeSpan
    }
}
