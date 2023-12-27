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

import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.listener.AggregateMetrics
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.TaskInfo

class ExecutorTimeline(val executorId: String, val hostId: Option[String], val cores: Int) extends Timeline {
    var executorMetrics = new AggregateMetrics()

    def updateAggregateTaskMetrics(taskMetrics: TaskMetrics, taskInfo: TaskInfo): Unit = {
        executorMetrics.update(taskMetrics, taskInfo)
    }

    override def getMap: Map[String, _ <: Any] = {
        Map("executorId" -> executorId, "hostId" -> hostId, "cores" -> cores, "executorMetrics" ->
          executorMetrics.getMap()) ++ super.getStartEndTime
    }

    def upTime(lastMetricTimeMs: Long)(implicit logger: SparkScopeLogger): Long = {
        val executorEndTime: Long = getEndTime match {
            case Some(time) => time
            case None =>
                logger.info(s"Missing remove time for executorId=${executorId}, using last metric timestamp to calculate uptime")
                lastMetricTimeMs*1000
        }
        getStartTime.map(startTime => executorEndTime - startTime).getOrElse(0L)
    }
}

object ExecutorTimeline {
    def apply(executorId: String, hostId: Option[String], cores: Int, startTime: Long): ExecutorTimeline = {
        val timeSpan = new ExecutorTimeline(executorId, hostId, cores)
        timeSpan.setStartTime(startTime)
        timeSpan
    }

    def apply(executorId: String, cores: Int, startTime: Long, endTime: Option[Long]): ExecutorTimeline = {
        val timeSpan = new ExecutorTimeline(executorId, None, cores)
        timeSpan.setStartTime(startTime)
        timeSpan.setEndTime(endTime)
        timeSpan
    }
}