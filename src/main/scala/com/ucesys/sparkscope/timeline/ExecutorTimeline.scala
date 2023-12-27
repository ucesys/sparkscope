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
import org.apache.spark.scheduler.{SparkListenerExecutorAdded, SparkListenerExecutorRemoved, TaskInfo}

case class ExecutorTimeline(executorId: String, hostId: String, cores: Int, startTime: Long, endTime: Option[Long] = None) {
    var executorMetrics = new AggregateMetrics()

    def updateAggregateTaskMetrics(taskMetrics: TaskMetrics, taskInfo: TaskInfo): Unit = {
        executorMetrics.update(taskMetrics, taskInfo)
    }

    def upTime(lastMetricTimeMs: Long)(implicit logger: SparkScopeLogger): Long = {
        val executorEndTime: Long = endTime match {
            case Some(time) => time
            case None =>
                logger.info(s"Missing remove time for executorId=${executorId}, using last metric timestamp to calculate uptime")
                lastMetricTimeMs*1000
        }
        executorEndTime - startTime
    }

    def end(executorRemoved: SparkListenerExecutorRemoved): ExecutorTimeline = {
        this.copy(endTime = Some(executorRemoved.time))
    }
}

object ExecutorTimeline {
    def apply(executorAdded: SparkListenerExecutorAdded): ExecutorTimeline = {
        new ExecutorTimeline(
            executorAdded.executorId,
            executorAdded.executorInfo.executorHost,
            executorAdded.executorInfo.totalCores,
            executorAdded.time
        )
    }
}