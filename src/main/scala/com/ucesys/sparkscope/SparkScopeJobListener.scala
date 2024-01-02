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

package com.ucesys.sparkscope

import com.ucesys.sparkscope.agg.TaskAggMetrics
import com.ucesys.sparkscope.common.AppContext
import com.ucesys.sparkscope.timeline.{ExecutorTimeline, JobTimeline, StageTimeline}
import org.apache.spark.SparkConf
import org.apache.spark.scheduler._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class SparkScopeJobListener(var sparkConf: SparkConf, val runner: SparkScopeRunner) extends SparkListener {
    private[sparkscope] var applicationStartEvent: Option[SparkListenerApplicationStart] = None
    private[sparkscope] val executorMap = new mutable.HashMap[String, ExecutorTimeline]
    private[sparkscope] val jobMap = new mutable.HashMap[Long, JobTimeline]
    private[sparkscope] val stageMap = new mutable.HashMap[Int, StageTimeline]
    private[sparkscope] val taskAggMetrics = TaskAggMetrics()
    private val failedStages = new ListBuffer[Int]

    def this(sparkConf: SparkConf) = {
        this(sparkConf, SparkScopeRunner())
    }

    private def getDriverTimePercentage(endTime: Option[Long]): Option[Double] = {
        getJobTimePercentage(endTime).map(jobTimePercentage => 1 - jobTimePercentage)
    }

    private def getJobTimePercentage(endTimeOpt: Option[Long]): Option[Double] = endTimeOpt match {
        case None => None
        case Some(endTime) =>
            val appTime = endTime - applicationStartEvent.map(_.time).getOrElse(0L)
            val jobTime = jobMap.values.flatMap(_.duration).sum
            Some(jobTime / appTime)
    }

    override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {
        val sparkConfUpdated = new SparkConf(false)

        environmentUpdate.environmentDetails.get("Spark Properties").foreach(_.foreach { case (key, value) => sparkConfUpdated.set(key, value) })

        this.sparkConf = sparkConfUpdated
    }

    override def onTaskEnd(end: SparkListenerTaskEnd): Unit = {
        taskAggMetrics.aggregate(end.taskMetrics, end.taskInfo)
    }

    override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
        applicationStartEvent = Some(applicationStart)
    }

    override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
        executorMap(executorAdded.executorId) = ExecutorTimeline(executorAdded)
    }

    override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
        executorMap.get(executorRemoved.executorId).foreach{ executorTimeline =>
            executorMap(executorRemoved.executorId) = executorTimeline.end(executorRemoved)
        }
    }

    override def onJobStart(jobStart: SparkListenerJobStart) {
        jobMap(jobStart.jobId) = JobTimeline(jobStart)
    }

    override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
        jobMap(jobEnd.jobId) = jobMap(jobEnd.jobId).end(jobEnd)
    }

    override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
        if (!stageMap.contains(stageSubmitted.stageInfo.stageId)) {
            stageMap(stageSubmitted.stageInfo.stageId) = StageTimeline(stageSubmitted)
        }
    }

    override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
        stageMap.get(stageCompleted.stageInfo.stageId).foreach { stageTimeline =>
            stageMap(stageCompleted.stageInfo.stageId) = stageTimeline.end(stageCompleted)
        }

        stageCompleted.stageInfo.failureReason.foreach { reason =>
            runner.logger.warn(s"Stage ${stageCompleted.stageInfo.stageId} failed with reason: ${reason}", this.getClass)
            failedStages += stageCompleted.stageInfo.stageId
        }
    }

    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
        runSparkScopeAnalysis(Some(applicationEnd.time))
    }

    def runSparkScopeAnalysis(applicationEnd: Option[Long]) = {
        val appStartEvent = applicationStartEvent.getOrElse(throw new IllegalArgumentException("App start event is empty"))

        val appContext = AppContext(
            appId = appStartEvent.appId.getOrElse(throw new IllegalArgumentException("App Id is empty")),
            appStartTime = appStartEvent.time,
            appEndTime = applicationEnd,
            executorMap = executorMap.toMap,
            stages = stageMap.values.toSeq
        )

        runner.run(appContext, sparkConf, taskAggMetrics)
    }
}
