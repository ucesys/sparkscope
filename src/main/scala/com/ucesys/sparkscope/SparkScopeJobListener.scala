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

// TODO GET RID OF

import com.ucesys.sparkscope.listener.{AggregateMetrics, StageFailure}
import com.ucesys.sparkscope.common.{SparkScopeContext, SparkScopeLogger}
import com.ucesys.sparkscope.io.metrics.{MetricReaderFactory, MetricsLoaderFactory}
import com.ucesys.sparkscope.io.property.PropertiesLoaderFactory
import com.ucesys.sparkscope.timeline.{ExecutorTimeline, JobTimeline, StageTimeline}
import com.ucesys.sparkscope.view.ReportGeneratorFactory
import org.apache.spark.SparkConf
import org.apache.spark.scheduler._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class SparkScopeJobListener(sparkConf: SparkConf) extends SparkListener {

    implicit val logger = new SparkScopeLogger

    private var applicationStartEvent: Option[SparkListenerApplicationStart] = None
    protected val executorMap = new mutable.HashMap[String, ExecutorTimeline]()
    protected val jobMap = new mutable.HashMap[Long, JobTimeline]
    protected val stageMap = new mutable.HashMap[Int, StageTimeline]
    protected val stageIDToJobID = new mutable.HashMap[Int, Long]
    protected val failedStages = new ListBuffer[StageFailure]
    protected val appMetrics = new AggregateMetrics()

    private def appAggregateMetrics(): AggregateMetrics = appMetrics

    private def executorAggregateMetrics(executorID: String): Option[AggregateMetrics] = {
        executorMap.get(executorID).map(x => x.executorMetrics)
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

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        val taskMetrics = taskEnd.taskMetrics
        val taskInfo = taskEnd.taskInfo

        if (taskMetrics != null) {
            // UPDATE APP TASK METRICS
            appMetrics.update(taskMetrics, taskInfo)

            // UPDATE EXECUTOR AGG TASK METRICS
            executorMap.get(taskInfo.executorId).foreach(_.updateAggregateTaskMetrics(taskMetrics, taskInfo))

            // UPDATE STAGE AGG TASK METRICS
            stageMap.get(taskEnd.stageId).foreach { stageTimeSpan =>
                stageTimeSpan.updateAggregateTaskMetrics(taskMetrics, taskInfo)
                stageTimeSpan.updateTasks(taskInfo, taskMetrics)
            }

            // UPDATE JOB AGG TASK METRICS
            stageIDToJobID.get(taskEnd.stageId).foreach { jobID =>
                jobMap.get(jobID).foreach(_.updateAggregateTaskMetrics(taskMetrics, taskInfo))
            }
        }
    }

    override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
        applicationStartEvent = Some(applicationStart)
    }

    override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
        executorMap(executorAdded.executorId) = ExecutorTimeline(executorAdded)
    }

    override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
        executorMap(executorRemoved.executorId) =  executorMap(executorRemoved.executorId).end(executorRemoved)
    }

    override def onJobStart(jobStart: SparkListenerJobStart) {
        jobMap(jobStart.jobId) = JobTimeline(jobStart.jobId, jobStart.time)
        jobStart.stageIds.foreach(stageID => stageIDToJobID(stageID) = jobStart.jobId)
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
        val stageTimelineCompleted = stageMap(stageCompleted.stageInfo.stageId).end(stageCompleted)
        stageMap(stageCompleted.stageInfo.stageId) = stageTimelineCompleted

        if (stageCompleted.stageInfo.failureReason.nonEmpty) {
            val stageInfo = stageCompleted.stageInfo
            failedStages += StageFailure(stageInfo.stageId, stageInfo.attemptNumber, stageIDToJobID(stageInfo.stageId), stageInfo.numTasks)
        } else {
            stageIDToJobID.get(stageCompleted.stageInfo.stageId).foreach(jobID =>
                jobMap.get(jobID).foreach(_.addStage(stageTimelineCompleted))
            )
        }
    }
    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {

        jobMap.foreach { case(jobId, jobTimeline) => {
            if (jobTimeline.endTime.isEmpty) {
                if (jobTimeline.stageMap.nonEmpty) {
                    val lastStageEndTime: Long = jobTimeline.stageMap.map { case (_, stageTimeline) => stageTimeline.endTime.getOrElse(0L) }.max
                    jobMap(jobId) = jobMap(jobId).copy(endTime = Some(lastStageEndTime))
                } else {
                    jobMap(jobId) = jobMap(jobId).copy(endTime = Some(applicationEnd.time))
                }
            }
        }}

        val appStartEvent = applicationStartEvent.getOrElse(throw new IllegalArgumentException("App start event is empty"))

        val sparkScopeRunner = new SparkScopeRunner(
            SparkScopeContext(
                appId = appStartEvent.appId.getOrElse(throw new IllegalArgumentException("App Id is empty")),
                appStartTime = appStartEvent.time,
                appEndTime = Option(applicationEnd.time),
                executorMap = executorMap.toMap,
                stages = stageMap.values.toSeq
            ),
            sparkConf,
            new SparkScopeConfLoader,
            new SparkScopeAnalyzer,
            new PropertiesLoaderFactory,
            new MetricsLoaderFactory(new MetricReaderFactory(offline = false)),
            new ReportGeneratorFactory
        )
        sparkScopeRunner.run()
    }
}
