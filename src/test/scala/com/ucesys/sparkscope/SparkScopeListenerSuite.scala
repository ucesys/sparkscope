
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

import com.ucesys.sparkscope.common.AppContext
import com.ucesys.sparkscope.agg.TaskAggMetrics
import com.ucesys.sparkscope.timeline.{ExecutorTimeline, JobTimeline, StageTimeline}
import org.apache.spark.SparkConf
import org.apache.spark.executor.{ExecutorMetrics, InputMetrics, OutputMetrics, ShuffleReadMetrics, ShuffleWriteMetrics, TaskMetrics}
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerExecutorAdded, SparkListenerExecutorRemoved, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd, StageInfo, TaskInfo}
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.{FunSuite, GivenWhenThen}

import scala.collection.mutable

class SparkScopeListenerSuite extends FunSuite with GivenWhenThen with MockitoSugar {
    val executorId = "456"
    val executorAddTime = 1695358650003L
    val executorRemoveTime = 1695358660003L
    val executorHost = "host.com"
    val executorCores = 8

    val stageId = 99
    val stageTasks = 200
    val stageSubmissionTime = 1695358650001L
    val stageCompletionTime = 1695358650999L
    val stageInfo = new StageInfo(stageId, 0, "", stageTasks, Seq.empty, Seq.empty, "", null, Seq.empty, None, 0)
    stageInfo.submissionTime = Some(stageSubmissionTime)
    stageInfo.completionTime = Some(stageCompletionTime)

    val jobId = 123
    val jobStartTime = 1695358650000L
    val jobEndTime = 1695359990000L

    val appStart = SparkListenerApplicationStart("appName", Some("app-123"), 0, "user", Some("attempt-123"))
    val jobStart = SparkListenerJobStart(jobId, jobStartTime, Seq(stageInfo))
    val jobEnd = SparkListenerJobEnd(jobId, jobEndTime, null)
    val stageSubmitted = SparkListenerStageSubmitted(stageInfo)
    val stageCompleted = SparkListenerStageCompleted(stageInfo)
    val executorAdded = SparkListenerExecutorAdded(executorAddTime, executorId, new ExecutorInfo(executorHost, executorCores, null))
    val executorRemoved = SparkListenerExecutorRemoved(executorRemoveTime, executorId, "no reason")

    val taskEnd1 = {
        val taskMetrics = mock[TaskMetrics]
        doReturn(mock[ShuffleWriteMetrics]).when(taskMetrics).shuffleWriteMetrics
        doReturn(mock[ShuffleReadMetrics]).when(taskMetrics).shuffleReadMetrics
        doReturn(mock[InputMetrics]).when(taskMetrics).inputMetrics
        doReturn(mock[OutputMetrics]).when(taskMetrics).outputMetrics

        doReturn(0L).when(taskMetrics).resultSize
        doReturn(0L).when(taskMetrics).peakExecutionMemory
        doReturn(5000L).when(taskMetrics).executorCpuTime

        doReturn(1000L).when(taskMetrics).jvmGCTime
        doReturn(1000L).when(taskMetrics).diskBytesSpilled
        doReturn(1000L).when(taskMetrics).memoryBytesSpilled

        val taskInfo1 = mock[TaskInfo]
        doReturn(10000L).when(taskInfo1).duration

        SparkListenerTaskEnd(stageId, 999, "task type", null, taskInfo1, mock[ExecutorMetrics], taskMetrics)
    }

    val taskEnd2 = {
        val taskMetrics = mock[TaskMetrics]
        doReturn(mock[ShuffleWriteMetrics]).when(taskMetrics).shuffleWriteMetrics
        doReturn(mock[ShuffleReadMetrics]).when(taskMetrics).shuffleReadMetrics
        doReturn(mock[InputMetrics]).when(taskMetrics).inputMetrics
        doReturn(mock[OutputMetrics]).when(taskMetrics).outputMetrics

        doReturn(0L).when(taskMetrics).resultSize
        doReturn(0L).when(taskMetrics).peakExecutionMemory
        doReturn(5000L).when(taskMetrics).executorCpuTime

        doReturn(2000L).when(taskMetrics).jvmGCTime
        doReturn(1500L).when(taskMetrics).diskBytesSpilled
        doReturn(2000L).when(taskMetrics).memoryBytesSpilled

        val taskInfo2 = mock[TaskInfo]
        doReturn(15000L).when(taskInfo2).duration

        SparkListenerTaskEnd(stageId, 999, "task type", null, taskInfo2, mock[ExecutorMetrics], taskMetrics)
    }



    test("SparkScopeListener onTaskEnd") {
        Given("SparkScopeListener")
        val listener = new SparkScopeJobListener(new SparkConf)

        When("SparkScopeListener.onTaskEnd")
        listener.onTaskEnd(taskEnd1)
        listener.onTaskEnd(taskEnd2)

        Then("Metrics should be aggregated correctly")
        assert(listener.taskAggMetrics.jvmGCTime.sum == 3000L)
        assert(listener.taskAggMetrics.jvmGCTime.max == 2000L)
        assert(listener.taskAggMetrics.jvmGCTime.min == 1000L)
        assert(listener.taskAggMetrics.jvmGCTime.mean == 1500L)

        assert(listener.taskAggMetrics.diskBytesSpilled.sum == 2500L)
        assert(listener.taskAggMetrics.diskBytesSpilled.max == 1500L)
        assert(listener.taskAggMetrics.diskBytesSpilled.min == 1000L)
        assert(listener.taskAggMetrics.diskBytesSpilled.mean == 1250L)

        assert(listener.taskAggMetrics.memoryBytesSpilled.sum == 3000L)
        assert(listener.taskAggMetrics.jvmGCTime.max == 2000L)
        assert(listener.taskAggMetrics.jvmGCTime.min == 1000L)
        assert(listener.taskAggMetrics.jvmGCTime.mean == 1500L)
    }

    test("SparkScopeListener onTaskEnd taskMetrics=null") {
        Given("SparkScopeListener")
        val listener = new SparkScopeJobListener(new SparkConf)

        When("SparkScopeListener.onTaskEnd with null taskMetrics")
        listener.onTaskEnd(taskEnd1.copy(taskMetrics = null))

        Then("Metrics should be aggregated correctly")
        assert(listener.taskAggMetrics.jvmGCTime.sum == 0L)
        assert(listener.taskAggMetrics.jvmGCTime.max == 0L)
        assert(listener.taskAggMetrics.jvmGCTime.min == 0L)
        assert(listener.taskAggMetrics.jvmGCTime.mean == 0L)

        assert(listener.taskAggMetrics.diskBytesSpilled.sum == 0L)
        assert(listener.taskAggMetrics.diskBytesSpilled.max == 0L)
        assert(listener.taskAggMetrics.diskBytesSpilled.min == 0L)
        assert(listener.taskAggMetrics.diskBytesSpilled.mean == 0L)

        assert(listener.taskAggMetrics.memoryBytesSpilled.sum == 0L)
        assert(listener.taskAggMetrics.jvmGCTime.max == 0L)
        assert(listener.taskAggMetrics.jvmGCTime.min == 0L)
        assert(listener.taskAggMetrics.jvmGCTime.mean == 0L)

        And("Task duration should be non-zero")
        assert(listener.taskAggMetrics.taskDuration.sum == 10000L)
    }

    test("SparkScopeListener onTaskEnd taskInfo=null") {
        Given("SparkScopeListener")
        val listener = new SparkScopeJobListener(new SparkConf)

        When("SparkScopeListener.onTaskEnd with null taskInfo")
        listener.onTaskEnd(taskEnd1.copy(taskInfo = null))
        listener.onTaskEnd(taskEnd2.copy(taskInfo = null))

        Then("Task duration should be zero")
        assert(listener.taskAggMetrics.taskDuration.sum == 0L)

        And("Metrics should be aggregated correctly")
        assert(listener.taskAggMetrics.jvmGCTime.sum == 3000L)
        assert(listener.taskAggMetrics.jvmGCTime.max == 2000L)
        assert(listener.taskAggMetrics.jvmGCTime.min == 1000L)
        assert(listener.taskAggMetrics.jvmGCTime.mean == 1500L)

        assert(listener.taskAggMetrics.diskBytesSpilled.sum == 2500L)
        assert(listener.taskAggMetrics.diskBytesSpilled.max == 1500L)
        assert(listener.taskAggMetrics.diskBytesSpilled.min == 1000L)
        assert(listener.taskAggMetrics.diskBytesSpilled.mean == 1250L)

        assert(listener.taskAggMetrics.memoryBytesSpilled.sum == 3000L)
        assert(listener.taskAggMetrics.jvmGCTime.max == 2000L)
        assert(listener.taskAggMetrics.jvmGCTime.min == 1000L)
        assert(listener.taskAggMetrics.jvmGCTime.mean == 1500L)
    }

    test("SparkScopeListener onExecutorAdded") {
        Given("SparkScopeListener")
        val listener = new SparkScopeJobListener(new SparkConf)

        When("SparkScopeListener.onExecutorAdded")
        listener.onExecutorAdded(SparkListenerExecutorAdded(executorAddTime, executorId, new ExecutorInfo(executorHost, executorCores, null)))

        Then("ExecutorTimeline should be added to executorMap")
        assert(listener.executorMap.size == 1)
        assert(listener.executorMap.contains(executorId))
        assert(listener.executorMap(executorId).executorId == executorId)
        assert(listener.executorMap(executorId).startTime == executorAddTime)
        assert(listener.executorMap(executorId).cores == executorCores)
        assert(listener.executorMap(executorId).hostId == executorHost)

        And("endTime should be empty")
        assert(listener.executorMap(executorId).endTime.isEmpty)
    }

    test("SparkScopeListener onExecutorRemoved") {
        Given("SparkScopeListener")
        val listener = new SparkScopeJobListener(new SparkConf)

        And("executorMap contains ExecutorTimeline")
        listener.executorMap(executorId) = ExecutorTimeline(executorId, executorHost, executorCores, executorAddTime, Some(executorRemoveTime))

        When("SparkScopeListener.onExecutorRemoved")
        listener.onExecutorRemoved(SparkListenerExecutorRemoved(executorRemoveTime, executorId, "no reason"))

        Then("ExecutorTimeline.endTime should be updated")
        assert(listener.executorMap.size == 1)
        assert(listener.executorMap.contains(executorId))
        assert(listener.executorMap(executorId).endTime.get == executorRemoveTime)

        And("Other executorTimeline fields should not be changed")
        assert(listener.executorMap(executorId).executorId == executorId)
        assert(listener.executorMap(executorId).startTime == executorAddTime)
        assert(listener.executorMap(executorId).cores == executorCores)
        assert(listener.executorMap(executorId).hostId == executorHost)
    }

    test("SparkScopeListener onStageSubmitted") {
        Given("SparkScopeListener")
        val listener = new SparkScopeJobListener(new SparkConf)

        When("SparkScopeListener.onStageSubmitted")
        listener.onStageSubmitted(SparkListenerStageSubmitted(stageInfo))

        Then("StageTimeline should be added to stageMap")
        assert(listener.stageMap.size == 1)
        assert(listener.stageMap.contains(stageId))
        assert(listener.stageMap(stageId).numberOfTasks == stageTasks)
        assert(listener.stageMap(stageId).startTime.get == stageSubmissionTime)

        And("endTime should be empty")
        assert(listener.stageMap(stageId).endTime.isEmpty)
    }

    test("SparkScopeListener onStageCompleted") {
        Given("SparkScopeListener")
        val listener = new SparkScopeJobListener(new SparkConf)

        And("stageMap containing unfinished stage")
        listener.stageMap(stageId) = StageTimeline(stageId, Some(stageSubmissionTime), stageTasks, Seq(999))

        When("SparkScopeListener.onStageCompleted")
        listener.onStageCompleted(SparkListenerStageCompleted(stageInfo))

        Then("StageTimeline.endTime should be updated")
        assert(listener.stageMap.size == 1)
        assert(listener.stageMap.contains(stageId))
        assert(listener.stageMap(stageId).endTime.get == stageCompletionTime)

        And("Other StageTimeline fields should not be changed")
        assert(listener.stageMap(stageId).numberOfTasks == stageTasks)
        assert(listener.stageMap(stageId).startTime.get == stageSubmissionTime)
    }

    test("SparkScopeListener onJobStart") {
        Given("SparkScopeListener")
        val listener = new SparkScopeJobListener(new SparkConf)

        When("SparkScopeListener.onJobStart")
        listener.onJobStart(jobStart)

        Then("jobTimeline should be added to jobMap")
        assert(listener.jobMap.size == 1)
        assert(listener.jobMap.contains(jobId))
        assert(listener.jobMap(jobId).jobID == jobId)
        assert(listener.jobMap(jobId).startTime == jobStartTime)
    }

    test("SparkScopeListener onJobEnd") {
        Given("SparkScopeListener")
        val listener = new SparkScopeJobListener(new SparkConf)

        And("JobTimeline in jobMap")
        listener.jobMap(jobId) = JobTimeline(jobStart)

        When("SparkScopeListener.onJobEnd")
        listener.onJobEnd(jobEnd)

        Then("jobTimeline.endTIme should be updated")
        assert(listener.jobMap.size == 1)
        assert(listener.jobMap.contains(jobId))
        assert(listener.jobMap(jobId).endTime.get == jobEndTime)

        And("Other JobTimeline fields should not be changed")
        assert(listener.jobMap(jobId).jobID == jobId)
        assert(listener.jobMap(jobId).startTime == jobStartTime)
    }

    test("SparkScopeListener onApplicationStart") {
        Given("SparkScopeListener")
        val listener = new SparkScopeJobListener(new SparkConf)

        When("SparkScopeListener.onApplicationStart")
        listener.onApplicationStart(mock[SparkListenerApplicationStart])

        Then("applicationStartEvent should be set")
        assert(listener.applicationStartEvent.nonEmpty)
    }

    test("SparkScopeListener onApplicationEnd") {
        Given("SparkScopeListener")
        val listenerMock = mock[SparkScopeJobListener]
        val runnerMock = mock[SparkScopeRunner]
        doCallRealMethod.when(listenerMock).onApplicationEnd(any[SparkListenerApplicationEnd])
        doCallRealMethod.when(listenerMock).runSparkScopeAnalysis(any[Option[Long]])
        doReturn(new mutable.HashMap[String, ExecutorTimeline]).when(listenerMock).executorMap
        doReturn(new mutable.HashMap[String, StageTimeline]).when(listenerMock).stageMap
        doReturn(new SparkConf).when(listenerMock).sparkConf
        doReturn(runnerMock).when(listenerMock).runner

        And("applicationStartEvent is set")
        doReturn(Some(appStart)).when(listenerMock).applicationStartEvent

        When("SparkScopeListener.onApplicationEnd")
        listenerMock.onApplicationEnd(mock[SparkListenerApplicationEnd])

        Then("sparkScopeRunner.run should be called")
        verify(runnerMock, times(1)).run(any[AppContext], any[SparkConf], any[TaskAggMetrics])
    }

    test("SparkScopeListener onApplicationEnd exception") {
        Given("SparkScopeListener")
        val listener = new SparkScopeJobListener(new SparkConf)

        And("applicationStartEvent is not set")
        listener.applicationStartEvent = None

        When("SparkScopeListener.onApplicationEnd")
        Then("IllegalArgumentException should be thrown")
        assertThrows[IllegalArgumentException] {
            listener.onApplicationEnd(mock[SparkListenerApplicationEnd])
        }
    }

    test("SparkScopeListener integration test") {
        Given("SparkScopeListener")
        val runnerMock = mock[SparkScopeRunner]
        val listener = new SparkScopeJobListener(new SparkConf, runnerMock)

        When("Spark Application runs and completes")
        listener.onApplicationStart(appStart)
        listener.onExecutorAdded(executorAdded)
        listener.onJobStart(jobStart)
        listener.onStageSubmitted(stageSubmitted)
        listener.onTaskEnd(taskEnd1)
        listener.onTaskEnd(taskEnd2)
        listener.onStageCompleted(stageCompleted)
        listener.onJobEnd(jobEnd)
        listener.onExecutorRemoved(executorRemoved)
        listener.onApplicationEnd(mock[SparkListenerApplicationEnd])

        Then("applicationStartEvent should be set")
        assert(listener.applicationStartEvent.nonEmpty)

        And("ExecutorTimeline should be added to executorMap")
        assert(listener.executorMap.size == 1)
        assert(listener.executorMap.contains(executorId))
        assert(listener.executorMap(executorId).endTime.get == executorRemoveTime)
        assert(listener.executorMap(executorId).executorId == executorId)
        assert(listener.executorMap(executorId).startTime == executorAddTime)
        assert(listener.executorMap(executorId).cores == executorCores)
        assert(listener.executorMap(executorId).hostId == executorHost)

        And("jobTimeline should be added to jobMap")
        assert(listener.jobMap.size == 1)
        assert(listener.jobMap.contains(jobId))
        assert(listener.jobMap(jobId).endTime.get == jobEndTime)
        assert(listener.jobMap(jobId).jobID == jobId)
        assert(listener.jobMap(jobId).startTime == jobStartTime)

        And("StageTimeline should be added to stageMap")
        assert(listener.stageMap.size == 1)
        assert(listener.stageMap.contains(stageId))
        assert(listener.stageMap(stageId).endTime.get == stageCompletionTime)
        assert(listener.stageMap(stageId).numberOfTasks == stageTasks)
        assert(listener.stageMap(stageId).startTime.get == stageSubmissionTime)

        And("And metrics should be aggregated correctly")
        assert(listener.taskAggMetrics.jvmGCTime.sum == 3000L)
        assert(listener.taskAggMetrics.jvmGCTime.max == 2000L)
        assert(listener.taskAggMetrics.jvmGCTime.min == 1000L)
        assert(listener.taskAggMetrics.jvmGCTime.mean == 1500L)

        assert(listener.taskAggMetrics.diskBytesSpilled.sum == 2500L)
        assert(listener.taskAggMetrics.diskBytesSpilled.max == 1500L)
        assert(listener.taskAggMetrics.diskBytesSpilled.min == 1000L)
        assert(listener.taskAggMetrics.diskBytesSpilled.mean == 1250L)

        assert(listener.taskAggMetrics.memoryBytesSpilled.sum == 3000L)
        assert(listener.taskAggMetrics.memoryBytesSpilled.max == 2000L)
        assert(listener.taskAggMetrics.memoryBytesSpilled.min == 1000L)
        assert(listener.taskAggMetrics.memoryBytesSpilled.mean == 1500L)

        Then("sparkScopeRunner.run should be called")
        verify(runnerMock, times(1)).run(any[AppContext], any[SparkConf], any[TaskAggMetrics])
    }
}

