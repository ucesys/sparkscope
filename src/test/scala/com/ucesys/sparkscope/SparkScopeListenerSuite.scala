
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
    val jobStart = SparkListenerJobStart(jobId, jobStartTime, Seq(stageInfo))
    val jobEnd = SparkListenerJobEnd(jobId, jobEndTime, null)


    test("SparkScopeListener onTaskEnd") {
        Given("SparkScopeListener")
        val listener = new SparkScopeJobListener(new SparkConf)

        And("task info")
        val taskInfo1 = mock[TaskInfo]
        doReturn(10000L).when(taskInfo1).duration

        And("task metrics")
        val taskMetrics1 = mock[TaskMetrics]
        doReturn(mock[ShuffleWriteMetrics]).when(taskMetrics1).shuffleWriteMetrics
        doReturn(mock[ShuffleReadMetrics]).when(taskMetrics1).shuffleReadMetrics
        doReturn(mock[InputMetrics]).when(taskMetrics1).inputMetrics
        doReturn(mock[OutputMetrics]).when(taskMetrics1).outputMetrics
        doReturn(0L).when(taskMetrics1).resultSize
        doReturn(0L).when(taskMetrics1).peakExecutionMemory
        doReturn(5000L).when(taskMetrics1).executorCpuTime

        doReturn(1000L).when(taskMetrics1).jvmGCTime
        doReturn(1000L).when(taskMetrics1).diskBytesSpilled
        doReturn(1000L).when(taskMetrics1).memoryBytesSpilled

        val taskInfo2 = mock[TaskInfo]
        doReturn(15000L).when(taskInfo2).duration

        val taskMetrics2 = mock[TaskMetrics]
        doReturn(mock[ShuffleWriteMetrics]).when(taskMetrics2).shuffleWriteMetrics
        doReturn(mock[ShuffleReadMetrics]).when(taskMetrics2).shuffleReadMetrics
        doReturn(mock[InputMetrics]).when(taskMetrics2).inputMetrics
        doReturn(mock[OutputMetrics]).when(taskMetrics2).outputMetrics
        doReturn(0L).when(taskMetrics2).resultSize
        doReturn(0L).when(taskMetrics2).peakExecutionMemory
        doReturn(5000L).when(taskMetrics2).executorCpuTime

        doReturn(2000L).when(taskMetrics2).jvmGCTime
        doReturn(1500L).when(taskMetrics2).diskBytesSpilled
        doReturn(2000L).when(taskMetrics2).memoryBytesSpilled

        val executorMetrics = mock[ExecutorMetrics]

        When("SparkScopeListener.onTaskEnd")
        listener.onTaskEnd(SparkListenerTaskEnd(stageId, 999, "task type", null, taskInfo1, executorMetrics, taskMetrics1))
        listener.onTaskEnd(SparkListenerTaskEnd(stageId, 999, "task type", null, taskInfo2, executorMetrics, taskMetrics2))

        Then("Metrics should be aggregated correctly")
        assert(listener.appMetrics.jvmGCTime.sum == 3000L)
        assert(listener.appMetrics.jvmGCTime.max == 2000L)
        assert(listener.appMetrics.jvmGCTime.min == 1000L)
        assert(listener.appMetrics.jvmGCTime.mean == 1500L)

        assert(listener.appMetrics.diskBytesSpilled.sum == 2500L)
        assert(listener.appMetrics.diskBytesSpilled.max == 1500L)
        assert(listener.appMetrics.diskBytesSpilled.min == 1000L)
        assert(listener.appMetrics.diskBytesSpilled.mean == 1250L)

        assert(listener.appMetrics.memoryBytesSpilled.sum == 3000L)
        assert(listener.appMetrics.jvmGCTime.max == 2000L)
        assert(listener.appMetrics.jvmGCTime.min == 1000L)
        assert(listener.appMetrics.jvmGCTime.mean == 1500L)
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
        doReturn(new mutable.HashMap[String, ExecutorTimeline]).when(listenerMock).executorMap
        doReturn(new mutable.HashMap[String, StageTimeline]).when(listenerMock).stageMap
        doReturn(runnerMock).when(listenerMock).sparkScopeRunner

        And("applicationStartEvent is set")
        val appStart = SparkListenerApplicationStart("appName", Some("app-123"), 0, "user", Some("attempt-123"))
        doReturn(Some(appStart)).when(listenerMock).applicationStartEvent

        When("SparkScopeListener.onApplicationEnd")
        listenerMock.onApplicationEnd(mock[SparkListenerApplicationEnd])

        Then("sparkScopeRunner.run should be called")
        verify(runnerMock, times(1)).run(any[AppContext])
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
}

