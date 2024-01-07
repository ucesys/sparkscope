
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

import com.ucesys.sparkscope.SparkScopeConfLoader._
import com.ucesys.sparkscope.TestHelpers.stageTimeline
import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.io.reader.FileReaderFactory
import org.apache.spark.SparkConf
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerEnvironmentUpdate, SparkListenerExecutorAdded, SparkListenerExecutorRemoved, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerStageSubmitted, SparkListenerTaskEnd}
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.{FunSuite, GivenWhenThen}

import java.nio.file.NoSuchFileException


class EventLogRunnerSuite extends FunSuite with GivenWhenThen with MockitoSugar {
    implicit val logger: SparkScopeLogger = new SparkScopeLogger

    test("EventLogRunner finished") {
        Given("Path to eventLog with finished application and no removed executors")
        val appId = "app-20231025121456-0004-eventLog-finished"
        val eventLogPath = s"src/test/resources/eventlog/${appId}"
        val listener = mock[SparkScopeJobListener]
        val runner = new EventLogRunner(listener)

        When("EventLogRunner.run")
        runner.run(new FileReaderFactory, SparkScopeArgs(eventLogPath, None, None, None))

        Then("EventLogRunner should call Spark listener events")
        verify(listener, times(1)).onEnvironmentUpdate(any[SparkListenerEnvironmentUpdate])
        verify(listener, times(1)).onApplicationStart(any[SparkListenerApplicationStart])
        verify(listener, times(2)).onExecutorAdded(any[SparkListenerExecutorAdded])
        verify(listener, times(1)).onJobStart(any[SparkListenerJobStart])
        verify(listener, times(1)).onStageSubmitted(any[SparkListenerStageSubmitted])
        verify(listener, times(1)).onTaskEnd(any[SparkListenerTaskEnd])
        verify(listener, times(1)).onStageCompleted(any[SparkListenerStageCompleted])
        verify(listener, times(1)).onJobEnd(any[SparkListenerJobEnd])
        verify(listener, times(0)).onExecutorRemoved(any[SparkListenerExecutorRemoved])
        verify(listener, times(1)).runSparkScopeAnalysis(any[Option[Long]])
    }

    test("EventLogRunner running") {
        Given("Path to eventLog with running application and no removed executors")
        val appId = "app-20231025121456-0004-eventLog-running"
        val eventLogPath = s"src/test/resources/eventlog/${appId}"
        val listener = mock[SparkScopeJobListener]
        val runner = new EventLogRunner(listener)

        When("EventLogRunner.load")
        runner.run(new FileReaderFactory, SparkScopeArgs(eventLogPath, None, None, None))

        Then("EventLogRunner should call Spark listener events")
        verify(listener, times(1)).onEnvironmentUpdate(any[SparkListenerEnvironmentUpdate])
        verify(listener, times(1)).onApplicationStart(any[SparkListenerApplicationStart])
        verify(listener, times(2)).onExecutorAdded(any[SparkListenerExecutorAdded])
        verify(listener, times(1)).onJobStart(any[SparkListenerJobStart])
        verify(listener, times(1)).onStageSubmitted(any[SparkListenerStageSubmitted])
        verify(listener, times(1)).onTaskEnd(any[SparkListenerTaskEnd])
        verify(listener, times(1)).onStageCompleted(any[SparkListenerStageCompleted])
        verify(listener, times(1)).onJobEnd(any[SparkListenerJobEnd])
        verify(listener, times(0)).onExecutorRemoved(any[SparkListenerExecutorRemoved])
        verify(listener, times(1)).runSparkScopeAnalysis(None)
    }

    test("EventLogRunner finished executors removed") {
        Given("Path to eventLog with finished application and removed executors")
        val appId = "app-20231025121456-0004-eventLog-finished-exec-removed"
        val eventLogPath = s"src/test/resources/eventlog/${appId}"
        val listener = mock[SparkScopeJobListener]
        val runner = new EventLogRunner(listener)

        When("EventLogRunner.load")
        runner.run(new FileReaderFactory, SparkScopeArgs(eventLogPath, None, None, None))

        Then("EventLogRunner should call Spark listener events")
        verify(listener, times(1)).onEnvironmentUpdate(any[SparkListenerEnvironmentUpdate])
        verify(listener, times(1)).onApplicationStart(any[SparkListenerApplicationStart])
        verify(listener, times(2)).onExecutorAdded(any[SparkListenerExecutorAdded])
        verify(listener, times(1)).onJobStart(any[SparkListenerJobStart])
        verify(listener, times(1)).onStageSubmitted(any[SparkListenerStageSubmitted])
        verify(listener, times(2)).onTaskEnd(any[SparkListenerTaskEnd])
        verify(listener, times(1)).onStageCompleted(any[SparkListenerStageCompleted])
        verify(listener, times(1)).onJobEnd(any[SparkListenerJobEnd])
        verify(listener, times(2)).onExecutorRemoved(any[SparkListenerExecutorRemoved])
        verify(listener, times(1)).runSparkScopeAnalysis(any[Option[Long]])
    }

    test("EventLogRunner running executors removed") {
        Given("Path to eventLog with running application and removed executors")
        val appId = "app-20231025121456-0004-eventLog-running-exec-removed"
        val eventLogPath = s"src/test/resources/eventlog/${appId}"
        val listener = mock[SparkScopeJobListener]
        val runner = new EventLogRunner(listener)

        When("EventLogRunner.load")
        runner.run(new FileReaderFactory, SparkScopeArgs(eventLogPath, None, None, None))

        Then("EventLogRunner should call Spark listener events")
        verify(listener, times(1)).onEnvironmentUpdate(any[SparkListenerEnvironmentUpdate])
        verify(listener, times(1)).onApplicationStart(any[SparkListenerApplicationStart])
        verify(listener, times(2)).onExecutorAdded(any[SparkListenerExecutorAdded])
        verify(listener, times(1)).onJobStart(any[SparkListenerJobStart])
        verify(listener, times(1)).onStageSubmitted(any[SparkListenerStageSubmitted])
        verify(listener, times(1)).onTaskEnd(any[SparkListenerTaskEnd])
        verify(listener, times(1)).onStageCompleted(any[SparkListenerStageCompleted])
        verify(listener, times(1)).onJobEnd(any[SparkListenerJobEnd])
        verify(listener, times(2)).onExecutorRemoved(any[SparkListenerExecutorRemoved])
        verify(listener, times(0)).onApplicationEnd(any[SparkListenerApplicationEnd])
    }

    test("EventLogRunner running incomplete eventlog") {
        Given("Path to eventLog with running application and removed executors")
        val appId = "app-20231025121456-0004-eventLog-running-incomplete"
        val eventLogPath = s"src/test/resources/eventlog/${appId}"
        val listener = mock[SparkScopeJobListener]
        val runner = new EventLogRunner(listener)

        When("EventLogRunner.load")
        runner.run(new FileReaderFactory, SparkScopeArgs(eventLogPath, None, None, None))

        Then("EventLogRunner should call Spark listener events")
        verify(listener, times(1)).onEnvironmentUpdate(any[SparkListenerEnvironmentUpdate])
        verify(listener, times(1)).onApplicationStart(any[SparkListenerApplicationStart])
        verify(listener, times(2)).onExecutorAdded(any[SparkListenerExecutorAdded])
        verify(listener, times(1)).onJobStart(any[SparkListenerJobStart])
        verify(listener, times(1)).onStageSubmitted(any[SparkListenerStageSubmitted])
        verify(listener, times(1)).onTaskEnd(any[SparkListenerTaskEnd])
        verify(listener, times(1)).onStageCompleted(any[SparkListenerStageCompleted])
        verify(listener, times(0)).onJobEnd(any[SparkListenerJobEnd])
        verify(listener, times(0)).onExecutorRemoved(any[SparkListenerExecutorRemoved])
        verify(listener, times(0)).onApplicationEnd(any[SparkListenerApplicationEnd])
    }

    test("EventLogRunner task metrics test") {
        Given("Overriden driverMetrics, executorMetrics and htmlPath args")
        val appId = "app-20231025121456-0004-eventLog-finished-exec-removed"
        val args = SparkScopeArgs(
            eventLog = s"src/test/resources/eventlog/${appId}",
            driverMetrics = Some("overrriden/path/to/driver/metrics"),
            executorMetrics = Some("overrriden/path/to/executor/metrics")
        )
        val listener = new SparkScopeJobListener(new SparkConf)
        val runner = new EventLogRunner(listener)

        When("EventLogRunner.load")
        runner.run(new FileReaderFactory, args)

        Then("task metrics should be aggregated")
        assert(listener.taskAggMetrics.jvmGCTime.count == 2)
        assert(listener.taskAggMetrics.jvmGCTime.sum == 11L)
        assert(listener.taskAggMetrics.jvmGCTime.max == 11L)
        assert(listener.taskAggMetrics.jvmGCTime.min == 0L)
        assert(listener.taskAggMetrics.jvmGCTime.mean == 5.5)

        assert(listener.taskAggMetrics.diskBytesSpilled.count == 2)
        assert(listener.taskAggMetrics.diskBytesSpilled.sum == 0L)
        assert(listener.taskAggMetrics.diskBytesSpilled.max == 0L)
        assert(listener.taskAggMetrics.diskBytesSpilled.min == 0L)
        assert(listener.taskAggMetrics.diskBytesSpilled.mean == 0)

        assert(listener.taskAggMetrics.memoryBytesSpilled.count == 2)
        assert(listener.taskAggMetrics.memoryBytesSpilled.sum == 0L)
        assert(listener.taskAggMetrics.memoryBytesSpilled.max == 0L)
        assert(listener.taskAggMetrics.memoryBytesSpilled.min == 0L)
        assert(listener.taskAggMetrics.memoryBytesSpilled.mean == 0)
    }

    test("EventLogRunner parse args test") {
        Given("Overriden driverMetrics, executorMetrics and htmlPath args")
        val appId = "app-20231025121456-0004-eventLog-finished-exec-removed"
        val args = SparkScopeArgs(
            eventLog = s"src/test/resources/eventlog/${appId}",
            driverMetrics = Some("overrriden/path/to/driver/metrics"),
            executorMetrics = Some("overrriden/path/to/executor/metrics"),
            htmlPath = Some("overrriden/path/to/html/report"),
        )
        val listener = new SparkScopeJobListener(new SparkConf)
        val runner = new EventLogRunner(listener)

        When("EventLogRunner.load")
        runner.run(new FileReaderFactory, args)

        Then("SparkConf should be read from env update event")
        assertSparkConf(listener.sparkConf)

        And("passed args should be overriden")
        assert(listener.sparkConf.get(SparkScopePropertyDriverMetricsDir) == "overrriden/path/to/driver/metrics")
        assert(listener.sparkConf.get(SparkScopePropertyExecutorMetricsDir) == "overrriden/path/to/executor/metrics")
        assert(listener.sparkConf.get(SparkScopePropertyHtmlPath) == "overrriden/path/to/html/report")
    }

    test("EventLogRunner finished stages test") {
        Given("Path to eventLog with finished application with stage events")
        val appId = "app-20231122115433-0000-eventLog-finished-stages"
        val eventLogPath = s"src/test/resources/eventlog/${appId}"
        val listener = new SparkScopeJobListener(new SparkConf)
        val runner = new EventLogRunner(listener)

        When("EventLogRunner.load")
        runner.run(new FileReaderFactory, SparkScopeArgs(eventLogPath, None, None, None))

        Then("StageTimelines should be created from Stage events")
        assert(listener.stageMap.size == 10)
        assert(listener.stageMap(0) == stageTimeline(0, 1700654082972L, 1700654087848L, 2))
        assert(listener.stageMap(1) == stageTimeline(1, 1700654088379L, 1700654089900L, 2))
        assert(listener.stageMap(3) == stageTimeline(3, 1700654090188L, 1700654093628L, 20, Seq(2)))
        assert(listener.stageMap(6) == stageTimeline(6, 1700654093945L, 1700654094398L, 1, Seq(5)))
        assert(listener.stageMap(7) == stageTimeline(7, 1700654094935L, 1700654096905L, 2))
        assert(listener.stageMap(8) == stageTimeline(8, 1700654094986L, 1700654095595L, 4))
        assert(listener.stageMap(10) == stageTimeline(10, 1700654096963L, 1700654099016L, 20, Seq(9)))
        assert(listener.stageMap(13) == stageTimeline(13, 1700654099238L, 1700654099521L, 1, Seq(12)))
        assert(listener.stageMap(16) == stageTimeline(16, 1700654099596L, 1700654099829L, 1, Seq(15)))
        assert(listener.stageMap(20) == stageTimeline(20, 1700654099981L, 1700654100329L, 1, Seq(19)))
    }

    test("EventLogRunner running stages not completed test") {
        Given("Path to eventLog with running application with incomplete stage events")
        val appId = "app-20231122115433-0000-eventLog-running-stages"
        val eventLogPath = s"src/test/resources/eventlog/${appId}"
        val listener = new SparkScopeJobListener(new SparkConf)
        val runner = new EventLogRunner(listener)

        When("EventLogRunner.load")
        runner.run(new FileReaderFactory, SparkScopeArgs(eventLogPath, None, None, None))

        Then("StageTimelines should be created from Stage events")
        assert(listener.stageMap.size == 10)
        assert(listener.stageMap(0) == stageTimeline(0, 1700654082972L, 1700654087848L, 2))
        assert(listener.stageMap(1) == stageTimeline(1, 1700654088379L, 1700654089900L, 2))
        assert(listener.stageMap(3) == stageTimeline(3, 1700654090188L, 1700654093628L, 20, Seq(2)))
        assert(listener.stageMap(6) == stageTimeline(6, 1700654093945L, 1700654094398L, 1, Seq(5)))
        assert(listener.stageMap(7) == stageTimeline(7, 1700654094935L, 1700654096905L, 2))
        assert(listener.stageMap(8) == stageTimeline(8, 1700654094986L, 1700654095595L, 4))
        assert(listener.stageMap(10) == stageTimeline(10, 1700654096963L, 1700654099016L, 20, Seq(9)))
        assert(listener.stageMap(13) == stageTimeline(13, 1700654099238L, 1700654099521L, 1, Seq(12)))
        assert(listener.stageMap(16) == stageTimeline(16, 1700654099596L, 1700654099829L, 1, Seq(15)))

        And("Incomplete stages should have empty endTime")
        assert(listener.stageMap(20).endTime.isEmpty)
    }

    test("EventLogRunner corrupted events test") {
        Given("Path to eventLog with corrupted executor added/removed and stage submitted/completed events")
        val appId = "app-20231122115433-0000-eventLog-corrupted-events"
        val eventLogPath = s"src/test/resources/eventlog/${appId}"
        val listener = new SparkScopeJobListener(new SparkConf)
        val runner = new EventLogRunner(listener)

        When("EventLogRunner.load")
        runner.run(new FileReaderFactory, SparkScopeArgs(eventLogPath, None, None, None))

        And("Corrupted stage events should be ignored")
        And("Stages be read from stage submitted/completed events")
        assert(listener.stageMap.size == 9)
        assert(listener.stageMap(0) == stageTimeline(0, 1700654082972L, 1700654087848L, 2))
        assert(listener.stageMap(1) == stageTimeline(1, 1700654088379L, 1700654089900L, 2))
        assert(listener.stageMap(3) == stageTimeline(3, 1700654090188L, 1700654093628L, 20, Seq(2)))
        assert(listener.stageMap(6) == stageTimeline(6, 1700654093945L, 1700654094398L, 1, Seq(5)))
        assert(listener.stageMap(7) == stageTimeline(7, 1700654094935L, 1700654096905L, 2))
        assert(listener.stageMap(8) == stageTimeline(8, 1700654094986L, 1700654095595L, 4))
        assert(listener.stageMap(10) == stageTimeline(10, 1700654096963L, 1700654099016L, 20, Seq(9)))
        assert(listener.stageMap(13) == stageTimeline(13, 1700654099238L, 1700654099521L, 1, Seq(12)))

        And("Corrupted executor events should be ignored")
        And("Executor timeline should be read from executor add/remove events")
        assert(listener.executorMap.size == 1)
        assert(listener.executorMap("1").startTime == 1700654079478L)
        assert(listener.executorMap("1").endTime.isEmpty)
        assert(listener.executorMap("1").cores == 2)
    }

    test("EventLogRunner bad path to eventLog") {
        Given("Bad path to eventLog")
        val eventLogPath = s"bad/path/to/event/log"
        val runner = new EventLogRunner(mock[SparkScopeJobListener])

        When("EventLogRunner.load")
        Then("NoSuchFileException should be thrown")
        assertThrows[NoSuchFileException] {
            runner.run(new FileReaderFactory, SparkScopeArgs(eventLogPath, None, None, None))
        }
    }

    test("EventLogRunner bad eventLog, no app start event") {
        Given("Path to bad eventLog, no app start event")
        val appId = "app-20231025121456-0004-eventLog-error-no-app-start"
        val eventLogPath = s"src/test/resources/eventlog/${appId}"
        val listener = new SparkScopeJobListener(new SparkConf)
        val runner = new EventLogRunner(listener)

        When("EventLogRunner.load")
        Then("IllegalArgumentException should be thrown")
        assertThrows[IllegalArgumentException] {
            runner.run(new FileReaderFactory, SparkScopeArgs(eventLogPath, None, None, None))
        }
    }

    test("EventLogRunner bad eventLog, no env update event") {
        Given("Path to bad eventLog, no env update event")
        val appId = "app-20231025121456-0004-eventLog-error-no-env-update"
        val eventLogPath = s"src/test/resources/eventlog/${appId}"
        val runner = new EventLogRunner(mock[SparkScopeJobListener])

        When("EventLogRunner.load")
        Then("IllegalArgumentException should be thrown")
        assertThrows[IllegalArgumentException] {
            runner.run(new FileReaderFactory, SparkScopeArgs(eventLogPath, None, None, None))
        }
    }

    test("EventLogRunner bad eventLog, bad app start event") {
        Given("Path to bad eventLog, no app start event")
        val appId = "app-20231025121456-0004-eventLog-error-bad-app-start"
        val eventLogPath = s"src/test/resources/eventlog/${appId}"
        val listener = new SparkScopeJobListener(new SparkConf)
        val runner = new EventLogRunner(listener)

        When("EventLogRunner.load")
        Then("IllegalArgumentException should be thrown")
        assertThrows[IllegalArgumentException] {
            runner.run(new FileReaderFactory, SparkScopeArgs(eventLogPath, None, None, None))
        }
    }

    test("EventLogRunner bad eventLog, bad update event") {
        Given("Path to bad eventLog, no env update event")
        val appId = "app-20231025121456-0004-eventLog-error-bad-env-update"
        val eventLogPath = s"src/test/resources/eventlog/${appId}"
        val runner = new EventLogRunner(mock[SparkScopeJobListener])

        When("EventLogRunner.load")
        Then("IllegalArgumentException should be thrown")
        assertThrows[IllegalArgumentException] {
            runner.run(new FileReaderFactory, SparkScopeArgs(eventLogPath, None, None, None))
        }
    }

    def assertSparkConf(sparkConf: SparkConf): Unit = {
        assert(sparkConf.get("spark.eventLog.enabled") == "true")
        assert(sparkConf.get("spark.executor.memory") == "900m")
        assert(sparkConf.get("spark.app.startTime") == "1698236095722")
        assert(sparkConf.get("spark.executor.id") == "driver")
        assert(sparkConf.get("spark.jars") == "file:///tmp/jars/sparkscope-spark3-0.1.1-SNAPSHOT.jar,file:/tmp/jars/spark-examples_2.10-1.1.1.jar")
        assert(sparkConf.get("spark.executor.cores") == "2")
        assert(sparkConf.get("spark.eventLog.dir") == "/tmp/spark-events")
        assert(sparkConf.get("spark.app.id") == "app-20231025121456-0004")
        assert(sparkConf.get("spark.metrics.conf") == "path/to/metrics.properties")
        assert(sparkConf.get("spark.driver.port") == "40457")
        assert(sparkConf.get("spark.master") == "spark://spark-master:7077")
        assert(sparkConf.get("spark.extraListeners") == "com.ucesys.sparkscope.SparkScopeJobListener")
        assert(sparkConf.get("spark.executor.instances") == "2")
        assert(sparkConf.get("spark.app.name") == "Spark Pi")
    }
}

