
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

package com.ucesys.sparkscope.event
import com.ucesys.sparkscope.SparkScopeArgs
import com.ucesys.sparkscope.SparkScopeConfLoader._
import com.ucesys.sparkscope.TestHelpers.stageTimeline
import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.io.file.FileReaderFactory
import com.ucesys.sparkscope.io.metrics.MetricReaderFactory
import com.ucesys.sparkscope.timeline.StageTimeline
import org.apache.spark.SparkConf
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSuite, GivenWhenThen}

import java.nio.file.NoSuchFileException


class EventLogContextLoaderSuite extends FunSuite with MockFactory with GivenWhenThen {
    implicit val logger: SparkScopeLogger = new SparkScopeLogger

    test("EventLogContextLoader finished") {
        Given("Path to eventLog with finished application and no removed executors")
        val appId = "app-20231025121456-0004-eventLog-finished"
        val eventLogPath = s"src/test/resources/${appId}"
        val eventLogContextLoader = new EventLogContextLoader

        When("EventLogContextLoader.load")
        val eventLogContext = eventLogContextLoader.load(new FileReaderFactory, SparkScopeArgs(eventLogPath, None, None, None))

        Then("App id, startTime, endTime should be read from app start/end events")
        assert(eventLogContext.appContext.appId == appId)
        assert(eventLogContext.appContext.appStartTime == 1698236095722L)
        assert(eventLogContext.appContext.appEndTime.get == 1698236104099L)

        And("Executor timeline should be read from executor add/remove events")
        assert(eventLogContext.appContext.executorMap.size == 2)
        assert(eventLogContext.appContext.executorMap("0").getStartTime.get == 1698236098507L)
        assert(eventLogContext.appContext.executorMap("0").getEndTime.isEmpty)
        assert(eventLogContext.appContext.executorMap("0").cores == 2)
        assert(eventLogContext.appContext.executorMap("1").getStartTime.get == 1698236098540L)
        assert(eventLogContext.appContext.executorMap("1").getEndTime.isEmpty)
        assert(eventLogContext.appContext.executorMap("1").cores == 2)

        And("SparkConf should be read from env update event")
        assertSparkConf(eventLogContext.sparkConf)
    }

    test("EventLogContextLoader running") {
        Given("Path to eventLog with running application and no removed executors")
        val appId = "app-20231025121456-0004-eventLog-running"
        val eventLogPath = s"src/test/resources/${appId}"
        val eventLogContextLoader = new EventLogContextLoader

        When("EventLogContextLoader.load")
        val eventLogContext = eventLogContextLoader.load(new FileReaderFactory, SparkScopeArgs(eventLogPath, None, None, None))

        Then("App id, startTime, endTime should be read from app start/end events")
        assert(eventLogContext.appContext.appId == appId)
        assert(eventLogContext.appContext.appStartTime == 1698236095722L)
        assert(eventLogContext.appContext.appEndTime.isEmpty)

        And("Executor timeline should be read from executor add/remove events")
        assert(eventLogContext.appContext.executorMap.size == 2)
        assert(eventLogContext.appContext.executorMap("0").getStartTime.get== 1698236098507L)
        assert(eventLogContext.appContext.executorMap("0").getEndTime.isEmpty)
        assert(eventLogContext.appContext.executorMap("0").cores == 2)
        assert(eventLogContext.appContext.executorMap("1").getStartTime.get== 1698236098540L)
        assert(eventLogContext.appContext.executorMap("1").getEndTime.isEmpty)
        assert(eventLogContext.appContext.executorMap("1").cores == 2)

        And("SparkConf should be read from env update event")
        assertSparkConf(eventLogContext.sparkConf)
    }

    test("EventLogContextLoader finished executors removed") {
        Given("Path to eventLog with finished application and removed executors")
        val appId = "app-20231025121456-0004-eventLog-finished-exec-removed"
        val eventLogPath = s"src/test/resources/${appId}"
        val eventLogContextLoader = new EventLogContextLoader

        When("EventLogContextLoader.load")
        val eventLogContext = eventLogContextLoader.load(new FileReaderFactory, SparkScopeArgs(eventLogPath, None, None, None))

        Then("App id, startTime, endTime should be read from app start/end events")
        assert(eventLogContext.appContext.appId == appId)
        assert(eventLogContext.appContext.appStartTime == 1698236095722L)
        assert(eventLogContext.appContext.appEndTime.get == 1698236104099L)

        And("Executor timeline should be read from executor add/remove events")
        assert(eventLogContext.appContext.executorMap.size == 2)
        assert(eventLogContext.appContext.executorMap("0").getStartTime.get== 1698236098507L)
        assert(eventLogContext.appContext.executorMap("0").getEndTime.get == 1698236102012L)
        assert(eventLogContext.appContext.executorMap("0").cores == 2)
        assert(eventLogContext.appContext.executorMap("1").getStartTime.get== 1698236098540L)
        assert(eventLogContext.appContext.executorMap("1").getEndTime.get == 1698236103345L)
        assert(eventLogContext.appContext.executorMap("1").cores == 2)

        And("SparkConf should be read from env update event")
        assertSparkConf(eventLogContext.sparkConf)

    }

    test("EventLogContextLoader running executors removed") {
        Given("Path to eventLog with running application and removed executors")
        val appId = "app-20231025121456-0004-eventLog-running-exec-removed"
        val eventLogPath = s"src/test/resources/${appId}"
        val eventLogContextLoader = new EventLogContextLoader

        When("EventLogContextLoader.load")
        val eventLogContext = eventLogContextLoader.load(new FileReaderFactory, SparkScopeArgs(eventLogPath, None, None, None))

        Then("App id, startTime, endTime should be read from app start/end events")
        assert(eventLogContext.appContext.appId == appId)
        assert(eventLogContext.appContext.appStartTime == 1698236095722L)
        assert(eventLogContext.appContext.appEndTime.isEmpty)

        And("Executor timeline should be read from executor add/remove events")
        assert(eventLogContext.appContext.executorMap.size == 2)
        assert(eventLogContext.appContext.executorMap("0").getStartTime.get== 1698236098507L)
        assert(eventLogContext.appContext.executorMap("0").getEndTime.get == 1698236102012L)
        assert(eventLogContext.appContext.executorMap("0").cores == 2)
        assert(eventLogContext.appContext.executorMap("1").getStartTime.get== 1698236098540L)
        assert(eventLogContext.appContext.executorMap("1").getEndTime.get == 1698236103345L)
        assert(eventLogContext.appContext.executorMap("1").cores == 2)

        And("SparkConf should be read from env update event")
        assertSparkConf(eventLogContext.sparkConf)
    }

    test("EventLogContextLoader running incomplete eventlog") {
        Given("Path to eventLog with running application and removed executors")
        val appId = "app-20231025121456-0004-eventLog-running-incomplete"
        val eventLogPath = s"src/test/resources/${appId}"
        val eventLogContextLoader = new EventLogContextLoader

        When("EventLogContextLoader.load")
        val eventLogContext = eventLogContextLoader.load(new FileReaderFactory, SparkScopeArgs(eventLogPath, None, None, None))

        Then("App id, startTime, endTime should be read from app start/end events")
        assert(eventLogContext.appContext.appId == appId)
        assert(eventLogContext.appContext.appStartTime == 1698236095722L)
        assert(eventLogContext.appContext.appEndTime.isEmpty)

        And("Executor timeline should be read from executor add/remove events")
        assert(eventLogContext.appContext.executorMap.size == 2)
        assert(eventLogContext.appContext.executorMap("0").getStartTime.get== 1698236098507L)
        assert(eventLogContext.appContext.executorMap("0").getEndTime.isEmpty)
        assert(eventLogContext.appContext.executorMap("0").cores == 2)
        assert(eventLogContext.appContext.executorMap("1").getStartTime.get== 1698236098540L)
        assert(eventLogContext.appContext.executorMap("1").getEndTime.isEmpty)
        assert(eventLogContext.appContext.executorMap("1").cores == 2)

        And("SparkConf should be read from env update event")
        assertSparkConf(eventLogContext.sparkConf)
    }

    test("EventLogContextLoader parse args test") {
        Given("Overriden driverMetrics, executorMetrics and htmlPath args")
        val appId = "app-20231025121456-0004-eventLog-finished-exec-removed"
        val args = SparkScopeArgs(
            eventLog = s"src/test/resources/${appId}",
            driverMetrics = Some("overrriden/path/to/driver/metrics"),
            executorMetrics = Some("overrriden/path/to/executor/metrics"),
            htmlPath = Some("overrriden/path/to/html/report"),
        )
        val eventLogContextLoader = new EventLogContextLoader

        When("EventLogContextLoader.load")
        val eventLogContext = eventLogContextLoader.load(new FileReaderFactory, args)

        Then("App id, startTime, endTime should be read from app start/end events")
        assert(eventLogContext.appContext.appId == appId)
        assert(eventLogContext.appContext.appStartTime == 1698236095722L)
        assert(eventLogContext.appContext.appEndTime.get == 1698236104099L)

        And("Executor timeline should be read from executor add/remove events")
        assert(eventLogContext.appContext.executorMap.size == 2)
        assert(eventLogContext.appContext.executorMap("0").getStartTime.get== 1698236098507L)
        assert(eventLogContext.appContext.executorMap("0").getEndTime.get == 1698236102012L)
        assert(eventLogContext.appContext.executorMap("0").cores == 2)
        assert(eventLogContext.appContext.executorMap("1").getStartTime.get== 1698236098540L)
        assert(eventLogContext.appContext.executorMap("1").getEndTime.get == 1698236103345L)
        assert(eventLogContext.appContext.executorMap("1").cores == 2)

        And("SparkConf should be read from env update event")
        assertSparkConf(eventLogContext.sparkConf)
        assert(eventLogContext.sparkConf.get(SparkScopePropertyDriverMetricsDir) == "overrriden/path/to/driver/metrics")
        assert(eventLogContext.sparkConf.get(SparkScopePropertyExecutorMetricsDir) == "overrriden/path/to/executor/metrics")
        assert(eventLogContext.sparkConf.get(SparkScopePropertyHtmlPath) == "overrriden/path/to/html/report")

    }

    test("EventLogContextLoader finished stages test") {
        Given("Path to eventLog with finished application with stage events")
        val appId = "app-20231122115433-0000-eventLog-finished-stages"
        val eventLogPath = s"src/test/resources/${appId}"
        val eventLogContextLoader = new EventLogContextLoader

        When("EventLogContextLoader.load")
        val eventLogContext = eventLogContextLoader.load(new FileReaderFactory, SparkScopeArgs(eventLogPath, None, None, None))

        Then("App id, startTime, endTime should be read from app start/end events")
        assert(eventLogContext.appContext.appId == appId)
        assert(eventLogContext.appContext.appStartTime == 1700654071065L)
        assert(eventLogContext.appContext.appEndTime.get == 1700654100452L)

        And("Stages be read from stage subbmitted/completed events")
        assert(eventLogContext.appContext.stages.length == 10)
        assert(eventLogContext.appContext.stages.head == stageTimeline(0, 1700654082, 1700654087, 2))
        assert(eventLogContext.appContext.stages(1) == stageTimeline(1, 1700654088, 1700654089, 2))
        assert(eventLogContext.appContext.stages(2) == stageTimeline(3, 1700654090, 1700654093, 20, Seq(2)))
        assert(eventLogContext.appContext.stages(3) == stageTimeline(6, 1700654093, 1700654094, 1, Seq(5)))
        assert(eventLogContext.appContext.stages(4) == stageTimeline(7, 1700654094, 1700654096, 2))
        assert(eventLogContext.appContext.stages(5) == stageTimeline(8, 1700654094, 1700654095, 4))
        assert(eventLogContext.appContext.stages(6) == stageTimeline(10, 1700654096, 1700654099, 20, Seq(9)))
        assert(eventLogContext.appContext.stages(7) == stageTimeline(13, 1700654099, 1700654099, 1, Seq(12)))
        assert(eventLogContext.appContext.stages(8) == stageTimeline(16, 1700654099, 1700654099, 1, Seq(15)))
        assert(eventLogContext.appContext.stages(9) == stageTimeline(20, 1700654099, 1700654100, 1, Seq(19)))
    }

    test("EventLogContextLoader running stages not completed test") {
        Given("Path to eventLog with running application with incomplete stage events")
        val appId = "app-20231122115433-0000-eventLog-running-stages"
        val eventLogPath = s"src/test/resources/${appId}"
        val eventLogContextLoader = new EventLogContextLoader

        When("EventLogContextLoader.load")
        val eventLogContext = eventLogContextLoader.load(new FileReaderFactory, SparkScopeArgs(eventLogPath, None, None, None))

        Then("App id, startTime, endTime should be read from app start/end events")
        assert(eventLogContext.appContext.appId == appId)
        assert(eventLogContext.appContext.appStartTime == 1700654071065L)
        assert(eventLogContext.appContext.appEndTime.isEmpty)

        And("Stages be read from stage subbmitted/completed events")
        assert(eventLogContext.appContext.stages.length == 9)
        assert(eventLogContext.appContext.stages.head == stageTimeline(0, 1700654082, 1700654087, 2))
        assert(eventLogContext.appContext.stages(1) == stageTimeline(1, 1700654088, 1700654089, 2))
        assert(eventLogContext.appContext.stages(2) == stageTimeline(3, 1700654090, 1700654093, 20, Seq(2)))
        assert(eventLogContext.appContext.stages(3) == stageTimeline(6, 1700654093, 1700654094, 1, Seq(5)))
        assert(eventLogContext.appContext.stages(4) == stageTimeline(7, 1700654094, 1700654096, 2))
        assert(eventLogContext.appContext.stages(5) == stageTimeline(8, 1700654094, 1700654095, 4))
        assert(eventLogContext.appContext.stages(6) == stageTimeline(10, 1700654096, 1700654099, 20, Seq(9)))
        assert(eventLogContext.appContext.stages(7) == stageTimeline(13, 1700654099, 1700654099, 1, Seq(12)))
        assert(eventLogContext.appContext.stages(8) == stageTimeline(16, 1700654099, 1700654099, 1, Seq(15)))
    }

    test("EventLogContextLoader corrupted events test") {
        Given("Path to eventLog with corrupted executor added/removed and stage submitted/completed events")
        val appId = "app-20231122115433-0000-eventLog-corrupted-events"
        val eventLogPath = s"src/test/resources/${appId}"
        val eventLogContextLoader = new EventLogContextLoader

        When("EventLogContextLoader.load")
        val eventLogContext = eventLogContextLoader.load(new FileReaderFactory, SparkScopeArgs(eventLogPath, None, None, None))

        Then("No exception should be thrown")
        And("App id, startTime, endTime should be read from app start/end events")
        assert(eventLogContext.appContext.appId == appId)
        assert(eventLogContext.appContext.appStartTime == 1700654071065L)
        assert(eventLogContext.appContext.appEndTime.isEmpty)

        And("Corrupted stage events should be ignored")
        And("Stages be read from stage submitted/completed events")
        assert(eventLogContext.appContext.stages.length == 8)
        assert(eventLogContext.appContext.stages.head == stageTimeline(0, 1700654082, 1700654087, 2))
        assert(eventLogContext.appContext.stages(1) == stageTimeline(1, 1700654088, 1700654089, 2))
        assert(eventLogContext.appContext.stages(2) == stageTimeline(3, 1700654090, 1700654093, 20, Seq(2)))
        assert(eventLogContext.appContext.stages(3) == stageTimeline(6, 1700654093, 1700654094, 1, Seq(5)))
        assert(eventLogContext.appContext.stages(4) == stageTimeline(7, 1700654094, 1700654096, 2))
        assert(eventLogContext.appContext.stages(5) == stageTimeline(8, 1700654094, 1700654095, 4))
        assert(eventLogContext.appContext.stages(6) == stageTimeline(10, 1700654096, 1700654099, 20, Seq(9)))
        assert(eventLogContext.appContext.stages(7) == stageTimeline(13, 1700654099, 1700654099, 1, Seq(12)))

        And("Corrupted executor events should be ignored")
        And("Executor timeline should be read from executor add/remove events")
        assert(eventLogContext.appContext.executorMap.size == 1)
        assert(eventLogContext.appContext.executorMap("1").getStartTime.get== 1700654079478L)
        assert(eventLogContext.appContext.executorMap("1").getEndTime.isEmpty)
        assert(eventLogContext.appContext.executorMap("1").cores == 2)
    }

    test("EventLogContextLoader bad path to eventLog") {
        Given("Bad path to eventLog")
        val eventLogPath = s"bad/path/to/event/log"

        When("EventLogContextLoader.load")
        Then("NoSuchFileException should be thrown")
        assertThrows[NoSuchFileException] {
            val eventLogContext = new EventLogContextLoader().load(new FileReaderFactory, SparkScopeArgs(eventLogPath, None, None, None))
        }
    }

    test("EventLogContextLoader bad eventLog, no app start event") {
        Given("Path to bad eventLog, no app start event")
        val appId = "app-20231025121456-0004-eventLog-error-no-app-start"
        val eventLogPath = s"src/test/resources/${appId}"

        When("EventLogContextLoader.load")
        Then("IllegalArgumentException should be thrown")
        assertThrows[IllegalArgumentException] {
            val eventLogContext = new EventLogContextLoader().load(new FileReaderFactory, SparkScopeArgs(eventLogPath, None, None, None))
        }
    }

    test("EventLogContextLoader bad eventLog, no env update event") {
        Given("Path to bad eventLog, no env update event")
        val appId = "app-20231025121456-0004-eventLog-error-no-env-update"
        val eventLogPath = s"src/test/resources/${appId}"

        When("EventLogContextLoader.load")
        Then("IllegalArgumentException should be thrown")
        assertThrows[IllegalArgumentException] {
            val eventLogContext = new EventLogContextLoader().load(new FileReaderFactory, SparkScopeArgs(eventLogPath, None, None, None))
        }
    }

    test("EventLogContextLoader bad eventLog, bad app start event") {
        Given("Path to bad eventLog, no app start event")
        val appId = "app-20231025121456-0004-eventLog-error-bad-app-start"
        val eventLogPath = s"src/test/resources/${appId}"

        When("EventLogContextLoader.load")
        Then("IllegalArgumentException should be thrown")
        assertThrows[IllegalArgumentException] {
            val eventLogContext = new EventLogContextLoader().load(new FileReaderFactory, SparkScopeArgs(eventLogPath, None, None, None))
        }
    }

    test("EventLogContextLoader bad eventLog, bad update event") {
        Given("Path to bad eventLog, no env update event")
        val appId = "app-20231025121456-0004-eventLog-error-bad-env-update"
        val eventLogPath = s"src/test/resources/${appId}"

        When("EventLogContextLoader.load")
        Then("IllegalArgumentException should be thrown")
        assertThrows[IllegalArgumentException] {
            val eventLogContext = new EventLogContextLoader().load(new FileReaderFactory, SparkScopeArgs(eventLogPath, None, None, None))
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

