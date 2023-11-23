
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

import com.ucesys.sparkscope.TestHelpers._
import com.ucesys.sparkscope.event.EventLogContextLoader
import com.ucesys.sparkscope.io._
import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.io.file.FileReaderFactory
import com.ucesys.sparkscope.io.metrics.{CsvMetricsLoader, HadoopMetricReader, MetricReaderFactory, MetricsLoaderFactory}
import com.ucesys.sparkscope.io.report.ReportGeneratorFactory
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FunSuite, GivenWhenThen}

import java.nio.file.{Files, Paths}

class SparkScopeAppSuite extends FunSuite with MockFactory with GivenWhenThen with BeforeAndAfterAll {
    override def beforeAll(): Unit = Files.createDirectories(Paths.get(TestDir))

    val sparkScopeConfHtmlReportPath = sparkScopeConf.copy(htmlReportPath = TestDir)
    val SparkLensOutput = Seq("Executor Timeline", "StageSkewAnalyzer text...")

    test("SparkScopeApp.runFromEventLog for finished application ") {
        implicit val logger: SparkScopeLogger = new SparkScopeLogger
        Given("Path to eventLog for finished application")
        val appId = "app-20231025121456-0004-eventLog-finished"
        val eventLogPath = s"src/test/resources/${appId}"
        val ac = mockAppContextWithDownscalingMuticore("", appId)
        val csvReaderMock = stub[HadoopMetricReader]
        mockMetricsEventLog(csvReaderMock, ac.appId)

        val metricsLoader = new CsvMetricsLoader(csvReaderMock)
        val metricsLoaderFactory = stub[MetricsLoaderFactory]
        (metricsLoaderFactory.get _).when(*, *).returns(metricsLoader)

        When("SparkScopeApp.runFromEventLog")
        SparkScopeApp.runFromEventLog(
            SparkScopeArgs(eventLogPath, None, None, None),
            new SparkScopeAnalyzer,
            new EventLogContextLoader,
            new SparkScopeConfLoader,
            new FileReaderFactory,
            getPropertiesLoaderFactoryMock,
            metricsLoaderFactory,
            new ReportGeneratorFactory
        )

        Then("Report should be generated")
        assert(Files.exists(Paths.get(TestDir, ac.appId + ".html")))
    }

    test("SparkScopeApp.runFromEventLog for running application") {
        implicit val logger: SparkScopeLogger = new SparkScopeLogger
        Given("Path to eventLog for running application")
        val appId = "app-20231025121456-0004-eventLog-running"
        val eventLogPath = s"src/test/resources/${appId}"
        val ac = mockAppContextWithDownscalingMuticore("", appId)
        val csvReaderMock = stub[HadoopMetricReader]
        mockMetricsEventLog(csvReaderMock, ac.appId)

        val metricsLoader = new CsvMetricsLoader(csvReaderMock)
        val metricsLoaderFactory = stub[MetricsLoaderFactory]
        (metricsLoaderFactory.get _).when(*, *).returns(metricsLoader)

        When("SparkScopeApp.runFromEventLog")
        SparkScopeApp.runFromEventLog(
            SparkScopeArgs(eventLogPath, None, None, None),
            new SparkScopeAnalyzer,
            new EventLogContextLoader,
            new SparkScopeConfLoader,
            new FileReaderFactory,
            getPropertiesLoaderFactoryMock,
            metricsLoaderFactory,
            new ReportGeneratorFactory
        )

        Then("Report should be generated")
        assert(Files.exists(Paths.get(TestDir, ac.appId + ".html")))
    }

    test("SparkScopeApp.runFromEventLog for finished application with removed executors") {
        implicit val logger: SparkScopeLogger = new SparkScopeLogger
        Given("Path to eventLog for finished application with removed executors")
        val appId = "app-20231025121456-0004-eventLog-finished-exec-removed"
        val eventLogPath = s"src/test/resources/${appId}"
        val ac = mockAppContextWithDownscalingMuticore("", appId)
        val csvReaderMock = stub[HadoopMetricReader]
        mockMetricsEventLog(csvReaderMock, ac.appId)

        val metricsLoader = new CsvMetricsLoader(csvReaderMock)
        val metricsLoaderFactory = stub[MetricsLoaderFactory]
        (metricsLoaderFactory.get _).when(*, *).returns(metricsLoader)

        When("SparkScopeApp.runFromEventLog")
        SparkScopeApp.runFromEventLog(
            SparkScopeArgs(eventLogPath, None, None, None),
            new SparkScopeAnalyzer,
            new EventLogContextLoader,
            new SparkScopeConfLoader,
            new FileReaderFactory,
            getPropertiesLoaderFactoryMock,
            metricsLoaderFactory,
            new ReportGeneratorFactory
        )

        Then("Report should be generated")
        assert(Files.exists(Paths.get(TestDir, ac.appId + ".html")))
    }

    test("SparkScopeApp.runFromEventLog for running application with removed executors") {
        implicit val logger: SparkScopeLogger = new SparkScopeLogger
        Given("Path to eventLog for running application with removed executors")
        val appId = "app-20231025121456-0004-eventLog-running-exec-removed"
        val eventLogPath = s"src/test/resources/${appId}"
        val ac = mockAppContextWithDownscalingMuticore("", appId)
        val csvReaderMock = stub[HadoopMetricReader]
        mockMetricsEventLog(csvReaderMock, ac.appId)

        val metricsLoader = new CsvMetricsLoader(csvReaderMock)
        val metricsLoaderFactory = stub[MetricsLoaderFactory]
        (metricsLoaderFactory.get _).when(*, *).returns(metricsLoader)

        When("SparkScopeApp.runFromEventLog")
        SparkScopeApp.runFromEventLog(
            SparkScopeArgs(eventLogPath, None, None, None),
            new SparkScopeAnalyzer,
            new EventLogContextLoader,
            new SparkScopeConfLoader,
            new FileReaderFactory,
            getPropertiesLoaderFactoryMock,
            metricsLoaderFactory,
            new ReportGeneratorFactory
        )

        Then("Report should be generated")
        assert(Files.exists(Paths.get(TestDir, ac.appId + ".html")))
    }

    test("SparkScopeApp.runFromEventLog for running application with incomplete eventlog") {
        implicit val logger: SparkScopeLogger = new SparkScopeLogger
        Given("Path to eventLog for running application which is incomplete(last json is half-written)")
        val appId = "app-20231025121456-0004-eventLog-running-incomplete"
        val eventLogPath = s"src/test/resources/${appId}"
        val ac = mockAppContextWithDownscalingMuticore("", appId)
        val csvReaderMock = stub[HadoopMetricReader]
        mockMetricsEventLog(csvReaderMock, ac.appId)

        val metricsLoader = new CsvMetricsLoader(csvReaderMock)
        val metricsLoaderFactory = stub[MetricsLoaderFactory]
        (metricsLoaderFactory.get _).when(*, *).returns(metricsLoader)

        When("SparkScopeApp.runFromEventLog")
        SparkScopeApp.runFromEventLog(
            SparkScopeArgs(eventLogPath, None, None, None),
            new SparkScopeAnalyzer,
            new EventLogContextLoader,
            new SparkScopeConfLoader,
            new FileReaderFactory,
            getPropertiesLoaderFactoryMock,
            metricsLoaderFactory,
            new ReportGeneratorFactory
        )

        Then("Report should be generated")
        assert(Files.exists(Paths.get(TestDir, ac.appId + ".html")))
    }

    test("SparkScopeApp.runFromEventLog for finished application with stage events") {
        implicit val logger: SparkScopeLogger = new SparkScopeLogger
        Given("Path to eventLog for running application with removed executors")
        val appId = "app-20231122115433-0000-eventLog-finished-stages"
        val eventLogPath = s"src/test/resources/${appId}"
        val ac = mockAppContextWithDownscalingMuticore("", appId)
        val csvReaderMock = stub[HadoopMetricReader]
        mockMetricsEventLogStages(csvReaderMock, ac.appId)

        val metricsLoader = new CsvMetricsLoader(csvReaderMock)
        val metricsLoaderFactory = stub[MetricsLoaderFactory]
        (metricsLoaderFactory.get _).when(*, *).returns(metricsLoader)

        When("SparkScopeApp.runFromEventLog")
        SparkScopeApp.runFromEventLog(
            SparkScopeArgs(eventLogPath, None, None, Some(".tests")),
            new SparkScopeAnalyzer,
            new EventLogContextLoader,
            new SparkScopeConfLoader,
            new FileReaderFactory,
            getPropertiesLoaderFactoryMock,
            metricsLoaderFactory,
            new ReportGeneratorFactory
        )

        Then("Report should be generated")
        assert(Files.exists(Paths.get(TestDir, ac.appId + ".html")))
    }
}

