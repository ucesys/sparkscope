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
import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.io.reader.FileReaderFactory
import com.ucesys.sparkscope.io.metrics.{CsvMetricsLoader, HadoopMetricReader, MetricsLoaderFactory}
import com.ucesys.sparkscope.io.report.ReporterFactory
import com.ucesys.sparkscope.io.writer.FileWriterFactory
import org.apache.commons.io.FileUtils
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FunSuite, GivenWhenThen}

import java.io.File
import java.nio.file.{Files, Path, Paths}

class SparkScopeAppSuite extends FunSuite with MockFactory with GivenWhenThen with BeforeAndAfterAll with SuiteDirectoryUtils {

    override def beforeAll(): Unit = prepareSuiteTestDir()

    val sparkScopeConfHtmlReportPath = sparkScopeConf.copy(htmlReportPath = Some(getSuiteTestDir()), logPath = getSuiteTestDir())

    test("SparkScopeApp.runFromEventLog for finished application ") {
        implicit val logger: SparkScopeLogger = new SparkScopeLogger
        Given("Path to eventLog for finished application")
        val appId = "app-20231025121456-0004-eventLog-finished"
        val eventLogPath = s"src/test/resources/eventlog/${appId}"
        val ac = mockAppContextWithDownscalingMuticore(appId, appName = "myApp")
        val csvReaderMock = stub[HadoopMetricReader]
        mockMetricsEventLog(csvReaderMock, ac.appId)

        val metricsLoader = new CsvMetricsLoader(csvReaderMock)
        val metricsLoaderFactory = stub[MetricsLoaderFactory]
        (metricsLoaderFactory.get _).when(*, *).returns(metricsLoader)

        When("SparkScopeApp.runFromEventLog")
        SparkScopeApp.runFromEventLog(
            SparkScopeArgs(eventLog = eventLogPath, driverMetrics = None, htmlPath = Some(getSuiteTestDir()), logPath = Some(getSuiteTestDir())),
            new SparkScopeAnalyzer,
            new SparkScopeConfLoader,
            new FileReaderFactory,
            getPropertiesLoaderFactoryMock,
            metricsLoaderFactory,
            new FileWriterFactory,
            new ReporterFactory
        )

        Then("Report should be generated")
        val htmlPath = Paths.get(getSuiteTestDir(), ac.appId + ".html")
        assert(Files.exists(htmlPath))
    }

    test("SparkScopeApp.runFromEventLog for running application") {
        implicit val logger: SparkScopeLogger = new SparkScopeLogger
        Given("Path to eventLog for running application")
        val appId = "app-20231025121456-0004-eventLog-running"
        val eventLogPath = s"src/test/resources/eventlog/${appId}"
        val ac = mockAppContextWithDownscalingMuticore(appId, appName = "myApp")
        val csvReaderMock = stub[HadoopMetricReader]
        mockMetricsEventLog(csvReaderMock, ac.appId)

        val metricsLoader = new CsvMetricsLoader(csvReaderMock)
        val metricsLoaderFactory = stub[MetricsLoaderFactory]
        (metricsLoaderFactory.get _).when(*, *).returns(metricsLoader)

        When("SparkScopeApp.runFromEventLog")
        SparkScopeApp.runFromEventLog(
            SparkScopeArgs(eventLog = eventLogPath, driverMetrics = None, htmlPath = Some(getSuiteTestDir()), logPath = Some(getSuiteTestDir())),
            new SparkScopeAnalyzer,
            new SparkScopeConfLoader,
            new FileReaderFactory,
            getPropertiesLoaderFactoryMock,
            metricsLoaderFactory,
            new FileWriterFactory,
            new ReporterFactory
        )

        Then("Report should be generated")
        assert(Files.exists(Paths.get(getSuiteTestDir(), ac.appId + ".html")))
    }

    test("SparkScopeApp.runFromEventLog for finished application with removed executors") {
        implicit val logger: SparkScopeLogger = new SparkScopeLogger
        Given("Path to eventLog for finished application with removed executors")
        val appId = "app-20231025121456-0004-eventLog-finished-exec-removed"
        val eventLogPath = s"src/test/resources/eventlog/${appId}"
        val ac = mockAppContextWithDownscalingMuticore(appId, appName = "myApp")
        val csvReaderMock = stub[HadoopMetricReader]
        mockMetricsEventLog(csvReaderMock, ac.appId)

        val metricsLoader = new CsvMetricsLoader(csvReaderMock)
        val metricsLoaderFactory = stub[MetricsLoaderFactory]
        (metricsLoaderFactory.get _).when(*, *).returns(metricsLoader)

        When("SparkScopeApp.runFromEventLog")
        SparkScopeApp.runFromEventLog(
            SparkScopeArgs(eventLog = eventLogPath, driverMetrics = None, htmlPath = Some(getSuiteTestDir()), logPath = Some(getSuiteTestDir())),
            new SparkScopeAnalyzer,
            new SparkScopeConfLoader,
            new FileReaderFactory,
            getPropertiesLoaderFactoryMock,
            metricsLoaderFactory,
            new FileWriterFactory,
            new ReporterFactory
        )

        Then("Report should be generated")
        assert(Files.exists(Paths.get(getSuiteTestDir(), ac.appId + ".html")))
    }

    test("SparkScopeApp.runFromEventLog for running application with removed executors") {
        implicit val logger: SparkScopeLogger = new SparkScopeLogger
        Given("Path to eventLog for running application with removed executors")
        val appId = "app-20231025121456-0004-eventLog-running-exec-removed"
        val eventLogPath = s"src/test/resources/eventlog/${appId}"
        val ac = mockAppContextWithDownscalingMuticore(appId, appName = "myApp")
        val csvReaderMock = stub[HadoopMetricReader]
        mockMetricsEventLog(csvReaderMock, ac.appId)

        val metricsLoader = new CsvMetricsLoader(csvReaderMock)
        val metricsLoaderFactory = stub[MetricsLoaderFactory]
        (metricsLoaderFactory.get _).when(*, *).returns(metricsLoader)

        When("SparkScopeApp.runFromEventLog")
        SparkScopeApp.runFromEventLog(
            SparkScopeArgs(eventLog = eventLogPath, driverMetrics = None, htmlPath = Some(getSuiteTestDir()), logPath = Some(getSuiteTestDir())),
            new SparkScopeAnalyzer,
            new SparkScopeConfLoader,
            new FileReaderFactory,
            getPropertiesLoaderFactoryMock,
            metricsLoaderFactory,
            new FileWriterFactory,
            new ReporterFactory
        )

        Then("Report should be generated")
        assert(Files.exists(Paths.get(getSuiteTestDir(), ac.appId + ".html")))
    }

    test("SparkScopeApp.runFromEventLog for running application with incomplete eventlog") {
        implicit val logger: SparkScopeLogger = new SparkScopeLogger
        Given("Path to eventLog for running application which is incomplete(last json is half-written)")
        val appId = "app-20231025121456-0004-eventLog-running-incomplete"
        val eventLogPath = s"src/test/resources/eventlog/${appId}"
        val ac = mockAppContextWithDownscalingMuticore(appId, appName = "myApp")
        val csvReaderMock = stub[HadoopMetricReader]
        mockMetricsEventLog(csvReaderMock, ac.appId)

        val metricsLoader = new CsvMetricsLoader(csvReaderMock)
        val metricsLoaderFactory = stub[MetricsLoaderFactory]
        (metricsLoaderFactory.get _).when(*, *).returns(metricsLoader)

        When("SparkScopeApp.runFromEventLog")
        SparkScopeApp.runFromEventLog(
            SparkScopeArgs(eventLog = eventLogPath, driverMetrics = None, htmlPath = Some(getSuiteTestDir()), logPath = Some(getSuiteTestDir())),
            new SparkScopeAnalyzer,
            new SparkScopeConfLoader,
            new FileReaderFactory,
            getPropertiesLoaderFactoryMock,
            metricsLoaderFactory,
            new FileWriterFactory,
            new ReporterFactory
        )

        Then("Report should be generated")
        assert(Files.exists(Paths.get(getSuiteTestDir(), ac.appId + ".html")))
    }

    test("SparkScopeApp.runFromEventLog for finished application with stage events") {
        implicit val logger: SparkScopeLogger = new SparkScopeLogger
        Given("Path to eventLog for running application with removed executors")
        val appId = "app-20231122115433-0000-eventLog-finished-stages"
        val eventLogPath = s"src/test/resources/eventlog/${appId}"
        val ac = mockAppContextWithDownscalingMuticore(appId, appName = "myApp")
        val csvReaderMock = stub[HadoopMetricReader]
        mockMetricsEventLogStages(csvReaderMock, ac.appId)

        val metricsLoader = new CsvMetricsLoader(csvReaderMock)
        val metricsLoaderFactory = stub[MetricsLoaderFactory]
        (metricsLoaderFactory.get _).when(*, *).returns(metricsLoader)

        When("SparkScopeApp.runFromEventLog")
        SparkScopeApp.runFromEventLog(
            SparkScopeArgs(eventLog = eventLogPath, driverMetrics = None, htmlPath = Some(getSuiteTestDir()), logPath = Some(getSuiteTestDir())),
            new SparkScopeAnalyzer,
            new SparkScopeConfLoader,
            new FileReaderFactory,
            getPropertiesLoaderFactoryMock,
            metricsLoaderFactory,
            new FileWriterFactory,
            new ReporterFactory
        )

        Then("Report should be generated")
        assert(Files.exists(Paths.get(getSuiteTestDir(), ac.appId + ".html")))
    }
}

