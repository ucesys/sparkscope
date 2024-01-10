
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

package com.ucesys.sparkscope.io.report

import com.ucesys.sparkscope.{SparkScopeAnalyzer, SuiteDirectoryUtils}
import com.ucesys.sparkscope.TestHelpers.{sparkScopeConf, _}
import com.ucesys.sparkscope.agg.TaskAggMetrics
import com.ucesys.sparkscope.common.{MemorySize, SparkScopeLogger}
import com.ucesys.sparkscope.io.metrics.HadoopMetricReader
import com.ucesys.sparkscope.io.writer.LocalFileWriter
import com.ucesys.sparkscope.view.warning.{DiskSpillWarning, GCTimeWarning}
import org.apache.commons.lang.SystemUtils
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FunSuite, GivenWhenThen}

import java.nio.file.{Files, Paths}
import scala.concurrent.duration.DurationInt

class HtmlFileReporterSuite extends FunSuite with MockFactory with BeforeAndAfterAll with GivenWhenThen with SuiteDirectoryUtils {
    override def beforeAll(): Unit = prepareSuiteTestDir()
    val fileWriter = new LocalFileWriter

    test("HtmlFileReporter end2end no warnings") {
        implicit val logger: SparkScopeLogger = new SparkScopeLogger
        Given("HtmlReportGenerator with logging to file enabled")
        val appId = s"${SampleAppId}-html-generator-no-warnings"
        val ac = mockAppContext(appId, SampleAppName)
        val sparkScopeConfHtml = sparkScopeConf.copy(htmlReportPath = Some(getSuiteTestDir()), logPath = getSuiteTestDir(), appName = Some("MyApp"))

        val htmlReportGenerator = new HtmlFileReporter(ac, sparkScopeConfHtml, htmlFileWriter=fileWriter)

        And("SparkScopeResult")
        val csvReaderMock = stub[HadoopMetricReader]
        mockcorrectMetrics(csvReaderMock, ac.appId)
        val executorMetricsAnalyzer = new SparkScopeAnalyzer
        val result = executorMetricsAnalyzer.analyze(DriverExecutorMetricsMock, ac, sparkScopeConfHtml, TaskAggMetrics()).copy(warnings = Seq.empty)

        When("calling HtmlReportGenerator.generate")
        htmlReportGenerator.report(result)

        Then("html report is created")
        val htmlPath = Paths.get(getSuiteTestDir(), ac.appId + ".html")
        assert(Files.exists(htmlPath))

        And("all templated files(${}) were properly ingested")
        assert(!new String(Files.readAllBytes(htmlPath)).contains("${"))

        And("html contents are correct")
        if (SystemUtils.OS_NAME == "Linux") {
            assert(new String(Files.readAllBytes(htmlPath)) == new String(Files.readAllBytes(Paths.get("src/test/resources/html/app-123-html-generator-no-warnings.html"))))
        }
    }

    test("HtmlFileReporter end2end with warnings") {
        implicit val logger: SparkScopeLogger = new SparkScopeLogger
        Given("HtmlReportGenerator with logging to file enabled")
        val appId = s"${SampleAppId}-html-generator-with-warnings"
        val ac = mockAppContext(appId, SampleAppName)
        val sparkScopeConfHtml = sparkScopeConf.copy(htmlReportPath = Some(getSuiteTestDir()), logPath = getSuiteTestDir(), appName = Some("MyApp"))

        val htmlReportGenerator = new HtmlFileReporter(ac, sparkScopeConfHtml, htmlFileWriter = fileWriter)

        And("SparkScopeResult")
        val csvReaderMock = stub[HadoopMetricReader]
        mockcorrectMetrics(csvReaderMock, ac.appId)
        val executorMetricsAnalyzer = new SparkScopeAnalyzer
        val result = executorMetricsAnalyzer.analyze(DriverExecutorMetricsMock, ac, sparkScopeConfHtml, TaskAggMetrics())
        val diskSpillWarning = DiskSpillWarning(MemorySize.fromMegaBytes(500), MemorySize.fromGigaBytes(5))
        val gcTimeWarning = GCTimeWarning(2000.seconds, 600.seconds, 0.3)

        val resultWithAllWarnings = result.copy(warnings = result.warnings ++ Seq(diskSpillWarning, gcTimeWarning))

        When("calling HtmlReportGenerator.generate")
        htmlReportGenerator.report(resultWithAllWarnings)

        Then("html report is created")
        val htmlPath = Paths.get(getSuiteTestDir(), ac.appId + ".html")
        assert(Files.exists(htmlPath))

        And("all templated files(${}) were properly ingested")
        assert(!new String(Files.readAllBytes(htmlPath)).contains("${"))

        And("html contents are correct")
        if (SystemUtils.OS_NAME == "Linux") {
            assert(new String(Files.readAllBytes(htmlPath)) == new String(Files.readAllBytes(Paths.get("src/test/resources/html/app-123-html-generator-with-warnings.html"))))
        }
    }
}
