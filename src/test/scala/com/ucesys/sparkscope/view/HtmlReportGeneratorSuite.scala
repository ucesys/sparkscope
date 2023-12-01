
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

package com.ucesys.sparkscope.view

import com.ucesys.sparkscope.SparkScopeAnalyzer
import com.ucesys.sparkscope.TestHelpers._
import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.io.file.LocalFileWriter
import com.ucesys.sparkscope.io.metrics.HadoopMetricReader
import com.ucesys.sparkscope.view.HtmlReportGenerator
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FunSuite, GivenWhenThen}

import java.nio.file.{Files, Paths}

class HtmlReportGeneratorSuite extends FunSuite with MockFactory with BeforeAndAfterAll with GivenWhenThen {
    override def beforeAll(): Unit = Files.createDirectories(Paths.get(TestDir))
    val fileWriter = new LocalFileWriter

    test("SparkScope end2end no warnings") {
        implicit val logger: SparkScopeLogger = new SparkScopeLogger
        Given("HtmlReportGenerator ")
        val htmlReportGenerator = new HtmlReportGenerator(
            sparkScopeConf.copy(htmlReportPath = TestDir, appName = Some("MyApp")),
            fileWriter=fileWriter
        )

        And("SparkScopeResult")
        val ac = mockAppContext("html-generator-no-warnings")
        val csvReaderMock = stub[HadoopMetricReader]
        mockcorrectMetrics(csvReaderMock, ac.appId)
        val executorMetricsAnalyzer = new SparkScopeAnalyzer
        val result = executorMetricsAnalyzer.analyze(DriverExecutorMetricsMock, ac).copy(warnings = Seq.empty)

        When("calling HtmlReportGenerator.generate")
        htmlReportGenerator.generate(result, Seq("Executor Timeline", "Sparkscope text"))

        Then("html report is created")
        val htmlPath = Paths.get(TestDir, result.appContext.appId + ".html")
        assert(Files.exists(htmlPath))

        And("all templated files(${}) were properly ingested")
        assert(!new String(Files.readAllBytes(htmlPath)).contains("${"))
    }

    test("SparkScope end2end with warnings") {
        implicit val logger: SparkScopeLogger = new SparkScopeLogger
        Given("HtmlReportGenerator ")
        val htmlReportGenerator = new HtmlReportGenerator(sparkScopeConf.copy(htmlReportPath = TestDir, appName = Some("MyApp")), fileWriter=fileWriter)

        And("SparkScopeResult")
        val ac = mockAppContextMissingExecutorMetrics("html-generator-with-warnings")
        val csvReaderMock = stub[HadoopMetricReader]
        mockcorrectMetrics(csvReaderMock, ac.appId)
        val executorMetricsAnalyzer = new SparkScopeAnalyzer
        val result = executorMetricsAnalyzer.analyze(DriverExecutorMetricsMock, ac)

        When("calling HtmlReportGenerator.generate")
        htmlReportGenerator.generate(result, Seq("Executor Timeline", "Sparkscope text"))

        Then("html report is created")
        val htmlPath = Paths.get(TestDir, result.appContext.appId + ".html")
        assert(Files.exists(htmlPath))

        And("all templated files(${}) were properly ingested")
        assert(!new String(Files.readAllBytes(htmlPath)).contains("${"))
    }
}

