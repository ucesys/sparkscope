
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

import com.ucesys.sparkscope.TestHelpers.{getFileReaderFactoryMock, getPropertiesLoaderFactoryMock, getPropertiesLoaderMock, missingMetricsWarning, mockAppContext, mockAppContextMissingExecutorMetrics, mockAppContextWithDownscaling, mockMetricsWithDownscaling, mockcorrectMetrics, sparkConf, sparkScopeConf}
import com.ucesys.sparkscope.io.{CsvHadoopMetricsLoader, HadoopFileReader, HtmlReportGenerator, PropertiesLoaderFactory}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSuite, GivenWhenThen}

import java.nio.file.{Files, Paths}

class SparkScopeRunnerSuite extends FunSuite with MockFactory with GivenWhenThen {

  test("SparkScopeRunner upscaling test") {
    Given("Metrics for application which was upscaled")
    val ac = mockAppContext()
    val csvReaderMock = stub[HadoopFileReader]
    mockcorrectMetrics(csvReaderMock)

    And("SparkScopeConf with specified html report path")
    val sparkScopeConfHtmlReportPath = sparkScopeConf.copy(htmlReportPath = "./")
    val metricsLoader = new CsvHadoopMetricsLoader(getFileReaderFactoryMock(csvReaderMock), ac, sparkScopeConfHtmlReportPath)
    val sparkScopeRunner = new SparkScopeRunner(ac, sparkScopeConfHtmlReportPath, metricsLoader, Seq("Executor Timeline", "Sparkscope text"))

    When("SparkScopeRunner.run")
    sparkScopeRunner.run()

    Then("Report should be generated")
    assert(Files.exists(Paths.get("./" + ac.appInfo.applicationID + ".html")))
  }


  test("SparkScopeRunner upscaling and downscaling test") {
    Given("Metrics for application which was upscaled and downscaled")
    val ac = mockAppContextWithDownscaling()
    val csvReaderMock = stub[HadoopFileReader]
    mockMetricsWithDownscaling(csvReaderMock)

    And("SparkScopeConf with specified html report path")
    val sparkScopeConfHtmlReportPath = sparkScopeConf.copy(htmlReportPath = "./")
    val metricsLoader = new CsvHadoopMetricsLoader(getFileReaderFactoryMock(csvReaderMock), ac, sparkScopeConfHtmlReportPath)
    val sparkScopeRunner = new SparkScopeRunner(ac, sparkScopeConfHtmlReportPath, metricsLoader, Seq("Executor Timeline", "Sparkscope text"))

    When("SparkScopeRunner.run")
    sparkScopeRunner.run()

    Then("Report should be generated")
    assert(Files.exists(Paths.get("./" + ac.appInfo.applicationID + ".html")))
  }
}

