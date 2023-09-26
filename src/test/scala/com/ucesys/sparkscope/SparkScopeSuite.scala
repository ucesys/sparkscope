
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
import org.scalatest.FunSuite

import java.nio.file.{Files, Paths}

class SparkScopeSuite extends FunSuite with MockFactory {

  test("SparkScope end2end executor upscaling test") {
    val ac = mockAppContext()
    val csvReaderMock = stub[HadoopFileReader]
    mockcorrectMetrics(csvReaderMock)
    val metricsLoader = new CsvHadoopMetricsLoader(getFileReaderFactoryMock(csvReaderMock), ac, sparkScopeConf)
    val executorMetricsAnalyzer = new SparkScopeAnalyzer
    val result = executorMetricsAnalyzer.analyze(metricsLoader.load(), ac)

    HtmlReportGenerator.generateHtml(result, "./", Seq("Executor Timeline", "Sparkscope text"), sparkConf)

    assert(Files.exists(Paths.get("./" + result.appInfo.applicationID + ".html")))
  }

  test("SparkScope end2end executor upscaling and downscaling test") {
    val ac = mockAppContextWithDownscaling()
    val csvReaderMock = stub[HadoopFileReader]
    mockMetricsWithDownscaling(csvReaderMock)
    val metricsLoader = new CsvHadoopMetricsLoader(getFileReaderFactoryMock(csvReaderMock), ac, sparkScopeConf)
    val executorMetricsAnalyzer = new SparkScopeAnalyzer
    val result = executorMetricsAnalyzer.analyze(metricsLoader.load(), ac)

    HtmlReportGenerator.generateHtml(result, "./", Seq("Executor Timeline", "Sparkscope text"), sparkConf)

    assert(Files.exists(Paths.get("./" + result.appInfo.applicationID + ".html")))
  }
}

