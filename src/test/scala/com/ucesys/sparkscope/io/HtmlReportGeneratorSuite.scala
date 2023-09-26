
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

package com.ucesys.sparkscope.io

import com.ucesys.sparkscope.SparkScopeAnalyzer
import com.ucesys.sparkscope.TestHelpers._
import org.scalamock.scalatest.MockFactory
import org.scalatest.FunSuite

import java.nio.file.{Files, Paths}

class HtmlReportGeneratorSuite extends FunSuite with MockFactory {

  test("SparkScope end2end no warnings") {
    val ac = mockAppContext()
    val csvReaderMock = stub[HadoopFileReader]
    mockcorrectMetrics(csvReaderMock)
    val executorMetricsAnalyzer = new SparkScopeAnalyzer
    val result = executorMetricsAnalyzer.analyze(DriverExecutorMetricsMock, ac).copy(warnings = Seq.empty)

    HtmlReportGenerator.generateHtml(result, "./", Seq("Executor Timeline", "Sparkscope text"), sparkConf)

    assert(Files.exists(Paths.get("./" + result.appInfo.applicationID + ".html")))
  }

  test("SparkScope end2end with warnings") {
    val ac = mockAppContextMissingExecutorMetrics()
    val csvReaderMock = stub[HadoopFileReader]
    mockcorrectMetrics(csvReaderMock)
    val executorMetricsAnalyzer = new SparkScopeAnalyzer
    val result = executorMetricsAnalyzer.analyze(DriverExecutorMetricsMock, ac)

    HtmlReportGenerator.generateHtml(result, "./", Seq("Executor Timeline", "Sparkscope text"), sparkConf)

    assert(Files.exists(Paths.get("./" + result.appInfo.applicationID + ".html")))
  }
}

