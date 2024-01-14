
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

import com.ucesys.sparkscope.SparkScopeAnalyzer.{DriverCsvMetrics, ExecutorCsvMetrics}
import com.ucesys.sparkscope.TestHelpers._
import org.apache.spark.SparkConf
import org.scalamock.scalatest.MockFactory
import org.scalatest.GivenWhenThen
import org.scalatest.FunSuite

class CsvHadoopMetricsLoaderSuite extends FunSuite with MockFactory with GivenWhenThen {
  test("Incorrect csv files test") {
    Given("Some csv metrics for driver and executor contain more rows than others")
    val csvReaderMock = stub[HadoopFileReader]
    mockIncorrectDriverMetrics(csvReaderMock)
    val metricsLoader = new CsvHadoopMetricsLoader(getFileReaderFactoryMock(csvReaderMock), mockAppContext(), sparkScopeConf)

    When("loading metrics")
    val driverExecutorMetrics = metricsLoader.load()

    Then("Driver and Executor Metrics should be loaded")
    assert(driverExecutorMetrics.driverMetrics.length == DriverCsvMetrics.length)
    assert(driverExecutorMetrics.executorMetricsMap("1").length == ExecutorCsvMetrics.length)

    And("CsvHadoopMetricsLoader should ignore extra rows for driver metrics")
    driverExecutorMetrics.driverMetrics.foreach{ metric =>
      assert(metric.numRows == 12)
    }

    And("CsvHadoopMetricsLoader should ignore extra rows for executor metrics")
    driverExecutorMetrics.executorMetricsMap("1").foreach { metric =>
      assert(metric.numRows == 10)
    }
  }

  test("Successful metrics load test") {
    Given("Correctly configured metrics properties path")
    val sparkConf = new SparkConf().set("spark.metrics.conf", MetricsPropertiesPath)
    And("Correct csv files")
    val csvReaderMock = stub[HadoopFileReader]
    mockcorrectMetrics(csvReaderMock)
    val metricsLoader = new CsvHadoopMetricsLoader(getFileReaderFactoryMock(csvReaderMock), mockAppContext(), sparkScopeConf)

    When("loading metrics")
    val driverExecutorMetrics = metricsLoader.load()

    Then("Driver and Executor Metrics should be loaded")
    assert(driverExecutorMetrics.driverMetrics.length == 4)
    assert(driverExecutorMetrics.driverMetrics.length == DriverCsvMetrics.length)
    assert(driverExecutorMetrics.executorMetricsMap.size == 4)
    assert(driverExecutorMetrics.executorMetricsMap.head._2.length == ExecutorCsvMetrics.length)
  }

  test("Missing metrics load test") {
    Given("Correctly configured metrics properties path")
    val sparkConf = new SparkConf().set("spark.metrics.conf", MetricsPropertiesPath)

    And("Csv metrics for 4 out of 5 executors(metrics for last executor are missing)")
    val csvReaderMock = stub[HadoopFileReader]
    mockcorrectMetrics(csvReaderMock)
    val metricsLoader = new CsvHadoopMetricsLoader(getFileReaderFactoryMock(csvReaderMock), mockAppContextMissingExecutorMetrics, sparkScopeConf)

    When("loading metrics")
    val driverExecutorMetrics = metricsLoader.load()

    Then("Driver and Executor Metrics should be loaded")
    assert(driverExecutorMetrics.driverMetrics.length == 4)
    assert(driverExecutorMetrics.driverMetrics.length == DriverCsvMetrics.length)
    assert(driverExecutorMetrics.executorMetricsMap.size == 4)
    assert(driverExecutorMetrics.executorMetricsMap.head._2.length == ExecutorCsvMetrics.length)

    And("Missing Executor Metrics should be skipped")
    assert(driverExecutorMetrics.executorMetricsMap.get("6").isEmpty)
  }
}
