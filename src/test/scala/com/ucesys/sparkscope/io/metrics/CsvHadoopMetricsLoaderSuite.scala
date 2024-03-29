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

package com.ucesys.sparkscope.io.metrics

import com.ucesys.sparkscope.TestHelpers._
import com.ucesys.sparkscope.common.MetricUtils.{DriverCsvColumns, ExecutorCsvColumns}
import com.ucesys.sparkscope.common.SparkScopeLogger
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSuite, GivenWhenThen}

class CsvHadoopMetricsLoaderSuite extends FunSuite with MockFactory with GivenWhenThen {
    implicit val logger: SparkScopeLogger = stub[SparkScopeLogger]

    test("Successful metrics load test") {
        Given("Correctly configured metrics properties path")
        And("Correct csv files")
        val csvReaderMock = stub[HadoopMetricReader]
        val appContext = mockAppContext(SampleAppId, "csv-loader-successful")
        mockcorrectMetrics(csvReaderMock, appContext.appId)
        val metricsLoader = new CsvMetricsLoader(csvReaderMock)

        When("loading metrics")
        val driverExecutorMetrics = metricsLoader.load(appContext, sparkScopeConf)

        Then("Driver and Executor Metrics should be loaded")
        assert(driverExecutorMetrics.driverMetrics.columns.length == DriverCsvColumns.length)
        assert(driverExecutorMetrics.driverMetrics.header == DriverCsvColumns.mkString(","))
        assert(driverExecutorMetrics.driverMetrics.numRows == 13)
        assert(driverExecutorMetrics.executorMetricsMap.size == 4)
        assert(driverExecutorMetrics.executorMetricsMap.head._2.columns.length == ExecutorCsvColumns.length)
        assert(driverExecutorMetrics.executorMetricsMap.head._2.header == ExecutorCsvColumns.mkString(","))
        assert(driverExecutorMetrics.executorMetricsMap("1").numRows == 11)
        assert(driverExecutorMetrics.executorMetricsMap("2").numRows == 11)
        assert(driverExecutorMetrics.executorMetricsMap("3").numRows == 6)
        assert(driverExecutorMetrics.executorMetricsMap("5").numRows == 3)
    }

    test("Missing metrics load test") {
        Given("Correctly configured metrics properties path")
        And("Csv metrics for 4 out of 5 executors(metrics for last executor are missing)")
        val csvReaderMock = stub[HadoopMetricReader]
        val appContext = mockAppContextMissingExecutorMetrics(SampleAppId, "csv-loader-missing-metrics")
        mockcorrectMetrics(csvReaderMock, appContext.appId)
        val metricsLoader = new CsvMetricsLoader(csvReaderMock)

        When("loading metrics")
        val driverExecutorMetrics = metricsLoader.load(appContext, sparkScopeConf)

        Then("Driver and Executor Metrics should be loaded")
        assert(driverExecutorMetrics.driverMetrics.columns.length == DriverCsvColumns.length)
        assert(driverExecutorMetrics.driverMetrics.header == DriverCsvColumns.mkString(","))
        assert(driverExecutorMetrics.driverMetrics.numRows == 13)

        assert(driverExecutorMetrics.executorMetricsMap.size == 4)
        assert(driverExecutorMetrics.executorMetricsMap.head._2.columns.length == ExecutorCsvColumns.length)
        assert(driverExecutorMetrics.executorMetricsMap.head._2.header == ExecutorCsvColumns.mkString(","))
        assert(driverExecutorMetrics.executorMetricsMap("1").numRows == 11)
        assert(driverExecutorMetrics.executorMetricsMap("2").numRows == 11)
        assert(driverExecutorMetrics.executorMetricsMap("3").numRows == 6)
        assert(driverExecutorMetrics.executorMetricsMap("5").numRows == 3)

        And("Missing Executor Metrics should be skipped")
        assert(driverExecutorMetrics.executorMetricsMap.get("6").isEmpty)
    }
}
