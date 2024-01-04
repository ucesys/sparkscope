
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
import com.ucesys.sparkscope.common.{JvmHeapMax, JvmHeapUsage, JvmHeapUsed, JvmNonHeapUsed, SparkScopeLogger}
import com.ucesys.sparkscope.io.reader.HadoopFileReader
import org.apache.commons.lang.SystemUtils
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSuite, GivenWhenThen}

class HadoopMetricReaderSuite extends FunSuite with MockFactory with GivenWhenThen {
    implicit val logger: SparkScopeLogger = stub[SparkScopeLogger]

    val sparkScopeConfHdfs = sparkScopeConf.copy(
        driverMetricsDir = s"hdfs:/tmp/csv-metrics",
        executorMetricsDir = s"hdfs:/tmp/csv-metrics"
    )

    test("HadoopMetricReader driver metrics test") {
        Given("HadoopMetricReader with HadoopFileReader")
        val appContext = mockAppContext("hadoop-metrics-reader-driver")
        val fileReaderMock = mock[HadoopFileReader]
        val metricsReader = new HadoopMetricReader(sparkScopeConfHdfs, fileReaderMock, appContext)

        When("calling HadoopMetricReader.readDriver")
        Then("HadoopFileReader.read should be called with correct path")
        // Skipping for windows due to Paths.get problems for hdfs:/ under windows
        if(SystemUtils.OS_NAME == "Linux") {
            (fileReaderMock.read _).expects(s"hdfs:/tmp/csv-metrics/${appContext.appId}/driver.csv").returns(DriverCsv)
            metricsReader.readDriver
        }
    }

    test("HadoopMetricReader executor metrics test") {
        Given("HadoopMetricReader with HadoopFileReader")
        val appContext = mockAppContext("hadoop-metrics-reader-executor")
        val fileReaderMock = mock[HadoopFileReader]
        val metricsReader = new HadoopMetricReader(sparkScopeConfHdfs, fileReaderMock, appContext)

        When("calling HadoopMetricReader.readDriver")
        Then("HadoopFileReader.read should be called with correct path")
        // Skipping for windows due to Paths.get problems for hdfs:/ under windows
        if(SystemUtils.OS_NAME == "Linux") {
            (fileReaderMock.read _).expects(s"hdfs:/tmp/csv-metrics/${appContext.appId}/1.csv").returns(Exec1Csv)

            metricsReader.readExecutor("1")
        }
    }
}
