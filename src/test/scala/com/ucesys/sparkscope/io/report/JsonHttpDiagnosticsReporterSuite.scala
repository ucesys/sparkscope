
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

import com.ucesys.sparkscope.TestHelpers._
import com.ucesys.sparkscope.common.{AppContext, SparkScopeLogger}
import com.ucesys.sparkscope.io.http.JsonHttpClient
import com.ucesys.sparkscope.io.report.JsonHttpDiagnosticsReporter
import com.ucesys.sparkscope.metrics.{SparkScopeMetrics, SparkScopeResult}
import com.ucesys.sparkscope.io.report.JsonHttpDiagnosticsReporter.DiagnosticsEndpoint
import com.ucesys.sparkscope.stats.{ClusterCPUStats, ClusterMemoryStats, DriverMemoryStats, ExecutorMemoryStats, SparkScopeStats}
import org.apache.http.client.HttpResponseException
import org.apache.http.conn.HttpHostConnectException
import org.apache.spark.SparkConf
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.{BeforeAndAfterAll, FunSuite, GivenWhenThen}

import java.net.{SocketTimeoutException, UnknownHostException}
import java.nio.file.{Files, Paths}

class JsonHttpDiagnosticsReporterSuite extends FunSuite with MockitoSugar with BeforeAndAfterAll with GivenWhenThen {
    override def beforeAll(): Unit = Files.createDirectories(Paths.get(TestDir))

    implicit val logger: SparkScopeLogger = mock[SparkScopeLogger]

    val appId = "app-123"
    val appStartTime = 1695358644000L
    val appEndTimeSome: Option[Long] = Some(1695358700000L)
    val appEndTimeNone: Option[Long] = None

    val appContextEnded: AppContext = AppContext(appId, appStartTime, appEndTimeSome, Map.empty, Seq.empty)
    val appContextRunning: AppContext = AppContext(appId, appStartTime, appEndTimeNone, Map.empty, Seq.empty)

    val sparkScopeStats: SparkScopeStats = SparkScopeStats(
        driverStats = DriverMemoryStats(
            heapSize = 910,
            maxHeap = 315,
            maxHeapPerc = 0.3465,
            avgHeap = 261,
            avgHeapPerc = 0.28736,
            avgNonHeap = 66,
            maxNonHeap = 69
        ), executorStats = ExecutorMemoryStats(
            heapSize = 800,
            maxHeap = 352,
            maxHeapPerc = 0.44029,
            avgHeap = 215,
            avgHeapPerc = 0.26972,
            avgNonHeap = 44,
            maxNonHeap = 48
        ), clusterMemoryStats = ClusterMemoryStats(
            maxHeap = 840,
            avgHeap = 614,
            maxHeapPerc = 0.4165,
            avgHeapPerc = 0.26972,
            avgHeapWastedPerc = 0.73028,
            executorTimeSecs = 152,
            heapGbHoursAllocated = 0.03299,
            heapGbHoursWasted = 0.02409,
            executorHeapSizeInGb = 0.78125
        ), clusterCPUStats = ClusterCPUStats(
            cpuUtil = 0.55483,
            cpuNotUtil = 0.44517,
            coreHoursAllocated = 0.04222,
            coreHoursWasted = 0.0188,
            executorTimeSecs = 152,
            executorCores = 1
        )
    )

    val expectedStatsJson =
        """
          |{
          |     "driverStats": {
          |         "heapSize":910,
          |         "maxHeap":315,
          |         "maxHeapPerc":0.3465,
          |         "avgHeap":261,
          |         "avgHeapPerc":0.28736,
          |         "avgNonHeap":66,
          |         "maxNonHeap":69
          |     }, "executorStats": {
          |         "heapSize":800,
          |         "maxHeap":352,
          |         "maxHeapPerc": 0.44029,
          |         "avgHeap":215,
          |         "avgHeapPerc":0.26972,
          |         "avgNonHeap":44,
          |         "maxNonHeap":48
          |     }, "clusterMemoryStats": {
          |         "maxHeap":840,
          |         "avgHeap":614,
          |         "maxHeapPerc":0.4165,
          |         "avgHeapPerc":0.26972,
          |         "avgHeapWastedPerc":0.73028,
          |         "executorTimeSecs":152,
          |         "heapGbHoursAllocated":0.03299,
          |         "heapGbHoursWasted":0.02409,
          |         "executorHeapSizeInGb":0.78125
          |     }, "clusterCPUStats":{
          |         "cpuUtil":0.55483,
          |         "cpuNotUtil":0.44517,
          |         "coreHoursAllocated":0.04222,
          |         "coreHoursWasted":0.0188,
          |         "executorTimeSecs":152,
          |         "executorCores":1
          |     }
          | }
          |""".stripMargin

    test("JsonHttpDiagnosticsReporter ended application") {
        Given("SparkConf with driver host set")
        And("With spark.app.name")
        val sparkConfWithDriverHost = (new SparkConf).set("spark.driver.host", "myhost.com").set("spark.app.name", "MySparkApp")

        And("SparkScopeResult of running application")
        val sparkScopeResult = SparkScopeResult(appContextEnded, Seq.empty, sparkScopeStats, mock[SparkScopeMetrics])

        And("JsonHttpDiagnosticsReporter")
        val jsonHttpClientMock = mock[JsonHttpClient]
        val jsonHttpDiagnosticsReporter = new JsonHttpDiagnosticsReporter(
            sparkScopeConf.copy(appName = Some("MyApp"), sparkConf = sparkConfWithDriverHost),
            jsonHttpClientMock
        )

        When("calling HtmlReportGenerator.generate")
        jsonHttpDiagnosticsReporter.report(sparkScopeResult)

        Then("Post request with diagnostics json is sent")
        And("Json contains end timestamp, duration and driverHost")
        And("Json contains stats")
        verify(jsonHttpClientMock, times(1)).post(
            DiagnosticsEndpoint,
            s"""{
              | "appInfo":{
              |     "appId":"app-123",
              |     "sparkAppName":"MySparkApp",
              |     "sparkScopeAppName":"MyApp",
              |     "startTs":1695358644000,
              |     "endTs":1695358700000,
              |     "duration":56000,
              |     "driverHost":"myhost.com"
              | },"stats": ${expectedStatsJson}
              |}""".stripMargin.replaceAll("[\n\r]", "").replace(" ", "")
        )
    }

    test("JsonHttpDiagnosticsReporter running application") {
        Given("SparkConf without driver host set")
        And("Without spark.app.name")
        val sparkConf = new SparkConf

        And("SparkScopeResult of running application")
        val sparkScopeResult = SparkScopeResult(appContextRunning, Seq.empty, sparkScopeStats, mock[SparkScopeMetrics])

        And("JsonHttpDiagnosticsReporter")
        val jsonHttpClientMock = mock[JsonHttpClient]
        val jsonHttpDiagnosticsReporter = new JsonHttpDiagnosticsReporter(
            sparkScopeConf.copy(appName = Some("MyApp"), sparkConf = sparkConf),
            jsonHttpClientMock
        )

        When("calling jsonHttpDiagnosticsReporter.report")
        jsonHttpDiagnosticsReporter.report(sparkScopeResult)

        Then("Post request with diagnostics json is sent")
        And("Json does not contain end timestamp, nor duration, nor  driverHost")
        And("Json contains stats")
        verify(jsonHttpClientMock, times(1)).post(
            DiagnosticsEndpoint,
            s"""{
               | "appInfo":{
               |     "appId":"app-123",
               |     "sparkScopeAppName":"MyApp",
               |     "startTs":1695358644000
               | },"stats": ${expectedStatsJson}
               |}""".stripMargin.replaceAll("[\n\r]", "").replace(" ", "")
        )
    }

    test("JsonHttpDiagnosticsReporter UnknownHostException") {
        Given("SparkScopeResult of running application")
        val sparkScopeResult = SparkScopeResult(appContextRunning, Seq.empty, sparkScopeStats, mock[SparkScopeMetrics])

        And("JsonHttpClient throwing UnknownHostException")
        val jsonHttpClientMock = mock[JsonHttpClient]
        doAnswer(() => throw new UnknownHostException("java.net.UnknownHostException: myhost: Temporary failure in name resolution"))
          .when(jsonHttpClientMock)
          .post(any[String], any[String],  any[Int])
        val loggerMock = mock[SparkScopeLogger]

        val jsonHttpDiagnosticsReporter = new JsonHttpDiagnosticsReporter(
            sparkScopeConf.copy(appName = Some("MyApp")),
            jsonHttpClientMock,
            "http://sparkscope.ai/diagnostics"
        )(loggerMock)

        When("calling jsonHttpDiagnosticsReporter.report")
        jsonHttpDiagnosticsReporter.report(sparkScopeResult)

        Then("Exception is caught")
        And("Warning is logged")
        verify(loggerMock, times(1)).warn(
            "java.net.UnknownHostException: java.net.UnknownHostException: myhost: Temporary failure in name resolution",
            jsonHttpDiagnosticsReporter.getClass,
            false
        )
    }

    test("JsonHttpDiagnosticsReporter Connection refused") {
        Given("SparkScopeResult of running application")
        val sparkScopeResult = SparkScopeResult(appContextRunning, Seq.empty, sparkScopeStats, mock[SparkScopeMetrics])

        And("JsonHttpClient throwing HttpHostConnectException")
        val jsonHttpClientMock = mock[JsonHttpClient]
        val exceptionMock = mock[HttpHostConnectException]
        doReturn("org.apache.http.conn.HttpHostConnectException: Connect to localhost:80 [localhost/127.0.0.1] failed: Connection refused (Connection refused)")
          .when(exceptionMock)
          .toString

        doAnswer(() => throw exceptionMock)
          .when(jsonHttpClientMock)
          .post(any[String], any[String], any[Int])
        val loggerMock = mock[SparkScopeLogger]

        val jsonHttpDiagnosticsReporter = new JsonHttpDiagnosticsReporter(
            sparkScopeConf.copy(appName = Some("MyApp")),
            jsonHttpClientMock,
            "http://sparkscope.ai/diagnostics"
        )(loggerMock)

        When("calling jsonHttpDiagnosticsReporter.report")
        jsonHttpDiagnosticsReporter.report(sparkScopeResult)

        Then("Exception is caught")
        And("Warning is logged")
        verify(loggerMock, times(1)).warn(
            "org.apache.http.conn.HttpHostConnectException: Connect to localhost:80 [localhost/127.0.0.1] failed: Connection refused (Connection refused)",
            jsonHttpDiagnosticsReporter.getClass,
            false
        )
    }

    test("JsonHttpDiagnosticsReporter Timeout") {
        Given("SparkScopeResult of running application")
        val sparkScopeResult = SparkScopeResult(appContextRunning, Seq.empty, sparkScopeStats, mock[SparkScopeMetrics])

        And("JsonHttpClient throwing SocketTimeoutException")
        val jsonHttpClientMock = mock[JsonHttpClient]

        doAnswer(() => throw new SocketTimeoutException("Read timed out"))
          .when(jsonHttpClientMock)
          .post(any[String], any[String], any[Int])
        val loggerMock = mock[SparkScopeLogger]

        val jsonHttpDiagnosticsReporter = new JsonHttpDiagnosticsReporter(
            sparkScopeConf.copy(appName = Some("MyApp")),
            jsonHttpClientMock,
            "http://sparkscope.ai/diagnostics"
        )(loggerMock)

        When("calling jsonHttpDiagnosticsReporter.report")
        jsonHttpDiagnosticsReporter.report(sparkScopeResult)

        Then("Exception is caught")
        And("Warning is logged")
        verify(loggerMock, times(1)).warn(
            "java.net.SocketTimeoutException: Read timed out",
            jsonHttpDiagnosticsReporter.getClass,
            false
        )
    }

    test("JsonHttpDiagnosticsReporter HttpResponseException") {
        Given("SparkScopeResult of running application")
        val sparkScopeResult = SparkScopeResult(appContextRunning, Seq.empty, sparkScopeStats, mock[SparkScopeMetrics])

        And("JsonHttpClient throwing SocketTimeoutException")
        val jsonHttpClientMock = mock[JsonHttpClient]

        doAnswer(() => throw new HttpResponseException(307, "307 Temporary Redirect"))
          .when(jsonHttpClientMock)
          .post(any[String], any[String], any[Int])
        val loggerMock = mock[SparkScopeLogger]

        val jsonHttpDiagnosticsReporter = new JsonHttpDiagnosticsReporter(
            sparkScopeConf.copy(appName = Some("MyApp")),
            jsonHttpClientMock,
            "http://sparkscope.ai/diagnostics"
        )(loggerMock)

        When("calling jsonHttpDiagnosticsReporter.report")
        jsonHttpDiagnosticsReporter.report(sparkScopeResult)

        Then("Exception is caught")
        And("Warning is logged")
        verify(loggerMock, times(1)).warn(
            "org.apache.http.client.HttpResponseException: 307 Temporary Redirect",
            jsonHttpDiagnosticsReporter.getClass,
            false
        )
    }
}
