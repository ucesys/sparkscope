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

import com.ucesys.sparkscope.SparkScopeConfLoader._
import com.ucesys.sparkscope.TestHelpers._
import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.io.property.{PropertiesLoader, PropertiesLoaderFactory}
import org.apache.spark.SparkConf
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSuite, GivenWhenThen}

import java.util.Properties

class SparkScopeConfLoaderSuite extends FunSuite with MockFactory with GivenWhenThen {

    implicit val logger: SparkScopeLogger = stub[SparkScopeLogger]

    test("extracting driver & executor metrics path from spark.sparkscope") {
        Given("SparkConf")
        And("with spark.sparkscope.metrics.dir.driver set")
        And("with spark.sparkscope.metrics.dir.executor set")
        And("with spark.metrics.conf.*.sink.csv.director set")
        And("with spark.metrics.conf.driver.sink.csv.director set")
        And("with spark.metrics.conf.executor.sink.csv.director set")
        And("with spark.metrics.conf set")
        And("with region set")
        And("with spark.metrics.conf.*.sink.csv.appName set")
        And("with spark.sparkscope.diagnostics.enabled=false set")
        val sparkConfWithMetrics = new SparkConf()
          .set(SparkScopePropertyDriverMetricsDir, "/sparkscope/path/to/driver/metrics")
          .set(SparkScopePropertyExecutorMetricsDir, "/sparkscope/path/to/executor/metrics")
          .set("spark.metrics.conf", MetricsPropertiesPath)
          .set("spark.metrics.conf.*.sink.csv.directory", "/spark/metric/path/to/all/metrics")
          .set("spark.metrics.conf.driver.sink.csv.directory ", "/spark/metrics/path/to/driver/metrics")
          .set("spark.metrics.conf.executor.sink.csv.directory ", "/spark/metrics/path/to/executor/metrics")
          .set("spark.metrics.conf.*.sink.csv.region", "us-east-1")
          .set("spark.metrics.conf.*.sink.csv.appName", "my-sample-app")
          .set("spark.sparkscope.diagnostics.enabled", "false")

        val propertiesLoaderFactoryMock = mock[PropertiesLoaderFactory]

        When("loading SparkScope config")
        val sparkScopeConfLoader = new SparkScopeConfLoader
        val sparkScopeConf = sparkScopeConfLoader.load(sparkConfWithMetrics, propertiesLoaderFactoryMock)

        Then("SparkScopeConf.driverMetricsDir should be extracted from spark.sparkscope.metrics.dir.driver")
        assert(sparkScopeConf.driverMetricsDir == "/sparkscope/path/to/driver/metrics")

        And("SparkScopeConf.executorMetricsDir should be extracted from spark.sparkscope.metrics.dir.executor")
        assert(sparkScopeConf.executorMetricsDir == "/sparkscope/path/to/executor/metrics")

        And("metrics properties should not be loaded")
        (propertiesLoaderFactoryMock.getPropertiesLoader _).expects(*).never

        And("SparkScopeConf.appName should be parsed from spark.sparkscope.appName")
        assert(sparkScopeConf.appName.get == "my-sample-app")

        And("SparkScopeConf.region should be parsed")
        assert(sparkScopeConf.region.get == "us-east-1")

        And("SparkScopeConf.sendDiagnostics should be false(disabled)")
        assert(sparkScopeConf.diagnosticsUrl.isEmpty)
    }

    test("extracting driver & executor metrics path from spark.metrics.conf.[driver|executor]") {
        Given("SparkConf")
        And("with spark.metrics.conf.driver.sink.csv.director set")
        And("with spark.metrics.conf.executor.sink.csv.director set")
        And("with spark.metrics.conf set")
        And("with spark.metrics.conf.*.sink.csv.appName set")
        And("with region set")
        And("without spark.sparkscope.metrics.dir.driver set")
        And("without spark.sparkscope.metrics.dir.executor set")
        And("without spark.metrics.conf.*.sink.csv.director set")
        And("without spark.sparkscope.diagnostics.enabled=false set")

        val sparkConfWithMetrics = new SparkConf()
          .set("spark.metrics.conf", MetricsPropertiesPath)
          .set("spark.metrics.conf.driver.sink.csv.directory", "/spark/metrics/path/to/driver/metrics")
          .set("spark.metrics.conf.executor.sink.csv.directory", "/spark/metrics/path/to/executor/metrics")
          .set("spark.metrics.conf.*.sink.csv.region", "us-east-1")
          .set("spark.metrics.conf.*.sink.csv.appName", "my-sample-app")

        val propertiesLoaderFactoryMock = mock[PropertiesLoaderFactory]

        When("loading SparkScope config")
        val sparkScopeConfLoader = new SparkScopeConfLoader
        val sparkScopeConf = sparkScopeConfLoader.load(sparkConfWithMetrics, propertiesLoaderFactoryMock)

        Then("SparkScopeConf.driverMetricsDir should be extracted from spark.metrics.conf.driver.sink.csv.directory")
        assert(sparkScopeConf.driverMetricsDir == "/spark/metrics/path/to/driver/metrics")

        And("SparkScopeConf.executorMetricsDir should be extracted from spark.metrics.conf.executor.sink.csv.directory")
        assert(sparkScopeConf.executorMetricsDir == "/spark/metrics/path/to/executor/metrics")

        And("metrics properties should not be loaded")
        (propertiesLoaderFactoryMock.getPropertiesLoader _).expects(*).never

        And("SparkScopeConf.appName should be parsed")
        assert(sparkScopeConf.appName.get == "my-sample-app")

        And("SparkScopeConf.region should be parsed")
        assert(sparkScopeConf.region.get == "us-east-1")

        And("SparkScopeConf.sendDiagnostics should be true(enabled)")
        assert(sparkScopeConf.diagnosticsUrl.get == DiagnosticsEndpoint)
    }

    test("extracting driver & executor metrics path from spark.metrics.conf.*") {
        Given("SparkConf")
        And("with spark.metrics.conf.*.sink.csv.director set")
        And("with spark.metrics.conf set")
        And("with spark.sparkscope.diagnostics.enabled=true set")
        And("without spark.sparkscope.metrics.dir.driver set")
        And("without spark.sparkscope.metrics.dir.executor set")
        And("without spark.metrics.conf.driver.sink.csv.director set")
        And("without spark.metrics.conf.executor.sink.csv.director set")
        val sparkConfWithMetrics = new SparkConf()
          .set("spark.metrics.conf", MetricsPropertiesPath)
          .set("spark.metrics.conf.*.sink.csv.directory", "/spark/metrics/path/to/all/metrics")
          .set("spark.sparkscope.diagnostics.enabled", "true")

        val propertiesLoaderFactoryMock = mock[PropertiesLoaderFactory]

        When("loading SparkScope config")
        val sparkScopeConfLoader = new SparkScopeConfLoader
        val sparkScopeConf = sparkScopeConfLoader.load(sparkConfWithMetrics, propertiesLoaderFactoryMock)

        Then("SparkScopeConf.driverMetricsDir should be extracted from spark.metrics.conf.*.sink.csv.directory")
        assert(sparkScopeConf.driverMetricsDir == "/spark/metrics/path/to/all/metrics")

        And("SparkScopeConf.executorMetricsDir should be extracted from spark.metrics.conf.*.sink.csv.directory")
        assert(sparkScopeConf.executorMetricsDir == "/spark/metrics/path/to/all/metrics")

        And("metrics properties should not be loaded")
        (propertiesLoaderFactoryMock.getPropertiesLoader _).expects(*).never

        And("SparkScopeConf.appName should be empty")
        assert(sparkScopeConf.appName.isEmpty)

        And("SparkScopeConf.region should be empty")
        assert(sparkScopeConf.region.isEmpty)

        And("SparkScopeConf.sendDiagnostics should be true(enabled)")
        assert(sparkScopeConf.diagnosticsUrl.get == DiagnosticsEndpoint)
    }

    test("extracting driver & executor metrics path from metrics.properties file") {
        Given("SparkConf")
        And("with spark.metrics.conf set")
        And("without spark.sparkscope.metrics.dir.driver set")
        And("without spark.sparkscope.metrics.dir.executor set")
        And("without spark.metrics.conf.driver.sink.csv.director set")
        And("without spark.metrics.conf.executor.sink.csv.director set")
        And("without spark.metrics.conf.*.sink.csv.director set")
        val sparkConfWithMetrics = new SparkConf()
          .set("spark.metrics.conf", MetricsPropertiesPath)

        When("loading SparkScope config")
        val sparkScopeConfLoader = new SparkScopeConfLoader
        val sparkScopeConf = sparkScopeConfLoader.load(sparkConfWithMetrics, getPropertiesLoaderFactoryMock)

        Then("SparkScopeConf.driverMetricsDir should be extracted from metrics.properties file")
        assert(sparkScopeConf.driverMetricsDir == "/tmp/csv-metrics")

        And("SparkScopeConf.executorMetricsDir should be extracted from metrics.properties file")
        assert(sparkScopeConf.executorMetricsDir == "/tmp/csv-metrics")

        And("SparkScopeConf.appName should be empty")
        assert(sparkScopeConf.appName.isEmpty)

        And("SparkScopeConf.region should be empty")
        assert(sparkScopeConf.region.isEmpty)
    }

    test("error extracting driver & executor metrics path, metrics.properties unset") {
        Given("SparkConf")
        And("without spark.sparkscope.metrics.dir.driver set")
        And("without spark.sparkscope.metrics.dir.executor set")
        And("without spark.metrics.conf.driver.sink.csv.director set")
        And("without spark.metrics.conf.executor.sink.csv.director set")
        And("without spark.metrics.conf.*.sink.csv.director set")
        And("without spark.metrics.conf")
        val sparkConf = new SparkConf()

        When("loading metrics")
        Then("SparkScopeConf should throw IllegalArgumentException")
        assertThrows[IllegalArgumentException] {
            val sparkScopeConfLoader = new SparkScopeConfLoader
            val sparkScopeConf = sparkScopeConfLoader.load(sparkConf, getPropertiesLoaderFactoryMock)
        }
    }

    test("error extracting driver & executor metrics path, metrics.properties file doesn't exist") {
        Given("SparkConf")
        And("with incorrect spark.metrics.conf setting")
        And("without spark.sparkscope.metrics.dir.driver set")
        And("without spark.sparkscope.metrics.dir.executor set")
        And("without spark.metrics.conf.driver.sink.csv.director set")
        And("without spark.metrics.conf.executor.sink.csv.director set")
        And("without spark.metrics.conf.*.sink.csv.director set")
        val sparkConf = new SparkConf()
          .set("spark.metrics.conf", "/bad/path/to/metrics.properties")

        When("loading metrics")
        Then("SparkScopeConf should throw java.io.FileNotFoundException")
        assertThrows[IllegalArgumentException] {
            val sparkScopeConfLoader = new SparkScopeConfLoader
            val sparkScopeConf = sparkScopeConfLoader.load(sparkConf, new PropertiesLoaderFactory)
        }
    }

    test("error extracting driver metrics path, driver sink not configured") {
        Given("SparkConf")
        And("with spark.metrics.conf")
        And("without spark.sparkscope.metrics.dir.driver set")
        And("without spark.sparkscope.metrics.dir.executor set")
        And("without spark.metrics.conf.driver.sink.csv.director set")
        And("without spark.metrics.conf.executor.sink.csv.director set")
        And("without spark.metrics.conf.*.sink.csv.director set")
        val sparkConf = new SparkConf().set("spark.metrics.conf", MetricsPropertiesPath)

        And("Driver sink is not configured")
        val propertiesLoaderMock = stub[PropertiesLoader]
        val properties = new Properties()
        properties.setProperty("executor.sink.csv.directory", csvMetricsPath)
        (propertiesLoaderMock.load _).when().returns(properties)
        val propertiesLoaderFactoryMock = stub[PropertiesLoaderFactory]
        (propertiesLoaderFactoryMock.getPropertiesLoader _).when(*).returns(propertiesLoaderMock)

        When("loading metrics")
        Then("CsvHadoopMetricsLoader should throw NoSuchFieldException")
        assertThrows[IllegalArgumentException] {
            val sparkScopeConfLoader = new SparkScopeConfLoader
            val sparkScopeConf = sparkScopeConfLoader.load(sparkConf, propertiesLoaderFactoryMock)
        }
    }

    test("error extracting executor metrics path, executor sink not configured") {
        Given("SparkConf")
        And("with spark.metrics.conf")
        And("without spark.sparkscope.metrics.dir.driver set")
        And("without spark.sparkscope.metrics.dir.executor set")
        And("without spark.metrics.conf.driver.sink.csv.director set")
        And("without spark.metrics.conf.executor.sink.csv.director set")
        And("without spark.metrics.conf.*.sink.csv.director set")
        val sparkConf = new SparkConf().set("spark.metrics.conf", MetricsPropertiesPath)

        And("Executor sink is not configured")
        val propertiesLoaderMock = stub[PropertiesLoader]
        val properties = new Properties()
        properties.setProperty("driver.sink.csv.directory", csvMetricsPath)
        (propertiesLoaderMock.load _).when().returns(properties)
        val propertiesLoaderFactoryMock = stub[PropertiesLoaderFactory]
        (propertiesLoaderFactoryMock.getPropertiesLoader _).when(*).returns(propertiesLoaderMock)

        When("loading metrics")
        Then("SparkScopeConf should throw NoSuchFieldException")
        assertThrows[IllegalArgumentException] {
            val sparkScopeConfLoader = new SparkScopeConfLoader
            val sparkScopeConf = sparkScopeConfLoader.load(sparkConf, propertiesLoaderFactoryMock)
        }
    }

    // MIXED CASES
    test("extracting driver from spark.sparkscope, executor from spark.metrics.conf.executor") {
        Given("SparkConf")
        And("with spark.sparkscope.metrics.dir.driver set")
        And("with spark.metrics.conf.driver.sink.csv.director set")
        And("with spark.metrics.conf.executor.sink.csv.director set")
        And("with spark.metrics.conf set")
        And("without spark.sparkscope.metrics.dir.executor set")
        val sparkConfWithMetrics = new SparkConf()
          .set("spark.metrics.conf", MetricsPropertiesPath)
          .set(SparkScopePropertyDriverMetricsDir, "/sparkscope/path/to/driver/metrics")
          .set("spark.metrics.conf.driver.sink.csv.directory", "/spark/metrics/path/to/driver/metrics")
          .set("spark.metrics.conf.executor.sink.csv.directory", "/spark/metrics/path/to/executor/metrics")

        val propertiesLoaderFactoryMock = mock[PropertiesLoaderFactory]

        When("loading SparkScope config")
        val sparkScopeConfLoader = new SparkScopeConfLoader
        val sparkScopeConf = sparkScopeConfLoader.load(sparkConfWithMetrics, propertiesLoaderFactoryMock)

        Then("SparkScopeConf.driverMetricsDir should be extracted from spark.sparkscope.metrics.dir.driver")
        assert(sparkScopeConf.driverMetricsDir == "/sparkscope/path/to/driver/metrics")

        And("SparkScopeConf.executorMetricsDir should be extracted from spark.metrics.conf.executor.sink.csv.directory")
        assert(sparkScopeConf.executorMetricsDir == "/spark/metrics/path/to/executor/metrics")

        And("metrics properties should not be loaded")
        (propertiesLoaderFactoryMock.getPropertiesLoader _).expects(*).never

        And("SparkScopeConf.appName should be empty")
        assert(sparkScopeConf.appName.isEmpty)

        And("SparkScopeConf.region should be empty")
        assert(sparkScopeConf.region.isEmpty)
    }

    test("extracting executor from spark.sparkscope, driver from spark.metrics.conf.driver") {
        Given("SparkConf")
        And("with spark.sparkscope.metrics.dir.executor set")
        And("with spark.metrics.conf.driver.sink.csv.director set")
        And("with spark.metrics.conf.executor.sink.csv.director set")
        And("with spark.metrics.conf set")
        And("without spark.sparkscope.metrics.dir.executor set")
        val sparkConfWithMetrics = new SparkConf()
          .set("spark.metrics.conf", MetricsPropertiesPath)
          .set(SparkScopePropertyExecutorMetricsDir, "/sparkscope/path/to/executor/metrics")
          .set("spark.metrics.conf.driver.sink.csv.directory", "/spark/metrics/path/to/driver/metrics")
          .set("spark.metrics.conf.executor.sink.csv.directory", "/spark/metrics/path/to/executor/metrics")

        val propertiesLoaderFactoryMock = mock[PropertiesLoaderFactory]

        When("loading SparkScope config")
        val sparkScopeConfLoader = new SparkScopeConfLoader
        val sparkScopeConf = sparkScopeConfLoader.load(sparkConfWithMetrics, propertiesLoaderFactoryMock)

        Then("SparkScopeConf.driverMetricsDir should be extracted from spark.sparkscope.metrics.dir.driver")
        assert(sparkScopeConf.driverMetricsDir == "/spark/metrics/path/to/driver/metrics")

        And("SparkScopeConf.executorMetricsDir should be extracted from spark.metrics.conf.executor.sink.csv.directory")
        assert(sparkScopeConf.executorMetricsDir == "/sparkscope/path/to/executor/metrics")

        And("metrics properties should not be loaded")
        (propertiesLoaderFactoryMock.getPropertiesLoader _).expects(*).never

        And("SparkScopeConf.appName should be empty")
        assert(sparkScopeConf.appName.isEmpty)

        And("SparkScopeConf.region should be empty")
        assert(sparkScopeConf.region.isEmpty)
    }

    test("extracting driver from spark.metrics.conf.driver, executor from metrics.properties file") {
        Given("SparkConf")
        And("with spark.metrics.conf.driver.sink.csv.director set")
        And("with spark.metrics.conf set")
        And("without spark.metrics.conf.executor.sink.csv.director set")
        And("without spark.sparkscope.metrics.dir.driver set")
        And("without spark.sparkscope.metrics.dir.executor set")
        val sparkConfWithMetrics = new SparkConf()
          .set("spark.metrics.conf", MetricsPropertiesPath)
          .set("spark.metrics.conf.driver.sink.csv.directory", "/spark/metrics/path/to/driver/metrics")

        When("loading SparkScope config")
        val sparkScopeConfLoader = new SparkScopeConfLoader
        val sparkScopeConf = sparkScopeConfLoader.load(sparkConfWithMetrics, getPropertiesLoaderFactoryMock)

        And("SparkScopeConf.driverMetricsDir should be extracted from spark.metrics.conf.driver.sink.csv.directory")
        assert(sparkScopeConf.driverMetricsDir == "/spark/metrics/path/to/driver/metrics")

        Then("SparkScopeConf.executorMetricsDir should be extracted from metrics.properties file")
        assert(sparkScopeConf.executorMetricsDir == "/tmp/csv-metrics")

        And("SparkScopeConf.appName should be empty")
        assert(sparkScopeConf.appName.isEmpty)

        And("SparkScopeConf.region should be empty")
        assert(sparkScopeConf.region.isEmpty)
    }

    test("extracting executor from spark.metrics.conf.executor, driver from metrics.properties file") {
        Given("SparkConf")
        And("with spark.metrics.conf.driver.sink.csv.director set")
        And("with spark.metrics.conf set")
        And("without spark.metrics.conf.executor.sink.csv.director set")
        And("without spark.sparkscope.metrics.dir.driver set")
        And("without spark.sparkscope.metrics.dir.executor set")
        val sparkConfWithMetrics = new SparkConf()
          .set("spark.metrics.conf", MetricsPropertiesPath)
          .set("spark.metrics.conf.executor.sink.csv.directory", "/spark/metrics/path/to/executor/metrics")

        When("loading SparkScope config")
        val sparkScopeConfLoader = new SparkScopeConfLoader
        val sparkScopeConf = sparkScopeConfLoader.load(sparkConfWithMetrics, getPropertiesLoaderFactoryMock)

        Then("SparkScopeConf.driverMetricsDir should be extracted from metrics.properties file")
        assert(sparkScopeConf.driverMetricsDir == "/tmp/csv-metrics")

        And("SparkScopeConf.executorMetricsDir should be extracted from spark.metrics.conf.executor.sink.csv.directory")
        assert(sparkScopeConf.executorMetricsDir == "/spark/metrics/path/to/executor/metrics")

        And("SparkScopeConf.appName should be empty")
        assert(sparkScopeConf.appName.isEmpty)

        And("SparkScopeConf.region should be empty")
        assert(sparkScopeConf.region.isEmpty)
    }

    test("extract load html dir from SparkConf") {
        Given("SparkConf with html path set")
        val sparkConf = new SparkConf()
          .set("spark.metrics.conf", MetricsPropertiesPath)
          .set("spark.sparkscope.report.html.path", "/path/to/html/report")

        When("loading SparkScope config")
        val sparkScopeConfLoader = new SparkScopeConfLoader
        val sparkScopeConf = sparkScopeConfLoader.load(sparkConf, getPropertiesLoaderFactoryMock)

        Then("SparkScopeConf should contain executorMetricsDir")
        assert(sparkScopeConf.htmlReportPath.get == "/path/to/html/report")
    }

    test("extract load json dir from SparkConf") {
        Given("SparkConf with json path set")
        val sparkConf = new SparkConf()
          .set("spark.metrics.conf", MetricsPropertiesPath)
          .set("spark.sparkscope.report.json.path", "/path/to/json/report")

        When("loading SparkScope config")
        val sparkScopeConfLoader = new SparkScopeConfLoader
        val sparkScopeConf = sparkScopeConfLoader.load(sparkConf, getPropertiesLoaderFactoryMock)

        Then("SparkScopeConf should contain executorMetricsDir")
        assert(sparkScopeConf.jsonReportPath.get == "/path/to/json/report")
    }

    test("extract load json server from SparkConf") {
        Given("SparkConf with json path set")
        val sparkConf = new SparkConf()
          .set("spark.metrics.conf", MetricsPropertiesPath)
          .set("spark.sparkscope.report.json.server", "http://sparkscope.ai/diagnostics")

        When("loading SparkScope config")
        val sparkScopeConfLoader = new SparkScopeConfLoader
        val sparkScopeConf = sparkScopeConfLoader.load(sparkConf, getPropertiesLoaderFactoryMock)

        Then("SparkScopeConf should contain executorMetricsDir")
        assert(sparkScopeConf.jsonReportServer.get == "http://sparkscope.ai/diagnostics")
    }

    test("extract driver and executor memoryOverhead from SparkConf") {
        Given("driver and executor memory overhead set in SparkConf")
        val sparkScopeConfLoader = new SparkScopeConfLoader
        val sparkConfMemOverhead = new SparkConf()
          .set("spark.metrics.conf", MetricsPropertiesPath)
          .set(SparkScopePropertyExecMemOverhead, "1500m")
          .set(SparkScopePropertyDriverMemOverhead, "1g");

        When("loading SparkScope config")
        val sparkScopeConf = sparkScopeConfLoader.load(
            sparkConfMemOverhead,
            getPropertiesLoaderFactoryMock
        )

        Then("driver and executor memory overhead should be extracted from SparkConf")
        assert(sparkScopeConf.executorMemOverhead.toMB == 1500)
        assert(sparkScopeConf.driverMemOverhead.toMB == 1024)
    }

    test("getMemoryOverhead, custom overhead") {
        Given("executor memory overhead set in SparkConf")
        val sparkScopeConfLoader = new SparkScopeConfLoader
        val sparkConfMemOverhead = new SparkConf().set(SparkScopePropertyExecMemOverhead, "1500m");

        When("loading SparkScope config")
        val memorySize = sparkScopeConfLoader.getMemoryOverhead(
            sparkConfMemOverhead,
            SparkScopePropertyExecMem,
            SparkScopePropertyExecMemOverhead,
            SparkScopePropertyExecMemOverheadFactor
        )

        Then("executor memory overhead should be extracted from SparkConf")
        assert(memorySize.toMB == 1500)
    }

    test("getMemoryOverhead, custom bad(less than minimum) overhead") {
        Given("too small executor memory overhead set in SparkConf")
        val sparkScopeConfLoader = new SparkScopeConfLoader
        val sparkConfMemOverhead = new SparkConf()
          .set(SparkScopePropertyExecMemOverhead, "300m");

        When("loading SparkScope config")
        val memorySize = sparkScopeConfLoader.getMemoryOverhead(
            sparkConfMemOverhead,
            SparkScopePropertyExecMem,
            SparkScopePropertyExecMemOverhead,
            SparkScopePropertyExecMemOverheadFactor
        )

        Then("executor memory overhead should be set to minimum")
        assert(memorySize.toMB == 384)
    }

    test("getMemoryOverhead, default overhead, overhead=minimum(384)") {
        Given("not set executor memory overhead in SparkConf")
        And("executor memory * fraction is < 384 MB")
        val sparkScopeConfLoader = new SparkScopeConfLoader
        val sparkConfMemOverhead = new SparkConf().set(SparkScopePropertyExecMem, "3000m");

        When("loading SparkScope config")
        val memorySize = sparkScopeConfLoader.getMemoryOverhead(
            sparkConfMemOverhead,
            SparkScopePropertyExecMem,
            SparkScopePropertyExecMemOverhead,
            SparkScopePropertyExecMemOverheadFactor
        )

        Then("executor memory overhead should be set to minimum(384)")
        assert(memorySize.toMB == 384)
    }

    test("getMemoryOverhead, default overhead, overhead=fraction(10%)*executor.memory") {
        Given("not set executor memory overhead in SparkConf")
        And("executor memory * fraction is >= 384 MB")
        val sparkScopeConfLoader = new SparkScopeConfLoader
        val sparkConfMemOverhead = new SparkConf().set(SparkScopePropertyExecMem, "5000m")

        When("loading SparkScope config")
        val memorySize = sparkScopeConfLoader.getMemoryOverhead(
            sparkConfMemOverhead,
            SparkScopePropertyExecMem,
            SparkScopePropertyExecMemOverhead,
            SparkScopePropertyExecMemOverheadFactor
        )

        Then("executor memory overhead should be set 10% of executor.memory")
        assert(memorySize.toMB == 500)
    }

    test("getMemoryOverhead, default overhead, overhead=fraction(20%)*executor.memory") {
        Given("not set executor memory overhead in SparkConf")
        And("executor memory * fraction is >= 384 MB and memoryOverheadFraction is set")
        val sparkScopeConfLoader = new SparkScopeConfLoader
        val sparkConfMemOverhead = new SparkConf()
          .set(SparkScopePropertyExecMem, "5000m")
          .set(SparkScopePropertyExecMemOverheadFactor, s"0.2")

        When("loading SparkScope config")
        val memorySize = sparkScopeConfLoader.getMemoryOverhead(
            sparkConfMemOverhead,
            SparkScopePropertyExecMem,
            SparkScopePropertyExecMemOverhead,
            SparkScopePropertyExecMemOverheadFactor
        )

        Then("executor memory overhead should be set to specified fraction of executor.memory")
        assert(memorySize.toMB == 1000)
    }
}
