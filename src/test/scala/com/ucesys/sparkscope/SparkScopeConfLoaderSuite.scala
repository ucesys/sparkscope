
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
import com.ucesys.sparkscope.io.{PropertiesLoader, PropertiesLoaderFactory}
import com.ucesys.sparkscope.common.SparkScopeLogger
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
        val sparkConfWithMetrics = new SparkConf()
          .set(SparkScopePropertyDriverMetricsDir, "/sparkscope/path/to/driver/metrics")
          .set(SparkScopePropertyExecutorMetricsDir, "/sparkscope/path/to/executor/metrics")
          .set("spark.metrics.conf", MetricsPropertiesPath)
          .set("spark.metrics.conf.*.sink.csv.directory", "/spark/metric/path/to/all/metrics")
          .set("spark.metrics.conf.driver.sink.csv.directory ", "/spark/metrics/path/to/driver/metrics")
          .set("spark.metrics.conf.executor.sink.csv.directory ", "/spark/metrics/path/to/executor/metrics")

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
    }

    test("extracting driver & executor metrics path from spark.metrics.conf.[driver|executor]") {
        Given("SparkConf")
        And("with spark.metrics.conf.driver.sink.csv.director set")
        And("with spark.metrics.conf.executor.sink.csv.director set")
        And("with spark.metrics.conf set")
        And("without spark.sparkscope.metrics.dir.driver set")
        And("without spark.sparkscope.metrics.dir.executor set")
        And("without spark.metrics.conf.*.sink.csv.director set")
        val sparkConfWithMetrics = new SparkConf()
          .set("spark.metrics.conf", MetricsPropertiesPath)
          .set("spark.metrics.conf.driver.sink.csv.directory", "/spark/metrics/path/to/driver/metrics")
          .set("spark.metrics.conf.executor.sink.csv.directory", "/spark/metrics/path/to/executor/metrics")

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
    }

    test("extracting driver & executor metrics path from spark.metrics.conf.*") {
        Given("SparkConf")
        And("with spark.metrics.conf.*.sink.csv.director set")
        And("with spark.metrics.conf set")
        And("without spark.sparkscope.metrics.dir.driver set")
        And("without spark.sparkscope.metrics.dir.executor set")
        And("without spark.metrics.conf.driver.sink.csv.director set")
        And("without spark.metrics.conf.executor.sink.csv.director set")
        val sparkConfWithMetrics = new SparkConf()
          .set("spark.metrics.conf", MetricsPropertiesPath)
          .set("spark.metrics.conf.*.sink.csv.directory", "/spark/metrics/path/to/all/metrics")

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
    }

    test("extract load html dir from SparkConf") {
        Given("Incorrectly configured metrics properties path")
        When("loading SparkScope config")
        val sparkScopeConfLoader = new SparkScopeConfLoader
        val sparkScopeConf = sparkScopeConfLoader.load(sparkConf, getPropertiesLoaderFactoryMock)

        Then("SparkScopeConf should contain executorMetricsDir")
        assert(sparkScopeConf.htmlReportPath == "/path/to/html/report")
    }
}
