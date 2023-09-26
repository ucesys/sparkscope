
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

import com.ucesys.sparkscope.TestHelpers.{MetricsPropertiesPath, csvMetricsPath, getPropertiesLoaderFactoryMock, getPropertiesLoaderMock, sparkConf}
import com.ucesys.sparkscope.io.{PropertiesLoader, PropertiesLoaderFactory}
import org.apache.spark.SparkConf
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSuite, GivenWhenThen}

import java.io.FileNotFoundException
import java.util.Properties

class SparkScopeConfigSuite extends FunSuite with MockFactory with GivenWhenThen {

  test("getMetricsPropertiesPath, spark.metrics.conf is unset, spark.home is set ") {
    Given("spark.metrics.conf unset in sparkConf")
    And("spark.home set in sparkConf")
    val sparkHome = "/opt/spark"
    val sparkConf = new SparkConf().set("spark.home", sparkHome)

    When("calling getMetricsPropertiesPath")
    val metricsPropertiesPath = SparkScopeConfig.getMetricsPropertiesPath(sparkConf)

    Then("$SPARK_HOME/conf/metrics.properties should be returned")
    assert(metricsPropertiesPath == sparkHome + "/conf/metrics.properties")
  }

  test("getMetricsPropertiesPath, spark.metrics.conf is set ") {
    Given("spark.metrics.conf set in sparkConf")
    val sparkConf = new SparkConf().set("spark.metrics.conf", MetricsPropertiesPath)

    When("calling getMetricsPropertiesPath")
    val metricsPropertiesPath = SparkScopeConfig.getMetricsPropertiesPath(sparkConf)

    Then("spark.metrics.conf should be taken from sparkConf")
    assert(metricsPropertiesPath == MetricsPropertiesPath)
  }

  test("metrics.properties file doesn't exist") {
    Given("Incorrectly configured metrics properties path")
    val sparkConf = new SparkConf().set("spark.metrics.conf", "/bad/path/to/metrics.properties")

    When("loading metrics")
    Then("SparkScopeConfig should throw java.io.FileNotFoundException")
    assertThrows[FileNotFoundException] { // Result type: Assertion
      val driverExecutorMetrics = SparkScopeConfig.load(sparkConf, new PropertiesLoaderFactory)
    }
  }

  test("Executor sink not configured") {
    Given("Correctly configured metrics properties path")
    val sparkConf = new SparkConf().set("spark.metrics.conf", MetricsPropertiesPath)

    And("Executor sink is not configured")
    val propertiesLoaderMock = stub[PropertiesLoader]
    val properties = new Properties()
    properties.setProperty("driver.sink.csv.directory", csvMetricsPath)
    (propertiesLoaderMock.load _).when().returns(properties)
    val propertiesLoaderFactoryMock = stub[PropertiesLoaderFactory]
    (propertiesLoaderFactoryMock.getPropertiesLoader _).when(*).returns(propertiesLoaderMock)

    When("loading metrics")
    Then("SparkScopeConfig should throw NoSuchFieldException")
    assertThrows[NoSuchFieldException] { // Result type: Assertion
      val driverExecutorMetrics = SparkScopeConfig.load(sparkConf, propertiesLoaderFactoryMock)
    }
  }

  test("Driver sink not configured") {
    Given("Correctly configured metrics properties path")
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
    assertThrows[NoSuchFieldException] { // Result type: Assertion
      val driverExecutorMetrics = SparkScopeConfig.load(sparkConf, propertiesLoaderFactoryMock)
    }
  }

  test("load driver csv metrics dir from properties") {
    Given("Incorrectly configured metrics properties path")
    val sparkConfWithMetrics = sparkConf.set("spark.sparkscope.metrics.dir.driver ", "/path/to/driver/metrics")
    When("loading SparkScope config")
    val driverExecutorMetrics = SparkScopeConfig.load(sparkConfWithMetrics, getPropertiesLoaderFactoryMock)

    Then("SparkScopeConfig should contain driverMetricsDir")
    assert(driverExecutorMetrics.driverMetricsDir == "/path/to/driver/metrics")
  }

  test("load executor csv metrics dir from SparkConf") {
    Given("Incorrectly configured metrics properties path")
    val sparkConfWithMetrics = sparkConf.set("spark.sparkscope.metrics.dir.executor", "/path/to/executor/metrics")

    When("loading SparkScope config")
    val driverExecutorMetrics = SparkScopeConfig.load(sparkConfWithMetrics, getPropertiesLoaderFactoryMock)

    Then("SparkScopeConfig should contain executorMetricsDir")
    assert(driverExecutorMetrics.executorMetricsDir == "/path/to/executor/metrics")
  }

  test("load html dir from SparkConf") {
    Given("Incorrectly configured metrics properties path")
    When("loading SparkScope config")
    val driverExecutorMetrics = SparkScopeConfig.load(sparkConf, getPropertiesLoaderFactoryMock)

    Then("SparkScopeConfig should contain executorMetricsDir")
    assert(driverExecutorMetrics.htmlReportPath == "/path/to/html/report")
  }
}
