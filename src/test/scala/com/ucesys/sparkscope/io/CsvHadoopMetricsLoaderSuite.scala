
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

import com.ucesys.sparkscope.SparkScopeAnalyzer.{DriverCsvMetrics, ExecutorCsvMetrics}
import com.ucesys.sparkscope.TestHelpers._
import com.ucesys.sparkscope.io.{CsvHadoopMetricsLoader, HadoopFileReader, HadoopPropertiesLoader, PropertiesLoader, PropertiesLoaderFactory}
import org.apache.spark.SparkConf
import org.scalamock.scalatest.MockFactory
import org.scalatest.GivenWhenThen

import java.io.FileNotFoundException
import java.util.Properties
import org.scalatest.FunSuite

class CsvHadoopMetricsLoaderSuite extends FunSuite with MockFactory with GivenWhenThen {
    test("getMetricsPropertiesPath, spark.metrics.conf is unset, spark.home is set ") {
    Given("spark.metrics.conf unset in sparkConf")
    And("spark.home set in sparkConf")
    val sparkHome = "/opt/spark"
    val sparkConf = new SparkConf().set("spark.home", sparkHome)
    val csvReaderMock = stub[HadoopFileReader]
    val metricsLoader = new CsvHadoopMetricsLoader(getFileReaderFactoryMock(csvReaderMock), mockAppContext(), sparkConf, getPropertiesLoaderFactoryMock)

    When("calling getMetricsPropertiesPath")
    val metricsPropertiesPath = metricsLoader.getMetricsPropertiesPath()

    Then("$SPARK_HOME/conf/metrics.properties should be returned")
    assert(metricsPropertiesPath == sparkHome + "/conf/metrics.properties")
  }

  test("getMetricsPropertiesPath, spark.metrics.conf is set ") {
    Given("spark.metrics.conf set in sparkConf")
    val sparkConf = new SparkConf().set("spark.metrics.conf", MetricsPropertiesPath)
    val csvReaderMock = stub[HadoopFileReader]
    val metricsLoader = new CsvHadoopMetricsLoader(getFileReaderFactoryMock(csvReaderMock), mockAppContext(), sparkConf, getPropertiesLoaderFactoryMock)

    When("calling getMetricsPropertiesPath")
    val metricsPropertiesPath = metricsLoader.getMetricsPropertiesPath()

    Then("spark.metrics.conf should be taken from sparkConf")
    assert(metricsPropertiesPath == MetricsPropertiesPath)
  }

  test("metrics.properties file doesn't exist") {
    Given("Incorrectly configured metrics properties path")
    val sparkConf = new SparkConf().set("spark.metrics.conf", "/bad/path/to/metrics.properties")
    val csvReaderMock = stub[HadoopFileReader]
    val metricsLoader = new CsvHadoopMetricsLoader(getFileReaderFactoryMock(csvReaderMock), mockAppContext(), sparkConf, new PropertiesLoaderFactory)

    When("loading metrics")
    Then("CsvHadoopMetricsLoader should throw java.io.FileNotFoundException")
    assertThrows[FileNotFoundException] { // Result type: Assertion
      val driverExecutorMetrics = metricsLoader.load()
    }
  }

  test("Executor sink not configured") {
    Given("Correctly configured metrics properties path")
    val sparkConf = new SparkConf().set("spark.metrics.conf", MetricsPropertiesPath)
    val csvReaderMock = stub[HadoopFileReader]

    And("Executor sink is not configured")
    val propertiesLoaderMock = stub[PropertiesLoader]
    val properties = new Properties()
    properties.setProperty("driver.sink.csv.directory", csvMetricsPath)
    (propertiesLoaderMock.load _).when().returns(properties)

    val metricsLoader = new CsvHadoopMetricsLoader(getFileReaderFactoryMock(csvReaderMock), mockAppContext(), sparkConf, getPropertiesLoaderFactoryMock(propertiesLoaderMock))

    When("loading metrics")
    Then("CsvHadoopMetricsLoader should throw NoSuchFieldException")
    assertThrows[NoSuchFieldException] { // Result type: Assertion
      val driverExecutorMetrics = metricsLoader.load()
    }
  }

  test("Driver sink not configured") {
    Given("Correctly configured metrics properties path")
    val sparkConf = new SparkConf().set("spark.metrics.conf", MetricsPropertiesPath)
    val csvReaderMock = stub[HadoopFileReader]

    And("Driver sink is not configured")
    val propertiesLoaderMock = stub[PropertiesLoader]
    val properties = new Properties()
    properties.setProperty("executor.sink.csv.directory", csvMetricsPath)
    (propertiesLoaderMock.load _).when().returns(properties)

    val metricsLoader = new CsvHadoopMetricsLoader(getFileReaderFactoryMock(csvReaderMock), mockAppContext(), sparkConf, getPropertiesLoaderFactoryMock(propertiesLoaderMock))

    When("loading metrics")
    Then("CsvHadoopMetricsLoader should throw NoSuchFieldException")
    assertThrows[NoSuchFieldException] { // Result type: Assertion
      val driverExecutorMetrics = metricsLoader.load()
    }
  }

  test("Incorrect csv files test") {
    Given("Correctly configured metrics properties path")
    val sparkConf = new SparkConf().set("spark.metrics.conf", MetricsPropertiesPath)
    And("Incorrect csv files")
    val csvReaderMock = stub[HadoopFileReader]
    mockIncorrectDriverMetrics(csvReaderMock)
    val metricsLoader = new CsvHadoopMetricsLoader(getFileReaderFactoryMock(csvReaderMock), mockAppContext(), sparkConf, getPropertiesLoaderFactoryMock)

    When("loading metrics")
    Then("CsvHadoopMetricsLoader should throw IllegalArgumentException")
    assertThrows[IllegalArgumentException] { // Result type: Assertion
      val driverExecutorMetrics = metricsLoader.load()
    }
  }

  test("Successful metrics load test") {
    Given("Correctly configured metrics properties path")
    val sparkConf = new SparkConf().set("spark.metrics.conf", MetricsPropertiesPath)
    And("Correct csv files")
    val csvReaderMock = stub[HadoopFileReader]
    mockcorrectMetrics(csvReaderMock)
    val metricsLoader = new CsvHadoopMetricsLoader(getFileReaderFactoryMock(csvReaderMock), mockAppContext(), sparkConf, getPropertiesLoaderFactoryMock)

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
    val metricsLoader = new CsvHadoopMetricsLoader(getFileReaderFactoryMock(csvReaderMock), mockAppContextMissingExecutorMetrics, sparkConf, getPropertiesLoaderFactoryMock)

    When("loading metrics")
    val driverExecutorMetrics = metricsLoader.load()

    Then("Driver and Executor Metrics should be loaded")
    assert(driverExecutorMetrics.driverMetrics.length == 4)
    assert(driverExecutorMetrics.driverMetrics.length == DriverCsvMetrics.length)
    assert(driverExecutorMetrics.executorMetricsMap.size == 4)
    assert(driverExecutorMetrics.executorMetricsMap.head._2.length == ExecutorCsvMetrics.length)

    And("Missing Executor Metrics should be skipped")
    assert(driverExecutorMetrics.executorMetricsMap.get(6).isEmpty)
  }
}
