
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
import org.scalatest.MustMatchers.{a, convertToAnyMustWrapper}
import org.scalatest.{FunSuite, GivenWhenThen}

import java.io.FileNotFoundException
import java.util.Properties

class PropertiesLoaderFactorySuite extends FunSuite with MockFactory with GivenWhenThen {

  val propertiesLoaderFactory = new PropertiesLoaderFactory()

  test("maprfs://path") {
    Given("maprfs path")
    val paths = Seq("maprfs:///dir", "maprfs:/path/to/file.ext")

    When("calling PropertiesLoaderFactory.getPropertiesLoader")
    Then("HadoopPropertiesLoader should be returned")
    paths.foreach {
      maprfsPath => {
        val propertiesLoader = propertiesLoaderFactory.getPropertiesLoader(maprfsPath)
        propertiesLoader mustBe a[HadoopPropertiesLoader]
      }
    }
  }

  test("hdfs:// path") {
    Given("hdfs path")
    val paths = Seq("hdfs:///dir", "hdfs:/path/to/file.ext")

    When("calling PropertiesLoaderFactory.getPropertiesLoader")
    Then("HadoopPropertiesLoader should be returned")
    paths.foreach {
      hdfsPath => {
        val propertiesLoader = propertiesLoaderFactory.getPropertiesLoader(hdfsPath)
        propertiesLoader mustBe a[HadoopPropertiesLoader]
      }
    }
  }

  test("file:// path") {
    Given("file:// path")
    val paths = Seq("file:///dir", "file:/path/to/file.ext")

    When("calling PropertiesLoaderFactory.getPropertiesLoader")
    Then("HadoopPropertiesLoader should be returned")
    paths.foreach {
      hdfsPath => {
        val propertiesLoader = propertiesLoaderFactory.getPropertiesLoader(hdfsPath)
        propertiesLoader mustBe a[HadoopPropertiesLoader]
      }
    }
  }

  test("other paths") {
    Given("non mapr/hdfs/file path")
    val paths = Seq("/absolute/path", "./relative/path", "/mapr/lookalike", "/hdfs/path", "/file/path")

    When("calling PropertiesLoaderFactory.getPropertiesLoader")
    Then("LocalPropertiesLoader should be returned")
    paths.foreach {
      hdfsPath => {
        val propertiesLoader = propertiesLoaderFactory.getPropertiesLoader(hdfsPath)
        propertiesLoader mustBe a[LocalPropertiesLoader]
      }
    }
  }
}
