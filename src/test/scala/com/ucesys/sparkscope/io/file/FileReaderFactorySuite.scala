
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

package com.ucesys.sparkscope.io.file

import com.ucesys.sparkscope.common.SparkScopeLogger
import org.scalamock.scalatest.MockFactory
import org.scalatest.MustMatchers.{a, convertToAnyMustWrapper}
import org.scalatest.{FunSuite, GivenWhenThen}

class FileReaderFactorySuite extends FunSuite with MockFactory with GivenWhenThen {

    val fileReaderFactory = new FileReaderFactory
    implicit val logger: SparkScopeLogger = new SparkScopeLogger

    test("maprfs://path") {
        Given("maprfs path")
        val paths = Seq("maprfs:///dir", "maprfs:/path/to/file.ext")

        When("calling FileReaderFactory.getMetricReader")
        Then("HadoopFileReader should be returned")
        paths.foreach {
            path => {
                val fileReader = fileReaderFactory.getFileReader(path)
                fileReader mustBe a[HadoopFileReader]
            }
        }
    }

    test("hdfs:// path") {
        Given("hdfs path")
        val paths = Seq("hdfs:///dir", "hdfs:/path/to/file.ext")

        When("calling FileReaderFactory.getMetricReader")
        Then("HadoopFileReader should be returned")
        paths.foreach {
            path => {
                val fileReader = fileReaderFactory.getFileReader(path)
                fileReader mustBe a[HadoopFileReader]
            }
        }
    }

    test("file:// path") {
        Given("file:// path")
        val paths = Seq("file:///dir", "file:/path/to/file.ext")

        When("calling FileReaderFactory.getMetricReader")
        Then("HadoopFileReader should be returned")
        paths.foreach {
            path => {
                val fileReader = fileReaderFactory.getFileReader(path)
                fileReader mustBe a[HadoopFileReader]
            }
        }
    }

    test("other paths") {
        Given("non mapr/hdfs/file path")
        val paths = Seq("/absolute/path", "./relative/path", "/mapr/lookalike", "/hdfs/path", "/file/path")

        When("calling FileReaderFactory.getMetricReader")
        Then("LocalFileReader should be returned")
        paths.foreach {
            path => {
                val fileReader = fileReaderFactory.getFileReader(path)
                fileReader mustBe a[LocalFileReader]
            }
        }
    }
}
