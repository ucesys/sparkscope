
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

class FileWriterFactorySuite extends FunSuite with MockFactory with GivenWhenThen {

    val fileWriterFactory = new FileWriterFactory
    val fileWriterFactoryWithRegion = new FileWriterFactory(Some("us-east-1"))
    implicit val logger: SparkScopeLogger = new SparkScopeLogger

    test("maprfs://path") {
        Given("maprfs path")
        val paths = Seq("maprfs:///dir", "maprfs:/path/to/file.ext")

        When("calling fileWriterFactory.getMetricReader")
        Then("HadoopFileReader should be returned")
        paths.foreach {
            path => {
                val fileReader = fileWriterFactory.get(path)
                fileReader mustBe a[HadoopFileWriter]
            }
        }
    }

    test("hdfs:// path") {
        Given("hdfs path")
        val paths = Seq("hdfs:///dir", "hdfs:/path/to/file.ext")

        When("calling fileWriterFactory.getMetricReader")
        Then("HadoopFileReader should be returned")
        paths.foreach {
            path => {
                val fileReader = fileWriterFactory.get(path)
                fileReader mustBe a[HadoopFileWriter]
            }
        }
    }

    test("file:// path") {
        Given("file:// path")
        val paths = Seq("file:///dir", "file:/path/to/file.ext")

        When("calling fileWriterFactory.getMetricReader")
        Then("HadoopFileReader should be returned")
        paths.foreach {
            path => {
                val fileReader = fileWriterFactory.get(path)
                fileReader mustBe a[HadoopFileWriter]
            }
        }
    }

    test("s3 path") {
        Given("s3:// path")
        val paths = Seq("s3://bucket-name/path", "s3://bucket-name/to/file.ext")

        When("calling fileWriterFactory.getMetricReader")
        Then("HadoopFileReader should be returned")
        paths.foreach {
            path => {
                val fileReader = fileWriterFactoryWithRegion.get(path)
                fileReader mustBe a[S3FileWriter]
            }
        }
    }

    test("s3a path") {
        Given("s3a:// path")
        val paths = Seq("s3a://bucket-name/path", "s3a://bucket-name/to/file.ext")

        When("calling fileWriterFactory.getMetricReader")
        Then("HadoopFileReader should be returned")
        paths.foreach {
            path => {
                val fileReader = fileWriterFactoryWithRegion.get(path)
                fileReader mustBe a[S3FileWriter]
            }
        }
    }

    test("s3n path") {
        Given("s3n:// path")
        val paths = Seq("s3n://bucket-name/path", "s3n://bucket-name/to/file.ext")

        When("calling fileWriterFactory.getMetricReader")
        Then("HadoopFileReader should be returned")
        paths.foreach {
            path => {
                val fileReader = fileWriterFactoryWithRegion.get(path)
                fileReader mustBe a[S3FileWriter]
            }
        }
    }

    test("local paths") {
        Given("non mapr/hdfs/file path")
        val paths = Seq("/absolute/path", "./relative/path", "/mapr/lookalike", "/hdfs/path", "/file/path")

        When("calling fileWriterFactory.getMetricReader")
        Then("LocalFileReader should be returned")
        paths.foreach {
            path => {
                val fileReader = fileWriterFactory.get(path)
                fileReader mustBe a[LocalFileWriter]
            }
        }
    }
}
