
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
import com.ucesys.sparkscope.io.file.LocalFileReader
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSuite, GivenWhenThen}

class S3LocationSuite extends FunSuite with MockFactory with GivenWhenThen {
    implicit val logger: SparkScopeLogger = stub[SparkScopeLogger]

    val sparkScopeConfHdfs = sparkScopeConf.copy(
        driverMetricsDir = s"/tmp/csv-metrics",
        executorMetricsDir = s"/tmp/csv-metrics"
    )

    test("Basic s3 url parse test") {
        Given("Basic s3 url")
        val s3Path = "s3://my-bucket/path"

        When("calling S3Location.apply")
        val s3Location = S3Location(s3Path)

        Then("bucket name and path should be parsed")
        assert(s3Location.bucketName == "my-bucket")
        assert(s3Location.path == "path")
    }

    test("Nested s3 url parse test") {
        Given("Nested s3 url")
        val s3Path = "s3://my-bucket/path/to/nested/dir"

        When("calling S3Location.apply")
        val s3Location = S3Location(s3Path)

        Then("bucket name and path should be parsed")
        assert(s3Location.bucketName == "my-bucket")
        assert(s3Location.path == "path/to/nested/dir")
    }

    test("Bad s3 url test") {
        Given("Bad s3 url directly to bucket")
        val s3Path = "s3://my-bucket/"

        When("calling S3Location.apply")
        Then("IllegalArgumentException should be thrown")
        assertThrows[IllegalArgumentException] {
            val s3Location = S3Location(s3Path)
        }
    }

    test("Bad s3 url test 2") {
        Given("Bad s3 url directly to bucket")
        val s3Path = "s3://my-bucket//"

        When("calling S3Location.apply")
        Then("IllegalArgumentException should be thrown")
        assertThrows[IllegalArgumentException] {
            val s3Location = S3Location(s3Path)
        }
    }

    test("Bad s3 url test 3") {
        Given("Bad s3 url without s3 prefix")
        val s3Path = "my-bucket/"

        When("calling S3Location.apply")
        Then("IllegalArgumentException should be thrown")
        assertThrows[IllegalArgumentException] {
            val s3Location = S3Location(s3Path)
        }
    }

    test("Bad s3 url test 4") {
        Given("Bad s3 url without s3 prefix")
        val s3Path = "my-bucket/my-dir"

        When("calling S3Location.apply")
        Then("IllegalArgumentException should be thrown")
        assertThrows[IllegalArgumentException] {
            val s3Location = S3Location(s3Path)
        }
    }

    test("S3Location toString") {
        Given("S3Location with bucket name and directory")
        val s3Location = S3Location("my-bucket", "my-directory")

        When("calling S3Location.toString")
        val s3Url = s3Location.getUrl

        Then("correct s3 url should be returned")
        assert(s3Url == "s3://my-bucket/my-directory")
    }

    test("S3Location toString 2") {
        Given("S3Location with bucket name and directory")
        val s3Location = S3Location("my-bucket", "path/to/nested/directory/")

        When("calling S3Location.toString")
        val s3Url = s3Location.getUrl

        Then("correct s3 url should be returned")
        assert(s3Url == "s3://my-bucket/path/to/nested/directory/")
    }
}
