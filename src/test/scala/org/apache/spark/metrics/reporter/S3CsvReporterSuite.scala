
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

package org.apache.spark.metrics.reporter

import com.codahale.metrics.MetricFilter
import com.codahale.metrics.MetricRegistry
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSuite, GivenWhenThen}

import java.util.Optional
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit;

class S3CsvReporterSuite extends FunSuite with MockFactory with GivenWhenThen {

    def createS3CsvReporter(directory: String, region: String): S3CsvReporter = {
        new S3BufferedCsvReporter(
            directory,
            Option(region),
            new MetricRegistry,
            null,
            ",",
            TimeUnit.SECONDS,
            TimeUnit.SECONDS,
            null,
            mock[MetricFilter],
            mock[ScheduledExecutorService],
            false
        )
    }

    test("bucket, metricsDir, appName extraction") {
        Given("s3 bucket url")
        val s3BucketUrl = "s3://my-bucket/metrics-dir"
        And("region")
        val region = "us-east1"

        When("calling S3CsvReporter constructor")
        val s3CsvReporter = createS3CsvReporter(s3BucketUrl, region)

        Then("bucketName and metricsDir should be extracted")
        assert(s3CsvReporter.s3Location.bucketName.equals("my-bucket"))
        assert(s3CsvReporter.s3Location.path.equals("metrics-dir"))
        assert(s3CsvReporter.s3 != null)
    }

    test("appName empty") {
        Given("s3 bucket url")
        val s3BucketUrl = "s3:///my-bucket/metrics-dir/"
        And("region")
        val region = "us-east1"

        When("calling S3CsvReporter constructor")
        val s3CsvReporter = createS3CsvReporter(s3BucketUrl, region)

        Then("bucketName and metricsDir should be extracted")
        assert(s3CsvReporter.s3Location.bucketName.equals("my-bucket"))
        assert(s3CsvReporter.s3Location.path.equals("metrics-dir"))
        assert(s3CsvReporter.s3 != null)
    }

    test("nested metricsDir") {
        Given("s3 bucket url")
        val s3BucketUrl = "s3:///my-bucket/nested-path/to/metrics-dir"
        And("region")
        val region = "us-east1"

        When("calling S3CsvReporter constructor")
        val s3CsvReporter = createS3CsvReporter(s3BucketUrl, region)

        Then("bucketName and metricsDir should be extracted")
        assert(s3CsvReporter.s3Location.bucketName.equals("my-bucket"))
        assert(s3CsvReporter.s3Location.path.equals("nested-path/to/metrics-dir"))
        assert(s3CsvReporter.s3 != null)
    }

    test("nested metricsDir, triple slash in s3:///") {
        Given("s3 bucket url")
        val s3BucketUrl = "s3:///my-bucket/nested-path/to/metrics-dir"
        And("region")
        val region = "us-east1"

        When("calling S3CsvReporter constructor")
        val s3CsvReporter = createS3CsvReporter(s3BucketUrl, region)

        Then("bucketName and metricsDir should be extracted")
        assert(s3CsvReporter.s3Location.bucketName.equals("my-bucket"))
        assert(s3CsvReporter.s3Location.path.equals("nested-path/to/metrics-dir"))
        assert(s3CsvReporter.s3 != null)
    }

    test("region unset") {
        Given("s3 bucket url")
        val s3BucketUrl = "s3:///my-bucket/nested-path/to/metrics-dir"
        And("region")
        val region = "us-east1"

        When("calling S3CsvReporter constructor")
        Then("IllegalArgumentException should be thrown")
        assertThrows[IllegalArgumentException] {
            val s3CsvReporter = createS3CsvReporter(s3BucketUrl, null)
        }
    }
}
