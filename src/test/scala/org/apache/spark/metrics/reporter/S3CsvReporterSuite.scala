
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

    def mockS3CsvReporter(directory: String, appName: String, region: String): S3CsvReporter = {
        new S3CsvReporter(
            directory,
            Optional.ofNullable(appName),
            Optional.ofNullable(region),
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
        And("app name")
        val appName = "myApp"
        And("region")
        val region = "us-east1"

        When("calling S3CsvReporter constructor")
        val s3CsvReporter = mockS3CsvReporter(s3BucketUrl, appName, region)

        Then("bucketName and metricsDir should be extracted")
        assert(s3CsvReporter.bucketName.equals("my-bucket"))
        assert(s3CsvReporter.metricsDir.equals("metrics-dir"))
        assert(s3CsvReporter.appName.equals("myApp"))
        assert(s3CsvReporter.appDir.equals("metrics-dir/myApp"))
        assert(s3CsvReporter.s3 != null)
    }

    test("appName empty") {
        Given("s3 bucket url")
        val s3BucketUrl = "s3:///my-bucket/metrics-dir/"
        And("app empty name")
        val appName: String = null
        And("region")
        val region = "us-east1"

        When("calling S3CsvReporter constructor")
        val s3CsvReporter = mockS3CsvReporter(s3BucketUrl, appName, region)

        Then("bucketName and metricsDir should be extracted")
        assert(s3CsvReporter.bucketName.equals("my-bucket"))
        assert(s3CsvReporter.metricsDir.equals("metrics-dir"))
        assert(s3CsvReporter.appName.equals(""))
        assert(s3CsvReporter.appDir.equals("metrics-dir"))
        assert(s3CsvReporter.s3 != null)
    }

    test("nested metricsDir") {
        Given("s3 bucket url")
        val s3BucketUrl = "s3:///my-bucket/nested-path/to/metrics-dir"
        And("app name")
        val appName = "myApp"
        And("region")
        val region = "us-east1"

        When("calling S3CsvReporter constructor")
        val s3CsvReporter = mockS3CsvReporter(s3BucketUrl, appName, region)

        Then("bucketName and metricsDir should be extracted")
        assert(s3CsvReporter.bucketName.equals("my-bucket"))
        assert(s3CsvReporter.metricsDir.equals("nested-path/to/metrics-dir"))
        assert(s3CsvReporter.appName.equals("myApp"))
        assert(s3CsvReporter.appDir.equals("nested-path/to/metrics-dir/myApp"))
        assert(s3CsvReporter.s3 != null)
    }

    test("nested metricsDir, triple slash in s3:///") {
        Given("s3 bucket url")
        val s3BucketUrl = "s3:///my-bucket/nested-path/to/metrics-dir"
        And("app name")
        val appName = "myApp"
        And("region")
        val region = "us-east1"

        When("calling S3CsvReporter constructor")
        val s3CsvReporter = mockS3CsvReporter(s3BucketUrl, appName, region)

        Then("bucketName and metricsDir should be extracted")
        assert(s3CsvReporter.bucketName.equals("my-bucket"))
        assert(s3CsvReporter.metricsDir.equals("nested-path/to/metrics-dir"))
        assert(s3CsvReporter.appName.equals("myApp"))
        assert(s3CsvReporter.appDir.equals("nested-path/to/metrics-dir/myApp"))
        assert(s3CsvReporter.s3 != null)
    }

    test("region unset") {
        Given("s3 bucket url")
        val s3BucketUrl = "s3:///my-bucket/nested-path/to/metrics-dir"
        And("app name")
        val appName = "myApp"
        And("region")
        val region = "us-east1"

        When("calling S3CsvReporter constructor")
        Then("IllegalArgumentException should be thrown")
        assertThrows[IllegalArgumentException] {
            val s3CsvReporter = mockS3CsvReporter(s3BucketUrl, appName, null)
        }
    }
}
