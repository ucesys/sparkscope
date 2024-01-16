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
import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.data.DataTable
import com.ucesys.sparkscope.io.writer.S3FileWriter
import org.apache.commons.lang.SystemUtils
import org.mockito.MockitoSugar
import org.scalatest.{FunSuite, GivenWhenThen}
import org.mockito.ArgumentMatchers.any

import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit;

class S3CsvReporterSuite extends FunSuite with GivenWhenThen with MockitoSugar {

    val driverMetricsStr: String =
        """t,jvm.heap.max,jvm.heap.usage,jvm.heap.used,jvm.non-heap.used
          |1695358645,954728448,0.29708835490780305,283638704,56840944""".stripMargin
    val driverMetrics = DataTable.fromCsv("driver", driverMetricsStr, ",")

    val exec1MetricsStr: String =
        """t,jvm.heap.max,jvm.heap.usage,jvm.heap.used,jvm.non-heap.used,executor.cpuTime
          |1695358697,838860800,0.21571252822875978,180952784,51120384,29815418742""".stripMargin

    val exec1Metrics: DataTable = DataTable.fromCsv("1", exec1MetricsStr, ",")

    def createS3CsvReporter(directory: String, writerMock: S3FileWriter = mock[S3FileWriter]): S3CsvReporter = {
        new S3CsvReporter(
            directory,
            new MetricRegistry,
            null,
            ",",
            TimeUnit.SECONDS,
            TimeUnit.SECONDS,
            null,
            mock[MetricFilter],
            writerMock
        )(mock[SparkScopeLogger])
    }

    test("bucket, metricsDir extraction") {
        Given("s3 bucket url")
        val s3BucketUrl = "s3://my-bucket/metrics-dir"

        When("calling S3CsvReporter constructor")
        val s3CsvReporter = createS3CsvReporter(s3BucketUrl)

        Then("bucketName and metricsDir should be extracted")
        assert(s3CsvReporter.s3Location.bucketName.equals("my-bucket"))
        assert(s3CsvReporter.s3Location.path.equals("metrics-dir"))
    }

    test("nested metricsDir") {
        Given("s3 bucket url")
        val s3BucketUrl = "s3:///my-bucket/nested-path/to/metrics-dir"

        When("calling S3CsvReporter constructor")
        val s3CsvReporter = createS3CsvReporter(s3BucketUrl)

        Then("bucketName and metricsDir should be extracted")
        assert(s3CsvReporter.s3Location.bucketName.equals("my-bucket"))
        assert(s3CsvReporter.s3Location.path.equals("nested-path/to/metrics-dir"))
    }

    test("nested metricsDir, triple slash in s3:///") {
        Given("s3 bucket url")
        val s3BucketUrl = "s3:///my-bucket/nested-path/to/metrics-dir"
        And("region")

        When("calling S3CsvReporter constructor")
        val s3CsvReporter = createS3CsvReporter(s3BucketUrl)

        Then("bucketName and metricsDir should be extracted")
        assert(s3CsvReporter.s3Location.bucketName.equals("my-bucket"))
        assert(s3CsvReporter.s3Location.path.equals("nested-path/to/metrics-dir"))
    }

    test("S3CsvReporter IN_PROGRESS exists") {
        Given("S3 IN_PROGRESS file exists")
        val writerMock = mock[S3FileWriter]
        doReturn(true).when(writerMock).exists(any[String])

        And("s3 reporter")
        val s3BucketUrl = "s3://my-bucket/metrics-dir"
        val s3CsvReporter = createS3CsvReporter(s3BucketUrl, writerMock)

        if (SystemUtils.OS_NAME == "Linux") {
            When("s3CsvReporter.report")
            s3CsvReporter.report("app-123-456", "driver", driverMetrics, 123)

            Then("Filesystem.exists should be called")
            verify(writerMock, times(2)).exists("s3://my-bucket/metrics-dir/.tmp/app-123-456/IN_PROGRESS")

            And("And new IN_PROGRESS file should not be created")
            verify(writerMock, times(0)).write("s3://my-bucket/metrics-dir/.tmp/app-123-456/IN_PROGRESS", "")

            And("file with single row should be written")
            verify(writerMock, times(1)).write(
                "s3://my-bucket/metrics-dir/.tmp/app-123-456/driver/driver.123.csv",
                driverMetrics.toCsv(",")
            )
        }
    }

    test("S3CsvReporter IN_PROGRESS doesn't exist ") {
        Given("S3 IN_PROGRESS file doesn't exist")
        val writerMock = mock[S3FileWriter]
        doReturn(false).when(writerMock).exists(any[String])

        And("s3 reporter")
        val s3BucketUrl = "s3://my-bucket/metrics-dir"
        val s3CsvReporter = createS3CsvReporter(s3BucketUrl, writerMock)

        if (SystemUtils.OS_NAME == "Linux") {
            When("s3CsvReporter.report")
            s3CsvReporter.report("app-123-456", "1", exec1Metrics, 123)

            Then("Filesystem.exists should be called")
            verify(writerMock, times(2)).exists("s3://my-bucket/metrics-dir/.tmp/app-123-456/IN_PROGRESS")

            And("And new IN_PROGRESS file should be created")
            verify(writerMock, times(1)).write("s3://my-bucket/metrics-dir/.tmp/app-123-456/IN_PROGRESS", "")
        }
    }
}
