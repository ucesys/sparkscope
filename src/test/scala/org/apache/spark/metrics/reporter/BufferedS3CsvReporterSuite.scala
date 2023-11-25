
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

import com.codahale.metrics.{MetricFilter, MetricRegistry}
import com.ucesys.sparkscope.io.file.S3FileWriter
import org.apache.commons.lang.SystemUtils
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.{FunSuite, GivenWhenThen}

import java.util.concurrent.{ScheduledExecutorService, TimeUnit};

class BufferedS3CsvReporterSuite extends FunSuite with GivenWhenThen with MockitoSugar {

    def createS3CsvReporter(directory: String, writerMock: S3FileWriter = mock[S3FileWriter]): BufferedS3CsvReporter = {
        new BufferedS3CsvReporter(
            directory,
            new MetricRegistry,
            null,
            ",",
            TimeUnit.SECONDS,
            TimeUnit.SECONDS,
            null,
            mock[MetricFilter],
            mock[ScheduledExecutorService],
            false,
            writerMock
        )
    }

    test("Buffered S3CsvReporter") {
        Given("s3 reporter")
        val writerMock = mock[S3FileWriter]
        val s3BucketUrl = "s3://my-bucket/metrics-dir"
        val s3CsvReporter = createS3CsvReporter(s3BucketUrl, writerMock)

        if (SystemUtils.OS_NAME == "Linux") {
            When("s3CsvReporter.report")
            s3CsvReporter.report(123, "app-123-456.driver.jvm.heap.used", "value", "%s", 1000)
            s3CsvReporter.report(124, "app-123-456.driver.jvm.heap.used", "value", "%s", 1500)
            s3CsvReporter.report(111, "app-123-456.0.executor.cpuTime", "count", "%d", 999)
            s3CsvReporter.report(112, "app-123-456.0.executor.cpuTime", "count", "%d", 500)

            Then("rows should not be written")
            verify(writerMock, times(0)).write(any[String], any[String])

            When("s3CsvReporter.close")
            s3CsvReporter.close()

            Then("metrics should be saved")
            verify(writerMock, times(1)).write(
                "s3://my-bucket/metrics-dir/app-123-456/driver/jvm.heap.used.csv",
                "t,value\n123,1000\n124,1500"
            )
            verify(writerMock, times(1)).write(
                "s3://my-bucket/metrics-dir/app-123-456/0/executor.cpuTime.csv",
                "t,count\n111,999\n112,500"
            )
        }
    }
}
