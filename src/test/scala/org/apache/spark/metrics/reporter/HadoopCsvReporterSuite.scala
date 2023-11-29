
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
import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.io.file.HadoopFileWriter
import org.apache.commons.lang.SystemUtils
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.{FunSuite, GivenWhenThen}

import java.io.IOException
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

class HadoopCsvReporterSuite extends FunSuite with MockitoSugar with GivenWhenThen {

    def createHadoopCsvReporter(directory: String,
                                writer: HadoopFileWriter = mock[HadoopFileWriter])
                               (implicit logger: SparkScopeLogger = mock[SparkScopeLogger]): HadoopCsvReporter = {
        new HadoopCsvReporter(
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
            writer
        )
    }

    test("HadoopCsvReporter fs closed") {
        Given("hadoop fs is closed")
        val hdfsPath = "hdfs:/tmp/path"
        val writerFsClosed = mock[HadoopFileWriter]
        doThrow(new IOException("Filesystem closed")).when(writerFsClosed).exists(any[String])

        And("hdfs reporter")
        val hadoopCsvReporter = createHadoopCsvReporter(hdfsPath, writerFsClosed)

        if(SystemUtils.OS_NAME == "Linux") {
            When("calling HadoopCsvReporter.report")
            hadoopCsvReporter.report(123, "app-123-456.driver.jvm.heap.used", "value", "%s", "123")

            Then("No exception should be thrown")
            And("write or append should not be called")
            verify(writerFsClosed, times(1)).exists("hdfs:/tmp/path/app-123-456.driver.jvm.heap.used.csv")
            verify(writerFsClosed, times(0)).write(any[String], any[String])
            verify(writerFsClosed, times(0)).append(any[String], any[String])
        }
    }

    test("HadoopCsvReporter report metrics append") {
        Given("hadoop fs is open and metrics file already exists")
        val writerMock = mock[HadoopFileWriter]
        doReturn(true).when(writerMock).exists(any[String])

        And("hdfs reporter")
        val hdfsPath = "hdfs:/tmp/path"
        val hadoopCsvReporter = createHadoopCsvReporter(hdfsPath, writerMock)


        if (SystemUtils.OS_NAME == "Linux") {
            When("calling HadoopCsvReporter.report")
            hadoopCsvReporter.report(123, "app-123-456.driver.jvm.heap.used", "t,value", "%s", 1000)
            hadoopCsvReporter.report(123, "app-123-456.0.executor.cpuTime", "count", "%d", 999)

            Then("Filesystem.exists should be called")
            verify(writerMock, times(1)).exists("hdfs:/tmp/path/app-123-456.driver.jvm.heap.used.csv")
            verify(writerMock, times(1)).exists("hdfs:/tmp/path/app-123-456.0.executor.cpuTime.csv")

            And("New file should not be created")
            verify(writerMock, times(0)).write(any[String], any[String])

            And("Row should be appended to file")
            verify(writerMock, times(1)).append("hdfs:/tmp/path/app-123-456.driver.jvm.heap.used.csv", "123,1000\n")
            verify(writerMock, times(1)).append("hdfs:/tmp/path/app-123-456.0.executor.cpuTime.csv", "123,999\n")
        }
    }

    test("HadoopCsvReporter report metrics create") {
        Given("hadoop fs is open and metrics file doesn't exists")
        val writerMock = mock[HadoopFileWriter]
        doReturn(false).when(writerMock).exists(any[String])

        And("hdfs reporter")
        val hdfsPath = "hdfs:/tmp/path"
        val hadoopCsvReporter = createHadoopCsvReporter(hdfsPath, writerMock)

        if (SystemUtils.OS_NAME == "Linux") {
            When("calling HadoopCsvReporter.report")
            hadoopCsvReporter.report(123, "app-123-456.driver.jvm.heap.used", "value", "%s", 1000)
            hadoopCsvReporter.report(123, "app-123-456.0.executor.cpuTime", "count", "%d", 999)

            Then("Filesystem.exists should be called")
            verify(writerMock, times(1)).exists("hdfs:/tmp/path/app-123-456.driver.jvm.heap.used.csv")
            verify(writerMock, times(1)).exists("hdfs:/tmp/path/app-123-456.0.executor.cpuTime.csv")

            And("New metrics file should be created")
            verify(writerMock, times(1)).write("hdfs:/tmp/path/app-123-456.driver.jvm.heap.used.csv", "t,value\n")
            verify(writerMock, times(1)).write("hdfs:/tmp/path/app-123-456.0.executor.cpuTime.csv", "t,count\n")


            And("Row should be appended to file")
            verify(writerMock, times(1)).append("hdfs:/tmp/path/app-123-456.driver.jvm.heap.used.csv", "123,1000\n")
            verify(writerMock, times(1)).append("hdfs:/tmp/path/app-123-456.0.executor.cpuTime.csv", "123,999\n")
        }
    }
}
