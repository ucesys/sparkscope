
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
import com.ucesys.sparkscope.io.file.LocalFileWriter
import org.apache.commons.lang.SystemUtils
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.{FunSuite, GivenWhenThen}

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

class LocalCsvReporterSuite extends FunSuite with MockitoSugar with GivenWhenThen {

    def createLocalCsvReporter(directory: String, writer: LocalFileWriter = mock[LocalFileWriter]): LocalCsvReporter = {
        new LocalCsvReporter(
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

    test("LocalCsvReporter report metrics append") {
        Given("Local metrics file already exists")
        val writerMock = mock[LocalFileWriter]
        doReturn(true).when(writerMock).exists(any[String])

        And("local reporter")
        val localPath = "/tmp/path"
        val localCsvReporter = createLocalCsvReporter(localPath, writerMock)


        if (SystemUtils.OS_NAME == "Linux") {
            When("calling LocalCsvReporter.report")
            localCsvReporter.report(123, "app-123-456.driver.jvm.heap.used", "t,value", "%s", 1000)
            localCsvReporter.report(123, "app-123-456.0.executor.cpuTime", "count", "%d", 999)

            Then("Filesystem.exists should be called")
            verify(writerMock, times(1)).exists("/tmp/path/app-123-456.driver.jvm.heap.used.csv")
            verify(writerMock, times(1)).exists("/tmp/path/app-123-456.0.executor.cpuTime.csv")

            And("New file should not be created")
            verify(writerMock, times(0)).write(any[String], any[String])

            And("Row should be appended to file")
            verify(writerMock, times(1)).append("/tmp/path/app-123-456.driver.jvm.heap.used.csv", "123,1000")
            verify(writerMock, times(1)).append("/tmp/path/app-123-456.0.executor.cpuTime.csv", "123,999")
        }
    }

    test("LocalCsvReporter report metrics create") {
        Given("Local metrics file doesn't exists")
        val writerMock = mock[LocalFileWriter]
        doReturn(false).when(writerMock).exists(any[String])

        And("local reporter")
        val localPath = "/tmp/path"
        val localCsvReporter = createLocalCsvReporter(localPath, writerMock)

        if (SystemUtils.OS_NAME == "Linux") {
            When("calling LocalCsvReporter.report")
            localCsvReporter.report(123, "app-123-456.driver.jvm.heap.used", "value", "%s", 1000)
            localCsvReporter.report(123, "app-123-456.0.executor.cpuTime", "count", "%d", 999)

            Then("Filesystem.exists should be called")
            verify(writerMock, times(1)).exists("/tmp/path/app-123-456.driver.jvm.heap.used.csv")
            verify(writerMock, times(1)).exists("/tmp/path/app-123-456.0.executor.cpuTime.csv")

            And("New metrics file should be created")
            verify(writerMock, times(1)).write("/tmp/path/app-123-456.driver.jvm.heap.used.csv", "t,value\n")
            verify(writerMock, times(1)).write("/tmp/path/app-123-456.0.executor.cpuTime.csv", "t,count\n")


            And("Row should be appended to file")
            verify(writerMock, times(1)).append("/tmp/path/app-123-456.driver.jvm.heap.used.csv", "123,1000")
            verify(writerMock, times(1)).append("/tmp/path/app-123-456.0.executor.cpuTime.csv", "123,999")
        }
    }
}
