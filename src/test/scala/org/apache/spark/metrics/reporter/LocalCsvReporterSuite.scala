
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
import com.ucesys.sparkscope.data.DataTable
import com.ucesys.sparkscope.io.file.LocalFileWriter
import org.apache.commons.lang.SystemUtils
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.{FunSuite, GivenWhenThen}

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

class LocalCsvReporterSuite extends FunSuite with MockitoSugar with GivenWhenThen {

    val driverMetricsStr: String =
        """t,jvm.heap.max,jvm.heap.usage,jvm.heap.used,jvm.non-heap.used
          |1695358645,954728448,0.29708835490780305,283638704,56840944""".stripMargin
    val driverMetrics = DataTable.fromCsv("driver", driverMetricsStr, ",")

    val exec1MetricsStr: String =
        """t,jvm.heap.max,jvm.heap.usage,jvm.heap.used,jvm.non-heap.used,executor.cpuTime
          |1695358697,838860800,0.21571252822875978,180952784,51120384,29815418742""".stripMargin

    val exec1Metrics: DataTable = DataTable.fromCsv("1", exec1MetricsStr, ",")

    def createLocalCsvReporter(directory: String, writer: LocalFileWriter = mock[LocalFileWriter], appName: Option[String]=None): LocalCsvReporter = {
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
            writer,
            appName
        )(mock[SparkScopeLogger])
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
            localCsvReporter.report("app-123-456", "driver", driverMetrics,  123)
            localCsvReporter.report("app-123-456", "1", exec1Metrics,  123)

            Then("Filesystem.exists should be called")
            verify(writerMock, times(1)).exists("/tmp/path/app-123-456/driver.csv")
            verify(writerMock, times(1)).exists("/tmp/path/app-123-456/1.csv")

            And("New file should not be created")
            verify(writerMock, times(0)).write(any[String], any[String])

            And("Row should be appended to file")
            verify(writerMock, times(1)).append("/tmp/path/app-123-456/driver.csv", driverMetrics.toCsvNoHeader(","))
            verify(writerMock, times(1)).append("/tmp/path/app-123-456/1.csv", exec1Metrics.toCsvNoHeader(","))
        }
    }

    test("LocalCsvReporter report metrics create file") {
        Given("Local metrics file doesn't exists")
        val writerMock = mock[LocalFileWriter]
        doReturn(false).when(writerMock).exists(any[String])

        And("local reporter")
        val localPath = "/tmp/path"
        val localCsvReporter = createLocalCsvReporter(localPath, writerMock)

        if (SystemUtils.OS_NAME == "Linux") {
            When("calling LocalCsvReporter.report")
            localCsvReporter.report("app-123-456", "driver", driverMetrics, 123)
            localCsvReporter.report("app-123-456", "1", exec1Metrics, 123)

            Then("Filesystem.exists should be called")
            verify(writerMock, times(1)).exists("/tmp/path/app-123-456/driver.csv")
            verify(writerMock, times(1)).exists("/tmp/path/app-123-456/1.csv")

            And("makeDir should be called with appId dir")
            verify(writerMock, times(2)).makeDir("/tmp/path/app-123-456")

            And("New metrics file should be created")
            verify(writerMock, times(1)).write("/tmp/path/app-123-456/driver.csv", driverMetrics.header + "\n")
            verify(writerMock, times(1)).write("/tmp/path/app-123-456/1.csv", exec1Metrics.header + "\n")

            And("Row should be appended to file")
            verify(writerMock, times(1)).append("/tmp/path/app-123-456/driver.csv", driverMetrics.toCsvNoHeader(","))
            verify(writerMock, times(1)).append("/tmp/path/app-123-456/1.csv", exec1Metrics.toCsvNoHeader(","))
        }
    }

    test("LocalCsvReporter report metrics create file appName set") {
        Given("Local metrics file doesn't exists")
        val writerMock = mock[LocalFileWriter]
        doReturn(false).when(writerMock).exists(any[String])

        And("local reporter")
        And("appName is set")
        val localPath = "/tmp/path"
        val localCsvReporter = createLocalCsvReporter(localPath, writerMock, Some("my-app"))

        if (SystemUtils.OS_NAME == "Linux") {
            When("calling LocalCsvReporter.report")
            localCsvReporter.report("app-123-456", "driver", driverMetrics, 123)
            localCsvReporter.report("app-123-456", "1", exec1Metrics, 123)

            Then("Filesystem.exists should be called including appName dir")
            verify(writerMock, times(1)).exists("/tmp/path/my-app/app-123-456/driver.csv")
            verify(writerMock, times(1)).exists("/tmp/path/my-app/app-123-456/1.csv")

            And("appName dir should be created")
            verify(writerMock, times(2)).makeDir("/tmp/path/my-app/app-123-456")

            And("New metrics file should be created in appName dir")
            verify(writerMock, times(1)).write("/tmp/path/my-app/app-123-456/driver.csv", driverMetrics.header + "\n")
            verify(writerMock, times(1)).write("/tmp/path/my-app/app-123-456/1.csv", exec1Metrics.header + "\n")

            And("Row should be appended to file")
            verify(writerMock, times(1)).append("/tmp/path/my-app/app-123-456/driver.csv", driverMetrics.toCsvNoHeader(","))
            verify(writerMock, times(1)).append("/tmp/path/my-app/app-123-456/1.csv", exec1Metrics.toCsvNoHeader(","))
        }
    }
}
