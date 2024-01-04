
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
import com.ucesys.sparkscope.io.writer.HadoopFileWriter
import org.apache.commons.lang.SystemUtils
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.{FunSuite, GivenWhenThen}

import java.io.IOException
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

class HadoopCsvReporterSuite extends FunSuite with MockitoSugar with GivenWhenThen {

    val driverMetricsStr: String =
        """t,jvm.heap.max,jvm.heap.usage,jvm.heap.used,jvm.non-heap.used
          |1695358645,954728448,0.29708835490780305,283638704,56840944""".stripMargin
    val driverMetrics = DataTable.fromCsv("driver", driverMetricsStr, ",")

    val exec1MetricsStr: String =
        """t,jvm.heap.max,jvm.heap.usage,jvm.heap.used,jvm.non-heap.used,executor.cpuTime
          |1695358697,838860800,0.21571252822875978,180952784,51120384,29815418742""".stripMargin

    val exec1Metrics: DataTable = DataTable.fromCsv("1", exec1MetricsStr, ",")

    def createHadoopCsvReporter(directory: String, writer: HadoopFileWriter = mock[HadoopFileWriter], appName: Option[String] = None)
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
            writer,
            appName
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
            hadoopCsvReporter.report("app-123-456", "driver", driverMetrics, 123)

            Then("No exception should be thrown")
            And("write or append should not be called")
            verify(writerFsClosed, times(1)).exists("hdfs:/tmp/path/app-123-456/driver.csv")
            verify(writerFsClosed, times(0)).write(any[String], any[String])
            verify(writerFsClosed, times(0)).append(any[String], any[String])
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
            hadoopCsvReporter.report("app-123-456", "driver", driverMetrics, 123)
            hadoopCsvReporter.report("app-123-456", "1", exec1Metrics, 123)

            Then("Filesystem.exists should be called")
            verify(writerMock, times(1)).exists("hdfs:/tmp/path/app-123-456/driver.csv")
            verify(writerMock, times(1)).exists("hdfs:/tmp/path/app-123-456/1.csv")

            And("app dir should be created")
            verify(writerMock, times(2)).makeDir("hdfs:/tmp/path/app-123-456")

            And("New metrics file should be created")
            verify(writerMock, times(1)).write("hdfs:/tmp/path/app-123-456/driver.csv", driverMetrics.header + "\n")
            verify(writerMock, times(1)).write("hdfs:/tmp/path/app-123-456/1.csv", exec1Metrics.header + "\n")

            And("Row should be appended to file")
            verify(writerMock, times(1)).append(
                "hdfs:/tmp/path/app-123-456/driver.csv",
                driverMetrics.toCsvNoHeader(",") + "\n"
            )
            verify(writerMock, times(1)).append(
                "hdfs:/tmp/path/app-123-456/1.csv",
                exec1Metrics.toCsvNoHeader(",") + "\n"
            )
        }
    }

    test("HadoopCsvReporter report metrics create appName set") {
        Given("hadoop fs is open and metrics file doesn't exists")
        val writerMock = mock[HadoopFileWriter]
        doReturn(false).when(writerMock).exists(any[String])

        And("hdfs reporter")
        val hdfsPath = "hdfs:/tmp/path"
        val hadoopCsvReporter = createHadoopCsvReporter(hdfsPath, writerMock, Some("my-app"))

        if (SystemUtils.OS_NAME == "Linux") {
            When("calling HadoopCsvReporter.report")
            hadoopCsvReporter.report("app-123-456", "driver", driverMetrics, 123)
            hadoopCsvReporter.report("app-123-456", "1", exec1Metrics, 123)

            Then("Filesystem.exists should be called")
            verify(writerMock, times(1)).exists("hdfs:/tmp/path/my-app/app-123-456/driver.csv")
            verify(writerMock, times(1)).exists("hdfs:/tmp/path/my-app/app-123-456/1.csv")

            And("app dir should be created")
            verify(writerMock, times(2)).makeDir("hdfs:/tmp/path/my-app/app-123-456")

            And("New metrics file should be created")
            verify(writerMock, times(1)).write("hdfs:/tmp/path/my-app/app-123-456/driver.csv", driverMetrics.header + "\n")
            verify(writerMock, times(1)).write("hdfs:/tmp/path/my-app/app-123-456/1.csv", exec1Metrics.header + "\n")

            And("Row should be appended to file")
            verify(writerMock, times(1)).append(
                "hdfs:/tmp/path/my-app/app-123-456/driver.csv",
                driverMetrics.toCsvNoHeader(",") + "\n"
            )
            verify(writerMock, times(1)).append(
                "hdfs:/tmp/path/my-app/app-123-456/1.csv",
                exec1Metrics.toCsvNoHeader(",") + "\n"
            )
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
            hadoopCsvReporter.report("app-123-456", "driver", driverMetrics, 123)
            hadoopCsvReporter.report("app-123-456", "1", exec1Metrics, 123)

            Then("Filesystem.exists should be called")
            verify(writerMock, times(1)).exists("hdfs:/tmp/path/app-123-456/driver.csv")
            verify(writerMock, times(1)).exists("hdfs:/tmp/path/app-123-456/1.csv")

            And("New file should not be created")
            verify(writerMock, times(0)).write(any[String], any[String])

            And("Row should be appended to file")
            verify(writerMock, times(1)).append(
                "hdfs:/tmp/path/app-123-456/driver.csv",
                driverMetrics.toCsvNoHeader(",") + "\n"
            )
            verify(writerMock, times(1)).append(
                "hdfs:/tmp/path/app-123-456/1.csv",
                exec1Metrics.toCsvNoHeader(",") + "\n"
            )
        }
    }
}
