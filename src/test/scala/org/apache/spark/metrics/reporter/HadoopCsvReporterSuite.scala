
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
import org.apache.commons.lang.SystemUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.Logger
import org.apache.spark.deploy.SparkHadoopUtil
import org.mockito.ArgumentMatchers.any
import org.mockito.MockitoSugar
import org.scalatest.{FunSuite, GivenWhenThen}

import java.io.IOException
import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

class HadoopCsvReporterSuite extends FunSuite with MockitoSugar with GivenWhenThen {

    def createHadoopCsvReporter(directory: String): HadoopCsvReporter = {
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
            false
        )
    }

    test("HadoopCsvReporter fs closed") {
            Given("hdfs reporter")
        val hdfsPath = "hdfs:/tmp/path"
        val hadoopCsvReporter = createHadoopCsvReporter(hdfsPath)

        And("hadoop fs is closed")
        val loggerMock = mock[Logger]
        val fsSpy = MockitoSugar.spy(FileSystem.get(SparkHadoopUtil.get.newConfiguration(null)))
        doThrow(new IOException("Filesystem closed")).when(fsSpy).exists(any[Path])

        val fsField = hadoopCsvReporter.getClass.getDeclaredField("fs");
        fsField.setAccessible(true);
        fsField.set(hadoopCsvReporter, fsSpy);

        val loggerField = hadoopCsvReporter.getClass.getDeclaredField("LOGGER");
        loggerField.setAccessible(true);
        loggerField.set(hadoopCsvReporter, loggerMock);

        if(SystemUtils.OS_NAME == "Linux") {
            When("calling HadoopCsvReporter.report")
            hadoopCsvReporter.report(123, "app-123-456.driver.jvm.heap.used", "t,value", "%d", "123,1000")

            Then("Filesystem closed warning should be logged")
            val fsClosedWarning = "IOException while writing app-123-456.driver.jvm.heap.used to hdfs:/tmp/path. java.io.IOException: Filesystem closed"
            verify(loggerMock, times(1)).warn(fsClosedWarning)
        }
    }
}
