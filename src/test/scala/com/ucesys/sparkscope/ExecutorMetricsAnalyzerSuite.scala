
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

package com.ucesys.sparkscope

import com.qubole.sparklens.common.{AggregateMetrics, AppContext, ApplicationInfo}
import com.qubole.sparklens.helper.JobOverlapHelper
import com.qubole.sparklens.timespan.{ExecutorTimeSpan, HostTimeSpan, JobTimeSpan, StageTimeSpan}
import com.ucesys.sparkscope.ExecutorMetricsAnalyzerSuite._
import com.ucesys.sparkscope.io.{CsvReader, HtmlReportGenerator, PropertiesLoader}
import org.apache.spark.SparkConf
import org.scalatest.funsuite.AnyFunSuite

import java.util.Properties
import org.scalamock.scalatest.MockFactory

import scala.collection.mutable

class ExecutorMetricsAnalyzerSuite extends AnyFunSuite with MockFactory {

  def createDummyAppContext(): AppContext = {

    val jobMap = new mutable.HashMap[Long, JobTimeSpan]
    for (i <- 1 to 4) {
      jobMap(i) = new JobTimeSpan(i)
    }

    val jobSQLExecIDMap = new mutable.HashMap[Long, Long]
    val r = scala.util.Random
    val sqlExecutionId = r.nextInt(10000)

    // Let, Job 1, 2 and 3 have same sqlExecutionId
    jobSQLExecIDMap(1) = sqlExecutionId
    jobSQLExecIDMap(2) = sqlExecutionId
    jobSQLExecIDMap(3) = sqlExecutionId
    jobSQLExecIDMap(4) = r.nextInt(10000)

    // Let, Job 2 and 3 are not running in parallel, even though they have same sqlExecutionId
    val baseTime = 1L
    jobMap(1).setStartTime(baseTime)
    jobMap(1).setEndTime(baseTime + 5L)

    jobMap(2).setStartTime(baseTime + 3L)
    jobMap(2).setEndTime(baseTime + 6L)

    jobMap(3).setStartTime(baseTime + 7L)
    jobMap(3).setEndTime(baseTime + 9L)

    jobMap(4).setStartTime(baseTime + 10L)
    jobMap(4).setEndTime(baseTime + 12L)

    val executorMap: mutable.HashMap[String, ExecutorTimeSpan] = mutable.HashMap()
    val executor0Timespan = new ExecutorTimeSpan("0", "0", 1)
    val executor1Timespan = new ExecutorTimeSpan("1", "0", 1)
    val executor2Timespan = new ExecutorTimeSpan("2", "0", 1)
    executor0Timespan.setStartTime(1694737416000L)
    executor0Timespan.setEndTime(1694737424000L)
    executor1Timespan.setStartTime(1694737416000L)
    executor1Timespan.setEndTime(1694737424000L)
    executor2Timespan.setStartTime(1694737422000L)
    executor2Timespan.setEndTime(1694737424000L)
    executorMap.put("0", executor0Timespan)
    executorMap.put("1", executor1Timespan)
    executorMap.put("2", executor2Timespan)

    new AppContext(
      new ApplicationInfo(appId, 1694731316000L,1694737424000L),
      new AggregateMetrics(),
      mutable.HashMap[String, HostTimeSpan](),
      executorMap,
      jobMap,
      jobSQLExecIDMap,
      mutable.HashMap[Int, StageTimeSpan](),
      mutable.HashMap[Int, Long]())
  }

  test("ExecutorMetricsAnalyzerSuite") {
    val ac = createDummyAppContext()
    val jobTime = JobOverlapHelper.estimatedTimeSpentInJobs(ac)

//    propertiesLoaderMock. (42) returning "Forty two" once
    val sparkConf = new SparkConf().set("spark.metrics.conf", metricsPropertiesPath)

    // Mocks
    val propertiesLoaderMock = stub[PropertiesLoader]
    val properties = new Properties()
    properties.setProperty("*.sink.csv.directory", csvMetricsPath)
    (propertiesLoaderMock.load _).when(metricsPropertiesPath).returns(properties)

    val csvReaderMock = stub[CsvReader]
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.driver.jvm.total.used.csv").returns(jvmTotalDriverCsv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.driver.jvm.heap.used.csv").returns(jvmHeapDriverCsv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.driver.jvm.heap.usage.csv").returns(jvmHeapUsageDriverCsv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.driver.jvm.heap.max.csv").returns(jvmHeapMaxDriverCsv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.driver.jvm.non-heap.used.csv").returns(jvmNonHeapDriverCsv)

    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.0.jvm.total.used.csv").returns(jvmTotalExec0Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.0.jvm.heap.used.csv").returns(jvmHeapExec0Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.0.jvm.heap.usage.csv").returns(jvmHeapUsageExec0Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.0.jvm.heap.max.csv").returns(jvmHeapMaxExec0Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.0.jvm.non-heap.used.csv").returns(jvmNonHeapExec0Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.0.executor.cpuTime.csv").returns(cpuTime0Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.0.executor.runTime.csv").returns(runTime0Csv)

    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.1.jvm.total.used.csv").returns(jvmTotalExec1Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.1.jvm.heap.used.csv").returns(jvmHeapExec1Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.1.jvm.heap.usage.csv").returns(jvmHeapUsageExec1Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.1.jvm.heap.max.csv").returns(jvmHeapMaxExec1Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.1.jvm.non-heap.used.csv").returns(jvmNonHeapExec1Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.1.executor.cpuTime.csv").returns(cpuTime1Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.1.executor.runTime.csv").returns(runTime1Csv)

    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.2.jvm.total.used.csv").returns(jvmTotalExec2Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.2.jvm.heap.used.csv").returns(jvmHeapExec2Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.2.jvm.heap.usage.csv").returns(jvmHeapUsageExec2Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.2.jvm.heap.max.csv").returns(jvmHeapMaxExec2Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.2.jvm.non-heap.used.csv").returns(jvmNonHeapExec2Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.2.executor.cpuTime.csv").returns(cpuTime2Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.2.executor.runTime.csv").returns(runTime2Csv)

    val executorMetricsAnalyzer = new ExecutorMetricsAnalyzer(sparkConf, csvReaderMock, propertiesLoaderMock)
    val out = executorMetricsAnalyzer.analyze(ac)

    HtmlReportGenerator.render(out, "./", Seq("Executor Timeline", "Sparkscope text"))

    println(out)

    assert(jobTime == 10, "Parallel Jobs are not being considered while computing the time spent in jobs")
  }
}

object ExecutorMetricsAnalyzerSuite {
  final val appId = "app-20230101010819-test"
  val metricsPropertiesPath = "path/to/metrics.properties"
  val csvMetricsPath = "/tmp/csv-metrics"

  val jvmTotalDriverCsv: String =
    """t,value
      |1694737411,312305768
      |1694737416,302305768
      |1694737421,400292232
      |1694737424,139839016""".stripMargin

  val jvmHeapDriverCsv: String =
    """t,value
      |1694737411,142305768
      |1694737416,133453096
      |1694737421,261379360
      |1694737424,198144808""".stripMargin

  val jvmHeapUsageDriverCsv: String =
    """t,value
      |1694737411,0.16908848762512207
      |1694737416,0.15908848762512207
      |1694737421,0.3115884780883789
      |1694737424,0.23620701789855958""".stripMargin

  val jvmHeapMaxDriverCsv: String =
    """t,value
      |1694737411,838860800
      |1694737416,838860800
      |1694737421,838860800
      |1694737424,838860800""".stripMargin

  val jvmNonHeapDriverCsv: String =
    """t,value
      |1694737411,50600840
      |1694737416,47505872
      |1694737421,50600840
      |1694737424,51593328""".stripMargin

  val jvmTotalExec0Csv: String =
    """t,value
      |1694737416,302305768
      |1694737421,400292232
      |1694737424,139839016""".stripMargin

  val jvmHeapExec0Csv: String =
    """t,value
      |1694737416,133453096
      |1694737421,261379360
      |1694737424,198144808""".stripMargin

  val jvmHeapUsageExec0Csv: String =
    """t,value
      |1694737416,0.15908848762512207
      |1694737421,0.3115884780883789
      |1694737424,0.23620701789855958""".stripMargin

  val jvmHeapMaxExec0Csv: String =
    """t,value
      |1694737416,838860800
      |1694737421,838860800
      |1694737424,838860800""".stripMargin

  val jvmNonHeapExec0Csv: String =
    """t,value
      |1694737416,47505872
      |1694737421,50600840
      |1694737424,51593328""".stripMargin

  val cpuTime0Csv: String =
    """t,count
      |1694737416,1495215972
      |1694737421,3133728967
      |1694737424,5533728967""".stripMargin

  val runTime0Csv: String =
    """t,count
      |1694737416,2686
      |1694737421,6440
      |1694737424,10840""".stripMargin

  // ---------- EXEC 1 ----------
  val jvmTotalExec1Csv: String =
    """t,value
      |1694737416,402305768
      |1694737421,500292232
      |1694737424,239839016""".stripMargin

  val jvmHeapExec1Csv: String =
    """t,value
      |1694737416,326353928
      |1694737421,166876288
      |1694737424,105890120""".stripMargin

  val jvmHeapUsageExec1Csv: String =
    """t,value
      |1694737416,0.38904419898986814
      |1694737421,0.1989320373535156
      |1694737424,0.12623085975646972""".stripMargin

  val jvmHeapMaxExec1Csv: String =
    """t,value
      |1694737416,838860800
      |1694737421,838860800
      |1694737424,838860800""".stripMargin

  val jvmNonHeapExec1Csv: String =
    """t,value
      |1694737416,47505872
      |1694737421,50600840
      |1694737424,51593328""".stripMargin

  val cpuTime1Csv: String =
    """t,count
      |1694737416,1895215972
      |1694737421,3433728967
      |1694737424,6533728967""".stripMargin

  val runTime1Csv: String =
    """t,count
      |1694737416,2686
      |1694737421,6440
      |1694737424,10840""".stripMargin

// ---------- EXEC 2 ----------
  val jvmTotalExec2Csv: String =
    """t,value
      |1694737422,600292232
      |1694737424,400292232
      |1694737424,339839016""".stripMargin

  val jvmHeapExec2Csv: String =
    """t,value
      |1694737422,326353928
      |1694737424,226353928
      |1694737424,105890120""".stripMargin

  val jvmHeapUsageExec2Csv: String =
    """t,value
      |1694737422,0.38904419898986814
      |1694737424,0.18904419898986814
      |1694737424,0.12623085975646972""".stripMargin

  val jvmHeapMaxExec2Csv: String =
    """t,value
      |1694737422,838860800
      |1694737424,838860800
      |1694737424,838860800""".stripMargin

  val jvmNonHeapExec2Csv: String =
    """t,value
      |1694737422,50600840
      |1694737424,52593328
      |1694737424,51593328""".stripMargin

  val cpuTime2Csv: String =
    """t,count
      |1694737422,995215972
      |1694737424,253728967
      |1694737424,2633728967""".stripMargin

  val runTime2Csv: String =
    """t,count
      |1694737422,2686
      |1694737424,6440
      |1694737424,6840""".stripMargin
}
