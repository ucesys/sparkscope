
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

package com.qubole.sparklens.analyzer

import com.qubole.sparklens.analyzer.ExecutorMetricsAnalyzerSuite._
import com.qubole.sparklens.common.{AggregateMetrics, AppContext, ApplicationInfo}
import com.qubole.sparklens.helper.{CsvReader, JobOverlapHelper, PropertiesLoader}
import com.qubole.sparklens.timespan.{ExecutorTimeSpan, HostTimeSpan, JobTimeSpan, StageTimeSpan}
import org.apache.spark.SparkConf
//import org.scalatest.FunSuite
import org.scalatest.funsuite.AnyFunSuite
//import org.scalatest.mock.EasyMockSugar.mock
//import org.scalatest.mock.MockitoSugar.mock

import java.util.Properties
//import org.scalatest.mock.EasyMockSugar.mock
import org.scalamock.scalatest.MockFactory
//import org.scalamock.function._
//import org.scalamock.clazz.Mock

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
    executorMap.put("0", new ExecutorTimeSpan("0", "0", 1))
    executorMap.put("1", new ExecutorTimeSpan("1", "0", 1))
    executorMap.put("2", new ExecutorTimeSpan("2", "0", 1))

    new AppContext(
      new ApplicationInfo(appId),
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
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.0.jvm.total.used.csv").returns(jvmTotalExec0Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.0.jvm.heap.used.csv").returns(jvmHeapExec0Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.0.jvm.non-heap.used.csv").returns(jvmNonHeapExec0Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.1.jvm.total.used.csv").returns(jvmTotalExec1Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.1.jvm.heap.used.csv").returns(jvmHeapExec1Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.1.jvm.non-heap.used.csv").returns(jvmNonHeapExec1Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.2.jvm.total.used.csv").returns(jvmTotalExec2Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.2.jvm.heap.used.csv").returns(jvmHeapExec2Csv)
    (csvReaderMock.read _).when(s"${csvMetricsPath}/${appId}.2.jvm.non-heap.used.csv").returns(jvmNonHeapExec2Csv)

    val executorMetricsAnalyzer = new ExecutorMetricsAnalyzer(sparkConf, csvReaderMock, propertiesLoaderMock)
    val out = executorMetricsAnalyzer.analyze(ac)
    println(out)
    assert(jobTime == 10, "Parallel Jobs are not being considered while computing the time spent in jobs")
  }
}

object ExecutorMetricsAnalyzerSuite {
  final val appId = "123456789_0001"
  val metricsPropertiesPath = "path/to/metrics.properties"
  val csvMetricsPath = "/tmp/csv-metrics"

  val jvmTotalExec0Csv =
    """t,jvm.total.used
      |1694711710,302305768
      |1694711715,400292232
      |1694711719,139839016""".stripMargin

  val jvmHeapExec0Csv =
    """t,jvm.heap.used
      |1694711710,250875544
      |1694711715,349057656
      |1694711719,86722064""".stripMargin

  val jvmNonHeapExec0Csv =
    """t,jvm.non-heap.used
      |1694711710,47505872
      |1694711715,50600840
      |1694711719,51593328""".stripMargin

  val jvmTotalExec1Csv =
    """t,jvm.total.used
      |1694711710,402305768
      |1694711715,500292232
      |1694711719,239839016""".stripMargin

  val jvmHeapExec1Csv =
    """t,jvm.heap.used
      |1694711710,350875544
      |1694711715,449057656
      |1694711719,186722064""".stripMargin

  val jvmNonHeapExec1Csv =
    """t,jvm.non-heap.used
      |1694711710,47505872
      |1694711715,50600840
      |1694711719,51593328""".stripMargin

  val jvmTotalExec2Csv =
    """t,jvm.total.used
      |1694711710,502305768
      |1694711715,600292232
      |1694711719,339839016""".stripMargin

  val jvmHeapExec2Csv =
    """t,jvm.heap.used
      |1694711710,450875544
      |1694711715,549057656
      |1694711719,286722064""".stripMargin

  val jvmNonHeapExec2Csv =
    """t,jvm.non-heap.used
      |1694711710,47505872
      |1694711715,50600840
      |1694711719,51593328""".stripMargin
}
