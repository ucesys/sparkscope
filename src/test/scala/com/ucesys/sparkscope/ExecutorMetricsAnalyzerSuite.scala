
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

import com.ucesys.sparkscope.TestHelpers.{EndTime, StartTime, appId, createDummyAppContext, getPropertiesLoaderMock, sparkConf}
import com.ucesys.sparkscope.io.CsvHadoopMetricsLoader
import com.ucesys.sparkscope.metrics._
import org.scalatest.funsuite.AnyFunSuite
import org.scalamock.scalatest.MockFactory

class ExecutorMetricsAnalyzerSuite extends AnyFunSuite with MockFactory {

  test("ExecutorMetricsAnalyzerSuite") {
    val ac = createDummyAppContext()

    val metricsLoader = new CsvHadoopMetricsLoader(TestHelpers.getCsvReaderMock, ac, sparkConf, getPropertiesLoaderMock)
    val executorMetricsAnalyzer = new ExecutorMetricsAnalyzer(sparkConf, metricsLoader)
    val result = executorMetricsAnalyzer.analyze(ac)

    assert(result.sparkConf == sparkConf)

    assert(result.appInfo.applicationID == appId)
    assert(result.appInfo.startTime == StartTime)
    assert(result.appInfo.endTime == EndTime)

    assert(result.stats.driverStats == DriverMemoryStats(
      heapSize = 910,
      maxHeap = 315,
      maxHeapPerc = 34.650217943437674,
      avgHeap = 261,
      avgHeapPerc = 28.73646085899713,
      avgNonHeap = 66,
      maxNonHeap = 69
    ))

    assert(result.stats.executorStats == ExecutorMemoryStats(
      heapSize = 800,
      maxHeap = 352,
      maxHeapPerc = 44.028958320617676,
      avgHeap = 204,
      avgHeapPerc = 0.25554119351460397,
      avgNonHeap = 43,
      maxNonHeap = 48
    ))

    assert(result.stats.clusterMemoryStats == ClusterMemoryStats(
      maxHeap = 840,
      avgHeap = 632,
      maxHeapPerc = 41.65,
      avgHeapPerc = 0.25554119351460397,
      executorTimeSecs=152,
      heapGbHoursAllocated=0.03298611111111111,
      heapGbHoursWasted=0.008429310202738674,
      executorHeapSizeInGb=0.78125
    ))

    assert(result.stats.clusterCPUStats == ClusterCPUStats(
      cpuUtil = 0.5429883501184211,
      coreHoursAllocated = 0.042222222222222223,
      coreHoursWasted = 0.022926174782777777,
      executorTimeSecs = 152,
      executorCores = 1
    ))
  }
}
