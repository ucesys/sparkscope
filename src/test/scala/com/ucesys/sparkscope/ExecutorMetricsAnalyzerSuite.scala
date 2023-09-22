
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
import com.ucesys.sparkscope.metrics.{ClusterStats, DriverStats, ExecutorStats, ResourceWasteMetrics}
import org.scalatest.funsuite.AnyFunSuite
import org.scalamock.scalatest.MockFactory

class ExecutorMetricsAnalyzerSuite extends AnyFunSuite with MockFactory {

  test("ExecutorMetricsAnalyzerSuite") {
    val ac = createDummyAppContext()

    val executorMetricsAnalyzer = new ExecutorMetricsAnalyzer(sparkConf, TestHelpers.getCsvReaderMock, getPropertiesLoaderMock)
    val result = executorMetricsAnalyzer.analyze(ac)

    assert(result.sparkConf == sparkConf)

    assert(result.appInfo.applicationID == appId)
    assert(result.appInfo.startTime == StartTime)
    assert(result.appInfo.endTime == EndTime)

    assert(result.stats.executorStats == ExecutorStats(352,44.028958320617676,204,0.25616775787085827,44,48))
    assert(result.stats.clusterStats == ClusterStats(840,614,41.65,0.25616775787085827,0.5731543695694444))
    assert(result.stats.driverStats == DriverStats(315,34.650217943437674,261,28.73646085899713,66,69))
    assert(result.resourceWasteMetrics == ResourceWasteMetrics(0.04,0.022926174782777777,0.03125,0.008005242433464321,144,0.5731543695694444,0.25616775787085827,0.78125,1))
  }
}
