
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

import com.ucesys.sparkscope.TestHelpers._
import com.ucesys.sparkscope.io.HadoopFileReader
import com.ucesys.sparkscope.metrics._
import com.ucesys.sparkscope.utils.SparkScopeLogger
import com.ucesys.sparkscope.warning.MissingMetricsWarning
import org.scalatest.FunSuite
import org.scalamock.scalatest.MockFactory
import org.scalatest.GivenWhenThen
import org.scalatest.MustMatchers.{a, convertToAnyMustWrapper}

class SparkScopeAnalyzerSuite extends FunSuite with MockFactory with GivenWhenThen {
    test("SparkScopeAnalyzer successful run") {
        Given("SparkScopeAnalyzer and correct driver & executormetrics")
        val ac = mockAppContext("analyzer-successful")
        val sparkScopeAnalyzer = new SparkScopeAnalyzer

        When("running SparkScopeAnalyzer.analyze")
        val result = sparkScopeAnalyzer.analyze(DriverExecutorMetricsMock, ac)

        Then("SparkScopeResult shouldn contain low CPU and low heap utilization warnings")
        assert(result.warnings.length == 2)

        And("SparkScopeResult should be returned with correct values")
        assert(result.appInfo.applicationID == ac.appInfo.applicationID)
        assert(result.appInfo.startTime == StartTime)
        assert(result.appInfo.endTime == EndTime)

        assert(result.stats.driverStats == DriverMemoryStats(
            heapSize = 910,
            maxHeap = 315,
            maxHeapPerc = 0.34650,
            avgHeap = 261,
            avgHeapPerc = 0.28736,
            avgNonHeap = 66,
            maxNonHeap = 69
        ))

        assert(result.stats.executorStats == ExecutorMemoryStats(
            heapSize = 800,
            maxHeap = 352,
            maxHeapPerc = 0.44029,
            avgHeap = 204,
            avgHeapPerc = 0.25554,
            avgNonHeap = 43,
            maxNonHeap = 48
        ))

        assert(result.stats.clusterMemoryStats == ClusterMemoryStats(
            maxHeap = 840,
            avgHeap = 632,
            maxHeapPerc = 0.4165,
            avgHeapPerc = 0.25554,
            executorTimeSecs=152,
            heapGbHoursAllocated=0.03299,
            heapGbHoursWasted=0.00843,
            executorHeapSizeInGb=0.78125
        ))

        assert(result.stats.clusterCPUStats == ClusterCPUStats(
            cpuUtil = 0.55483,
            coreHoursAllocated = 0.04222,
            coreHoursWasted = 0.02343,
            executorTimeSecs = 152,
            executorCores = 1
        ))
    }

  test("SparkScopeAnalyzer missing metrics") {
    Given("SparkScopeAnalyzer and missing csv metrics for one executor")
    val ac = mockAppContextMissingExecutorMetrics("analyzer-missing-metrics")
    val csvReaderMock = stub[HadoopFileReader]
    mockcorrectMetrics(csvReaderMock, ac.appInfo.applicationID)
    val sparkScopeAnalyzer = new SparkScopeAnalyzer

    When("running SparkScopeAnalyzer.analyze")
    val result: SparkScopeResult = sparkScopeAnalyzer.analyze(DriverExecutorMetricsMock, ac)

    Then("Result should contain a warning regarding missing executor metrics")
    assert(result.warnings.length == 3)
    val missingMetricsWarning = result.warnings.head
    missingMetricsWarning mustBe a[MissingMetricsWarning]
    missingMetricsWarning.toString.contains("Missing metrics for 1 out of 5 executors")
    missingMetricsWarning.toString.contains("Missing metrics for the following executor ids: 5")

    And("SparkScopeResult should be returned with correct values")
    assert(result.appInfo.applicationID == ac.appInfo.applicationID)
    assert(result.appInfo.startTime == StartTime)
    assert(result.appInfo.endTime == EndTime)

    assert(result.stats.driverStats == DriverMemoryStats(
      heapSize = 910,
      maxHeap = 315,
      maxHeapPerc = 0.34650,
      avgHeap = 261,
      avgHeapPerc = 0.28736,
      avgNonHeap = 66,
      maxNonHeap = 69
    ))

    assert(result.stats.executorStats == ExecutorMemoryStats(
      heapSize = 800,
      maxHeap = 352,
      maxHeapPerc = 0.44029,
      avgHeap = 204,
      avgHeapPerc = 0.25554,
      avgNonHeap = 43,
      maxNonHeap = 48
    ))

    assert(result.stats.clusterMemoryStats == ClusterMemoryStats(
      maxHeap = 840,
      avgHeap = 632,
      maxHeapPerc = 0.4165,
      avgHeapPerc = 0.25554,
      executorTimeSecs = 152,
      heapGbHoursAllocated = 0.03299,
      heapGbHoursWasted = 0.00843,
      executorHeapSizeInGb = 0.78125
    ))

    assert(result.stats.clusterCPUStats == ClusterCPUStats(
      cpuUtil = 0.55483,
      coreHoursAllocated = 0.04222,
      coreHoursWasted = 0.02343,
      executorTimeSecs = 152,
      executorCores = 1
    ))
  }
}
