
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
import com.ucesys.sparkscope.agg.TaskAggMetrics
import com.ucesys.sparkscope.metrics._
import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.io.metrics.HadoopMetricReader
import com.ucesys.sparkscope.stats.{ClusterCPUStats, ClusterMemoryStats, DriverMemoryStats, ExecutorMemoryStats}
import com.ucesys.sparkscope.view.warning.MissingMetricsWarning
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSuite, GivenWhenThen}
import org.scalatest.MustMatchers.{a, convertToAnyMustWrapper}

class SparkScopeAnalyzerSuite extends FunSuite with MockFactory with GivenWhenThen {
    implicit val logger: SparkScopeLogger = stub[SparkScopeLogger]

    test("SparkScopeAnalyzer successful run") {
        Given("SparkScopeAnalyzer and correct driver & executormetrics")
        val ac = mockAppContext("analyzer-successful")
        val sparkScopeAnalyzer = new SparkScopeAnalyzer

        When("running SparkScopeAnalyzer.analyze")
        val result = sparkScopeAnalyzer.analyze(DriverExecutorMetricsMock, ac, TaskAggMetrics())

        Then("SparkScopeResult should contain low CPU and low heap utilization warnings")
        assert(result.warnings.length == 2)

        And("SparkScopeResult should be returned with correct values")
        assert(result.appContext.appId == ac.appId)
        assert(result.appContext.appStartTime == StartTime)
        assert(result.appContext.appEndTime.get == EndTime)
        assert(result.appContext.executorMap.size == 4)

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
            avgHeap = 215,
            avgHeapPerc = 0.26972,
            avgNonHeap = 44,
            maxNonHeap = 48
        ))

        assert(result.stats.clusterMemoryStats == ClusterMemoryStats(
            maxHeap = 840,
            avgHeap = 614,
            maxHeapPerc = 0.4165,
            avgHeapPerc = 0.26972,
            avgHeapWastedPerc = 0.73028,
            executorTimeSecs = 152,
            heapGbHoursAllocated = 0.03299,
            heapGbHoursWasted = 0.02409,
            executorHeapSizeInGb = 0.78125
        ))

        assert(result.stats.clusterCPUStats == ClusterCPUStats(
            cpuUtil = 0.55483,
            cpuNotUtil = 0.44517,
            coreHoursAllocated = 0.04222,
            coreHoursWasted = 0.0188,
            executorTimeSecs = 152,
            executorCores = 1
        ))
    }

    test("SparkScopeAnalyzer, executors not removed") {
        Given("SparkScopeAnalyzer and correct driver & executor metrics without executors removed")
        val ac = mockAppContextExecutorsNotRemoved("analyzer-executors-not-removed")
        val sparkScopeAnalyzer = new SparkScopeAnalyzer

        When("running SparkScopeAnalyzer.analyze")
        val result = sparkScopeAnalyzer.analyze(DriverExecutorMetricsMock, ac, TaskAggMetrics())

        Then("SparkScopeResult should contain low heap utilization warnings")
        assert(result.warnings.length == 1)

        And("SparkScopeResult should be returned with correct values")
        assert(result.appContext.appId == ac.appId)
        assert(result.appContext.appStartTime == StartTime)
        assert(result.appContext.appEndTime.get == EndTime)
        assert(result.appContext.executorMap.size == 4)

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
            avgHeap = 215,
            avgHeapPerc = 0.26972,
            avgNonHeap = 44,
            maxNonHeap = 48
        ))

        assert(result.stats.clusterMemoryStats == ClusterMemoryStats(
            maxHeap = 840,
            avgHeap = 614,
            maxHeapPerc = 0.4165,
            avgHeapPerc = 0.26972,
            avgHeapWastedPerc = 0.73028,
            executorTimeSecs = 140,
            heapGbHoursAllocated = 0.03038,
            heapGbHoursWasted = 0.02219,
            executorHeapSizeInGb = 0.78125
        ))

        assert(result.stats.clusterCPUStats == ClusterCPUStats(
            cpuUtil = 0.60239,
            cpuNotUtil = 0.39761,
            coreHoursAllocated = 0.03889,
            coreHoursWasted = 0.01546,
            executorTimeSecs = 140,
            executorCores = 1
        ))
    }

    test("SparkScopeAnalyzer missing metrics") {
        Given("SparkScopeAnalyzer and missing csv metrics for one executor")
        val ac = mockAppContextMissingExecutorMetrics("analyzer-missing-metrics")
        val csvReaderMock = stub[HadoopMetricReader]
        mockcorrectMetrics(csvReaderMock, ac.appId)
        val sparkScopeAnalyzer = new SparkScopeAnalyzer

        When("running SparkScopeAnalyzer.analyze")
        val result: SparkScopeResult = sparkScopeAnalyzer.analyze(DriverExecutorMetricsMock, ac, TaskAggMetrics())

        Then("Result should contain a warning regarding missing executor metrics")
        assert(result.warnings.length == 3)
        val missingMetricsWarning = result.warnings.head
        missingMetricsWarning mustBe a[MissingMetricsWarning]
        missingMetricsWarning.toString.contains("Missing metrics for 1 out of 5 executors")
        missingMetricsWarning.toString.contains("Missing metrics for the following executor ids: 5")

        And("SparkScopeResult should be returned with correct values")
        assert(result.appContext.appId == ac.appId)
        assert(result.appContext.appStartTime == StartTime)
        assert(result.appContext.appEndTime.get == EndTime)
        assert(result.appContext.executorMap.size == 5)

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
            avgHeap = 215,
            avgHeapPerc = 0.26972,
            avgNonHeap = 44,
            maxNonHeap = 48
        ))

        assert(result.stats.clusterMemoryStats == ClusterMemoryStats(
            maxHeap = 840,
            avgHeap = 614,
            maxHeapPerc = 0.4165,
            avgHeapPerc = 0.26972,
            avgHeapWastedPerc = 0.73028,
            executorTimeSecs = 152,
            heapGbHoursAllocated = 0.03299,
            heapGbHoursWasted = 0.02409,
            executorHeapSizeInGb = 0.78125
        ))

        assert(result.stats.clusterCPUStats == ClusterCPUStats(
            cpuUtil = 0.55483,
            cpuNotUtil = 0.44517,
            coreHoursAllocated = 0.04222,
            coreHoursWasted = 0.0188,
            executorTimeSecs = 152,
            executorCores = 1
        ))
    }
}
