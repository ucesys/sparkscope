
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
import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.io.metrics.{CsvMetricsLoader, HadoopMetricReader, MetricsLoaderFactory}
import com.ucesys.sparkscope.io.property.PropertiesLoaderFactory
import com.ucesys.sparkscope.metrics.{ClusterCPUStats, ClusterMemoryStats, DriverMemoryStats, ExecutorMemoryStats}
import com.ucesys.sparkscope.view.ReportGeneratorFactory
import org.apache.spark.SparkConf
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FunSuite, GivenWhenThen}

import java.nio.file.{Files, Paths}

class SparkScopeRunnerSuite extends FunSuite with MockFactory with GivenWhenThen with BeforeAndAfterAll {
    override def beforeAll(): Unit = Files.createDirectories(Paths.get(TestDir))

    val sparkScopeConfHtmlReportPath = sparkScopeConf.copy(htmlReportPath = TestDir)
    val SparkLensOutput = Seq("Executor Timeline", "StageSkewAnalyzer text...")

    test("SparkScopeRunner.run upscaling test") {
        Given("Metrics for application which was upscaled")
        val ac = mockAppContext("runner-upscale")
        val csvReaderMock = stub[HadoopMetricReader]
        mockcorrectMetrics(csvReaderMock, ac.appId)

        And("SparkScopeConf with specified html report path")
        implicit val logger: SparkScopeLogger = new SparkScopeLogger
        val metricsLoader = new CsvMetricsLoader(csvReaderMock)
        val metricsLoaderFactory = stub[MetricsLoaderFactory]
        (metricsLoaderFactory.get _).when(*, *).returns(metricsLoader)

        val sparkScopeConfLoader = stub[SparkScopeConfLoader]
        (sparkScopeConfLoader.load _).when(*, *).returns(sparkScopeConfHtmlReportPath)
        val sparkScopeRunner = new SparkScopeRunner(
            new SparkConf,
            sparkScopeConfLoader,
            new SparkScopeAnalyzer,
            new PropertiesLoaderFactory,
            metricsLoaderFactory,
            new ReportGeneratorFactory
        )

        When("SparkScopeRunner.run")
        sparkScopeRunner.run(ac)

        Then("Report should be generated")
        assert(Files.exists(Paths.get(TestDir, ac.appId + ".html")))
    }


    test("SparkScopeRunner.run upscaling and downscaling test") {
        Given("Metrics for application which was upscaled and downscaled")
        val ac = mockAppContextWithDownscaling("runner-upscale-downscale")
        val csvReaderMock = stub[HadoopMetricReader]
        mockMetricsWithDownscaling(csvReaderMock, ac.appId)

        And("SparkScopeConf with specified html report path")
        implicit val logger: SparkScopeLogger = new SparkScopeLogger
        val metricsLoader = new CsvMetricsLoader(csvReaderMock)
        val metricsLoaderFactory = stub[MetricsLoaderFactory]
        (metricsLoaderFactory.get _).when(*, *).returns(metricsLoader)

        val sparkScopeConfLoader = stub[SparkScopeConfLoader]
        (sparkScopeConfLoader.load _).when(*, *).returns(sparkScopeConfHtmlReportPath)
        val sparkScopeRunner = new SparkScopeRunner(
            new SparkConf,
            sparkScopeConfLoader,
            new SparkScopeAnalyzer,
            new PropertiesLoaderFactory,
            metricsLoaderFactory,
            new ReportGeneratorFactory
        )

        When("SparkScopeRunner.run")
        sparkScopeRunner.run(ac)

        Then("Report should be generated")
        assert(Files.exists(Paths.get(TestDir, ac.appId + ".html")))
    }

    test("SparkScopeRunner.run upscaling and downscaling multicore test") {
        Given("Metrics for application which was upscaled and downscaled")
        val ac = mockAppContextWithDownscalingMuticore("runner-upscale-downscale-multicore")
        val csvReaderMock = stub[HadoopMetricReader]
        mockMetricsWithDownscaling(csvReaderMock, ac.appId)

        And("SparkScopeConf with specified html report path")
        implicit val logger: SparkScopeLogger = new SparkScopeLogger
        val metricsLoader = new CsvMetricsLoader(csvReaderMock)
        val metricsLoaderFactory = stub[MetricsLoaderFactory]
        (metricsLoaderFactory.get _).when(*, *).returns(metricsLoader)

        val sparkScopeConfLoader = stub[SparkScopeConfLoader]
        (sparkScopeConfLoader.load _).when(*, *).returns(sparkScopeConfHtmlReportPath)
        val sparkScopeRunner = new SparkScopeRunner(
            new SparkConf,
            sparkScopeConfLoader,
            new SparkScopeAnalyzer,
            getPropertiesLoaderFactoryMock,
            metricsLoaderFactory,
            new ReportGeneratorFactory
        )

        When("SparkScopeRunner.run")
        sparkScopeRunner.run(ac)

        Then("Report should be generated")
        assert(Files.exists(Paths.get(TestDir, ac.appId + ".html")))
    }

    test("SparkScopeRunner.runAnalysis test") {
        implicit val logger: SparkScopeLogger = new SparkScopeLogger

        Given("correct metrics for application")
        val ac = mockAppContext("runner-analysis-upscale-downscale")
        val csvReaderMock = stub[HadoopMetricReader]
        mockcorrectMetrics(csvReaderMock, ac.appId)
        val metricsLoader = new CsvMetricsLoader(csvReaderMock)
        val metricsLoaderFactory = stub[MetricsLoaderFactory]
        (metricsLoaderFactory.get _).when(*, *).returns(metricsLoader)

        val sparkScopeRunner = new SparkScopeRunner(
            new SparkConf,
            stub[SparkScopeConfLoader],
            new SparkScopeAnalyzer,
            getPropertiesLoaderFactoryMock,
            metricsLoaderFactory,
            new ReportGeneratorFactory
        )

        When("SparkScopeRunner.runAnalysis")
        val result = sparkScopeRunner.runAnalysis(sparkScopeConfHtmlReportPath, ac)

        Then("Report should be generated")
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

    test("SparkScopeRunner.runAnalysis upscaling and downscaling test") {
        implicit val logger: SparkScopeLogger = new SparkScopeLogger

        Given("Metrics for application which was upscaled and downscaled")
        val ac = mockAppContextWithDownscaling("runner-analysis-upscale-downscale")
        val csvReaderMock = stub[HadoopMetricReader]
        mockMetricsWithDownscaling(csvReaderMock, ac.appId)
        val metricsLoader = new CsvMetricsLoader(csvReaderMock)
        val metricsLoaderFactory = stub[MetricsLoaderFactory]
        (metricsLoaderFactory.get _).when(*, *).returns(metricsLoader)

        val sparkScopeRunner = new SparkScopeRunner(
            new SparkConf,
            stub[SparkScopeConfLoader],
            new SparkScopeAnalyzer,
            getPropertiesLoaderFactoryMock,
            metricsLoaderFactory,
            new ReportGeneratorFactory
        )

        When("SparkScopeRunner.run")
        val result = sparkScopeRunner.runAnalysis(sparkScopeConfHtmlReportPath, ac)

        Then("Report should be generated")
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
            avgHeap = 212,
            avgHeapPerc = 0.2662,
            avgNonHeap = 43,
            maxNonHeap = 48
        ))

        assert(result.stats.clusterMemoryStats == ClusterMemoryStats(
            maxHeap = 1079,
            avgHeap = 621,
            maxHeapPerc = 0.4165,
            avgHeapPerc = 0.2662,
            avgHeapWastedPerc = 0.7338,
            executorTimeSecs = 180,
            heapGbHoursAllocated = 0.03906,
            heapGbHoursWasted = 0.02866,
            executorHeapSizeInGb = 0.78125
        ))

        assert(result.stats.clusterCPUStats == ClusterCPUStats(
            cpuUtil = 0.56276,
            cpuNotUtil = 0.43724,
            coreHoursAllocated = 0.05,
            coreHoursWasted = 0.02186,
            executorTimeSecs = 180,
            executorCores = 1
        ))
    }

    test("SparkScopeRunner.runAnalysis upscaling and downscaling multicore test") {
        implicit val logger: SparkScopeLogger = new SparkScopeLogger

        Given("Metrics for application which was upscaled and downscaled")
        val ac = mockAppContextWithDownscalingMuticore("runner-analysis-upscale-downscale-multicore")
        val csvReaderMock = stub[HadoopMetricReader]
        mockMetricsWithDownscaling(csvReaderMock, ac.appId)
        val metricsLoader = new CsvMetricsLoader(csvReaderMock)
        val metricsLoaderFactory = stub[MetricsLoaderFactory]
        (metricsLoaderFactory.get _).when(*, *).returns(metricsLoader)

        val sparkScopeRunner = new SparkScopeRunner(
            new SparkConf,
            stub[SparkScopeConfLoader],
            new SparkScopeAnalyzer,
            getPropertiesLoaderFactoryMock,
            metricsLoaderFactory,
            new ReportGeneratorFactory
        )

        When("SparkScopeRunner.run")
        val result = sparkScopeRunner.runAnalysis(sparkScopeConfHtmlReportPath, ac)

        Then("Report should be generated")
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
            avgHeap = 202,
            avgHeapPerc = 0.25273,
            avgNonHeap = 43,
            maxNonHeap = 48
        ))

        assert(result.stats.clusterMemoryStats == ClusterMemoryStats(
            maxHeap = 1079,
            avgHeap = 640,
            maxHeapPerc = 0.4165,
            avgHeapPerc = 0.25273,
            avgHeapWastedPerc = 0.74727,
            executorTimeSecs = 180,
            heapGbHoursAllocated = 0.03906,
            heapGbHoursWasted = 0.02919,
            executorHeapSizeInGb = 0.78125
        ))

        assert(result.stats.clusterCPUStats == ClusterCPUStats(
            cpuUtil = 0.28138,
            cpuNotUtil = 0.71862,
            coreHoursAllocated = 0.1,
            coreHoursWasted = 0.07186,
            executorTimeSecs = 180,
            executorCores = 2
        ))
    }
}

