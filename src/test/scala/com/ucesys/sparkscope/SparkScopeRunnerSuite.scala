
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
import com.ucesys.sparkscope.io.{CsvHadoopMetricsLoader, HadoopFileReader, MetricsLoaderFactory, ReportGeneratorFactory}
import com.ucesys.sparkscope.utils.SparkScopeLogger
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FunSuite, GivenWhenThen}

import java.nio.file.{Files, Paths}

class SparkScopeRunnerSuite extends FunSuite with MockFactory with GivenWhenThen with BeforeAndAfterAll {
    override def beforeAll(): Unit = Files.createDirectories(Paths.get(TestDir))

    val sparkScopeConfHtmlReportPath = sparkScopeConf.copy(htmlReportPath = TestDir)

    test("SparkScopeRunner upscaling test") {
        Given("Metrics for application which was upscaled")
        val ac = mockAppContext("runner-upscale")
        val csvReaderMock = stub[HadoopFileReader]
        mockcorrectMetrics(csvReaderMock, ac.appInfo.applicationID)

        And("SparkScopeConf with specified html report path")
        implicit val logger: SparkScopeLogger = new SparkScopeLogger
        val metricsLoader = new CsvHadoopMetricsLoader(getFileReaderFactoryMock(csvReaderMock), ac, sparkScopeConfHtmlReportPath)
        val metricsLoaderFactory = stub[MetricsLoaderFactory]
        (metricsLoaderFactory.get _).when(*, *).returns(metricsLoader)

        val sparkScopeConfLoader = stub[SparkScopeConfLoader]
        (sparkScopeConfLoader.load _).when().returns(sparkScopeConfHtmlReportPath)
        val sparkScopeRunner = new SparkScopeRunner(ac, sparkScopeConfLoader, metricsLoaderFactory, new ReportGeneratorFactory, Seq("Executor Timeline", "Sparkscope text"))

        When("SparkScopeRunner.run")
        sparkScopeRunner.run()

        Then("Report should be generated")
        assert(Files.exists(Paths.get(TestDir, ac.appInfo.applicationID + ".html")))
    }


    test("SparkScopeRunner upscaling and downscaling test") {
        Given("Metrics for application which was upscaled and downscaled")
        val ac = mockAppContextWithDownscaling("runner-upscale-downscale")
        val csvReaderMock = stub[HadoopFileReader]
        mockMetricsWithDownscaling(csvReaderMock, ac.appInfo.applicationID)

        And("SparkScopeConf with specified html report path")
        implicit val logger: SparkScopeLogger = new SparkScopeLogger
        val metricsLoader = new CsvHadoopMetricsLoader(getFileReaderFactoryMock(csvReaderMock), ac, sparkScopeConfHtmlReportPath)
        val metricsLoaderFactory = stub[MetricsLoaderFactory]
        (metricsLoaderFactory.get _).when(*, *).returns(metricsLoader)

        val sparkScopeConfLoader = stub[SparkScopeConfLoader]
        (sparkScopeConfLoader.load _).when().returns(sparkScopeConfHtmlReportPath)
        val sparkScopeRunner = new SparkScopeRunner(ac, sparkScopeConfLoader, metricsLoaderFactory, new ReportGeneratorFactory, Seq("Executor Timeline", "Sparkscope text"))

        When("SparkScopeRunner.run")
        sparkScopeRunner.run()

        Then("Report should be generated")
        assert(Files.exists(Paths.get(TestDir, ac.appInfo.applicationID + ".html")))
    }

    test("SparkScopeRunner upscaling and downscaling multicore test") {
        Given("Metrics for application which was upscaled and downscaled")
        val ac = mockAppContextWithDownscalingMuticore("runner-upscale-downscale-multicore")
        val csvReaderMock = stub[HadoopFileReader]
        mockMetricsWithDownscaling(csvReaderMock, ac.appInfo.applicationID)

        And("SparkScopeConf with specified html report path")
        implicit val logger: SparkScopeLogger = new SparkScopeLogger
        val metricsLoader = new CsvHadoopMetricsLoader(getFileReaderFactoryMock(csvReaderMock), ac, sparkScopeConfHtmlReportPath)
        val metricsLoaderFactory = stub[MetricsLoaderFactory]
        (metricsLoaderFactory.get _).when(*, *).returns(metricsLoader)

        val sparkScopeConfLoader = stub[SparkScopeConfLoader]
        (sparkScopeConfLoader.load _).when().returns(sparkScopeConfHtmlReportPath)
        val sparkScopeRunner = new SparkScopeRunner(ac, sparkScopeConfLoader, metricsLoaderFactory, new ReportGeneratorFactory, Seq("Executor Timeline", "Sparkscope text"))

        When("SparkScopeRunner.run")
        sparkScopeRunner.run()

        Then("Report should be generated")
        assert(Files.exists(Paths.get(TestDir, ac.appInfo.applicationID + ".html")))
    }
}

