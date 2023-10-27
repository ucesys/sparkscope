
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
import com.ucesys.sparkscope.eventlog.EventLogContextLoader
import com.ucesys.sparkscope.io._
import com.ucesys.sparkscope.utils.SparkScopeLogger
import org.apache.spark.sql.SparkSession
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FunSuite, GivenWhenThen}

import java.nio.file.{Files, Paths}

class SparkScopeAppSuite extends FunSuite with MockFactory with GivenWhenThen with BeforeAndAfterAll {
    override def beforeAll(): Unit = Files.createDirectories(Paths.get(TestDir))

    val sparkScopeConfHtmlReportPath = sparkScopeConf.copy(htmlReportPath = TestDir)
    val SparkLensOutput = Seq("Executor Timeline", "StageSkewAnalyzer text...")
    val spark = SparkSession.builder().master("local").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val appId = "app-20231025121456-0004"

    test("SparkScopeRunner offline from eventLog test") {
        implicit val logger: SparkScopeLogger = new SparkScopeLogger
        Given("Metrics for application which was upscaled and downscaled")
        val ac = mockAppContextWithDownscalingMuticore("", appId)
        val csvReaderMock = stub[HadoopFileReader]
        mockMetricsWithDownscaling(csvReaderMock, ac.appInfo.applicationID)

        val fileReaderFactoryMock = getFileReaderFactoryMock(csvReaderMock)
        val metricsLoader = new CsvHadoopMetricsLoader(fileReaderFactoryMock)
        val metricsLoaderFactory = stub[MetricsLoaderFactory]
        (metricsLoaderFactory.get _).when(*).returns(metricsLoader)

        When("SparkScopeRunner.run")
        SparkScopeApp.runFromEventLog(
            "src/test/resources/app-20231025121456-0004-eventLog-finished",
            spark,
            new SparkScopeAnalyzer,
            new EventLogContextLoader,
            new SparkScopeConfLoader,
            getPropertiesLoaderFactoryMock,
            metricsLoaderFactory,
            new ReportGeneratorFactory
        )

        Then("Report should be generated")
        assert(Files.exists(Paths.get(TestDir, ac.appInfo.applicationID + ".html")))
    }
}

