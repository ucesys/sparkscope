
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

package com.ucesys.sparkscope.io.metrics

import com.ucesys.sparkscope.TestHelpers._
import com.ucesys.sparkscope.common.SparkScopeLogger
import org.scalatest.MustMatchers.{a, convertToAnyMustWrapper}
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSuite, GivenWhenThen}

class MetricReaderFactorySuite extends FunSuite with MockFactory with GivenWhenThen {
    implicit val logger: SparkScopeLogger = stub[SparkScopeLogger]

    val metricsReaderFactory = new MetricReaderFactory(false)
    val metricsReaderFactoryOffline = new MetricReaderFactory(true)

    val appContext = mockAppContext(SampleAppId, "MetricReaderFactorySuite")


    test("HDFS MetricReaderFactory test") {
        Given("HDFS driverMetricsDir")
        val sparkScopeConfHadoop = sparkScopeConf.copy(
            driverMetricsDir = "hdfs:///path/to/hdfs"
        )

        When("calling MetricReaderFactory.getMetricReader")
        val metricReader = metricsReaderFactory.getMetricReader(sparkScopeConfHadoop, appContext)

        Then("HadoopMetricReader should be returned")
        metricReader mustBe a[HadoopMetricReader]
    }

    test("MaprFS MetricReaderFactory test") {
        Given("MaprFS driverMetricsDir")
        val sparkScopeConfHadoop = sparkScopeConf.copy(
            driverMetricsDir = "maprfs:///path/to/maprfs"
        )

        When("calling MetricReaderFactory.getMetricReader")
        val metricReader = metricsReaderFactory.getMetricReader(sparkScopeConfHadoop, appContext)

        Then("HadoopMetricReader should be returned")
        metricReader mustBe a[HadoopMetricReader]
    }

    test("Local Hadoop MetricReaderFactory test") {
        Given("Local Hadoop driverMetricsDir")
        val sparkScopeConfHadoop = sparkScopeConf.copy(
            driverMetricsDir = "file://path/to/localfs"
        )

        When("calling MetricReaderFactory.getMetricReader")
        val metricReader = metricsReaderFactory.getMetricReader(sparkScopeConfHadoop, appContext)

        Then("HadoopMetricReader should be returned")
        metricReader mustBe a[HadoopMetricReader]
    }

    test("Local absolute MetricReaderFactory test") {
        Given("Local absolute driverMetricsDir")
        val sparkScopeConfLocal = sparkScopeConf.copy(
            driverMetricsDir = "/abs/path/"
        )

        When("calling MetricReaderFactory.getMetricReader")
        val metricReader = metricsReaderFactory.getMetricReader(sparkScopeConfLocal, appContext)

        Then("HadoopMetricReader should be returned")
        metricReader mustBe a[LocalMetricReader]
    }

    test("Local relative MetricReaderFactory test") {
        Given("Local relative driverMetricsDir")
        val sparkScopeConfLocal = sparkScopeConf.copy(
            driverMetricsDir = "relative/path/"
        )

        When("calling MetricReaderFactory.getMetricReader")
        val metricReader = metricsReaderFactory.getMetricReader(sparkScopeConfLocal, appContext)

        Then("HadoopMetricReader should be returned")
        metricReader mustBe a[LocalMetricReader]
    }

    test("S3 attached MetricReaderFactory test") {
        Given("S3 driverMetricsDir")
        val sparkScopeConfLocal = sparkScopeConf.copy(
            driverMetricsDir = "s3://bucket-without-directory",
            executorMetricsDir = "s3://bucket-without-directory",
            region = Some("us-east-1")
        )

        When("calling MetricReaderFactory.getMetricReader")
        val caught = intercept[IllegalArgumentException] {
            val metricReader = metricsReaderFactory.getMetricReader(sparkScopeConfLocal, appContext)
        }

        Then("S3CleanupMetricReader should be returned")
        assert(caught.getStackTrace.exists(_.getFileName == "S3CleanupMetricReader.scala"))

        And("IllegalArgumentException should be thrown with s3 parse error")
        assert(caught.getMessage == "Couldn't parse s3 directory path: s3://bucket-without-directory")
    }

    test("S3 offline MetricReaderFactory test") {
        Given("S3 driverMetricsDir")
        val sparkScopeConfLocal = sparkScopeConf.copy(
            driverMetricsDir = "s3://bucket/key/",
            executorMetricsDir = "s3://bucket/key/",
            region = Some("us-east-1")
        )

        When("calling MetricReaderFactory.getMetricReader")
        val metricReader = metricsReaderFactoryOffline.getMetricReader(sparkScopeConfLocal, appContext)

        Then("HadoopMetricReader should be returned")
        metricReader mustBe a[S3OfflineMetricReader]
    }
}
