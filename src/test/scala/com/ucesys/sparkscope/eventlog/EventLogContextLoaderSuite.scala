
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

package com.ucesys.sparkscope.eventlog

import com.ucesys.sparkscope.utils.SparkScopeLogger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FunSuite, GivenWhenThen}


class EventLogContextLoaderSuite extends FunSuite with MockFactory with GivenWhenThen {
    implicit val logger: SparkScopeLogger = new SparkScopeLogger

    val spark = SparkSession.builder().master("local").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    test("SparkScopeRunner offline app finished") {
        Given("Metrics for application which was upscaled and downscaled")
        val eventLogContextLoader = new EventLogContextLoader

        When("EventLogContextLoader.load")

        val eventLogContext = eventLogContextLoader.load(
            spark,
            s"src/test/resources/app-20231025121456-0004-eventLog-finished",
        )

        Then("App id, startTime, endTime should be read from app start/end events")
        assert(eventLogContext.appContext.appInfo.applicationID == "app-20231025121456-0004")
        assert(eventLogContext.appContext.appInfo.startTime == 1698236095722L)
        assert(eventLogContext.appContext.appInfo.endTime == 1698236104099L)

        And("Executor timeline should be read from executor add/remove events")
        assert(eventLogContext.appContext.executorMap.size == 2)
        assert(eventLogContext.appContext.executorMap("0").startTime == 1698236098507L)
        assert(eventLogContext.appContext.executorMap("0").endTime == 0)
        assert(eventLogContext.appContext.executorMap("0").cores == 2)
        assert(eventLogContext.appContext.executorMap("1").startTime == 1698236098540L)
        assert(eventLogContext.appContext.executorMap("1").endTime == 0)
        assert(eventLogContext.appContext.executorMap("1").cores == 2)

        And("SparkConf should be read from env update event")
        assertSparkConf(eventLogContext.sparkConf)
    }

    test("SparkScopeRunner offline app running") {
        Given("Metrics for application which was upscaled and downscaled")
        val eventLogContextLoader = new EventLogContextLoader

        When("EventLogContextLoader.load")
        val eventLogContext = eventLogContextLoader.load(
            spark,
            s"src/test/resources/app-20231025121456-0004-eventLog-running",
        )

        Then("App id, startTime, endTime should be read from app start/end events")
        assert(eventLogContext.appContext.appInfo.applicationID == "app-20231025121456-0004")
        assert(eventLogContext.appContext.appInfo.startTime == 1698236095722L)
        assert(eventLogContext.appContext.appInfo.endTime == 0)

        And("Executor timeline should be read from executor add/remove events")
        assert(eventLogContext.appContext.executorMap.size == 2)
        assert(eventLogContext.appContext.executorMap("0").startTime == 1698236098507L)
        assert(eventLogContext.appContext.executorMap("0").endTime == 0)
        assert(eventLogContext.appContext.executorMap("0").cores == 2)
        assert(eventLogContext.appContext.executorMap("1").startTime == 1698236098540L)
        assert(eventLogContext.appContext.executorMap("1").endTime == 0)
        assert(eventLogContext.appContext.executorMap("1").cores == 2)

        And("SparkConf should be read from env update event")
        assertSparkConf(eventLogContext.sparkConf)

    }

    test("SparkScopeRunner offline app finished executors removed") {
        Given("Metrics for application which was upscaled and downscaled")
        val eventLogContextLoader = new EventLogContextLoader

        When("EventLogContextLoader.load")
        val eventLogContext = eventLogContextLoader.load(
            spark,
            s"src/test/resources/app-20231025121456-0004-eventLog-finished-exec-removed",
        )

        Then("App id, startTime, endTime should be read from app start/end events")
        assert(eventLogContext.appContext.appInfo.applicationID == "app-20231025121456-0004")
        assert(eventLogContext.appContext.appInfo.startTime == 1698236095722L)
        assert(eventLogContext.appContext.appInfo.endTime == 1698236104099L)

        And("Executor timeline should be read from executor add/remove events")
        assert(eventLogContext.appContext.executorMap.size == 2)
        assert(eventLogContext.appContext.executorMap("0").startTime == 1698236098507L)
        assert(eventLogContext.appContext.executorMap("0").endTime == 1698236102012L)
        assert(eventLogContext.appContext.executorMap("0").cores == 2)
        assert(eventLogContext.appContext.executorMap("1").startTime == 1698236098540L)
        assert(eventLogContext.appContext.executorMap("1").endTime == 1698236103345L)
        assert(eventLogContext.appContext.executorMap("1").cores == 2)

        And("SparkConf should be read from env update event")
        assertSparkConf(eventLogContext.sparkConf)

    }

    test("SparkScopeRunner offline app running executors removed") {
        Given("Metrics for application which was upscaled and downscaled")
        val eventLogContextLoader = new EventLogContextLoader

        When("EventLogContextLoader.load")
        val eventLogContext = eventLogContextLoader.load(
            spark,
            s"src/test/resources/app-20231025121456-0004-eventLog-running-exec-removed",
        )

        Then("App id, startTime, endTime should be read from app start/end events")
        assert(eventLogContext.appContext.appInfo.applicationID == "app-20231025121456-0004")
        assert(eventLogContext.appContext.appInfo.startTime == 1698236095722L)
        assert(eventLogContext.appContext.appInfo.endTime == 0)

        And("Executor timeline should be read from executor add/remove events")
        assert(eventLogContext.appContext.executorMap.size == 2)
        assert(eventLogContext.appContext.executorMap("0").startTime == 1698236098507L)
        assert(eventLogContext.appContext.executorMap("0").endTime == 1698236102012L)
        assert(eventLogContext.appContext.executorMap("0").cores == 2)
        assert(eventLogContext.appContext.executorMap("1").startTime == 1698236098540L)
        assert(eventLogContext.appContext.executorMap("1").endTime == 1698236103345L)
        assert(eventLogContext.appContext.executorMap("1").cores == 2)

        And("SparkConf should be read from env update event")
        assertSparkConf(eventLogContext.sparkConf)
    }

    def assertSparkConf(sparkConf: SparkConf): Unit = {
        assert(sparkConf.get("spark.eventLog.enabled") == "true")
        assert(sparkConf.get("spark.executor.memory") == "900m")
        assert(sparkConf.get("spark.app.startTime") == "1698236095722")
        assert(sparkConf.get("spark.executor.id") == "driver")
        assert(sparkConf.get("spark.jars") == "file:///tmp/jars/sparkscope-spark3-0.1.1-SNAPSHOT.jar,file:/tmp/jars/spark-examples_2.10-1.1.1.jar")
        assert(sparkConf.get("spark.executor.cores") == "2")
        assert(sparkConf.get("spark.eventLog.dir") == "/tmp/spark-events")
        assert(sparkConf.get("spark.app.id") == "app-20231025121456-0004")
        assert(sparkConf.get("spark.metrics.conf") == "path/to/metrics.properties")
        assert(sparkConf.get("spark.driver.port") == "40457")
        assert(sparkConf.get("spark.master") == "spark://spark-master:7077")
        assert(sparkConf.get("spark.extraListeners") == "com.ucesys.sparkscope.SparkScopeJobListener")
        assert(sparkConf.get("spark.executor.instances") == "2")
        assert(sparkConf.get("spark.app.name") == "Spark Pi")
    }
}

