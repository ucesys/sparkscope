
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

package com.ucesys.sparkscope.io

import com.ucesys.sparkscope.eventlog.EventLogContextLoader
import com.ucesys.sparkscope.utils.SparkScopeLogger
import org.apache.spark.sql.SparkSession
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FunSuite, GivenWhenThen}


class EventLogContextSuite extends FunSuite with MockFactory with GivenWhenThen {

    test("SparkScopeRunner offline from eventLog test") {
        implicit val logger: SparkScopeLogger = new SparkScopeLogger
        val appId = "app-20231025121456-0004"
        Given("Metrics for application which was upscaled and downscaled")

        val spark = SparkSession.builder().master("local").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        When("SparkScopeRunner.run")
        val eventLogContextLoader = new EventLogContextLoader
        val eventLogContext = eventLogContextLoader.load(
            spark,
            s"src/test/resources/app-20231025121456-0004-eventLog",
        )

        Then("App id, startTime, endTime should be read from app start/end events")
        assert(eventLogContext.appContext.appInfo.applicationID == "app-20231025121456-0004")
        assert(eventLogContext.appContext.appInfo.startTime == 1698236095722L)
        assert(eventLogContext.appContext.appInfo.endTime == 1698236104099L)

        And("Executor timeline should be read from executor add/remove events")
        assert(eventLogContext.appContext.executorMap.size == 2)
        assert(eventLogContext.appContext.executorMap("0").startTime == 1698236098507L)
        assert(eventLogContext.appContext.executorMap("1").startTime == 1698236098540L)
        assert(eventLogContext.appContext.executorMap("0").endTime == 0)
        assert(eventLogContext.appContext.executorMap("1").endTime == 0)

        And("SparkConf should be read from env update event")
        assert(eventLogContext.sparkConf.get("spark.eventLog.enabled") == "true")
        assert(eventLogContext.sparkConf.get("spark.executor.memory") == "900m")
        assert(eventLogContext.sparkConf.get("spark.app.startTime") == "1698236095722")
        assert(eventLogContext.sparkConf.get("spark.executor.id") == "driver")
        assert(eventLogContext.sparkConf.get("spark.jars") == "file:///tmp/jars/sparkscope-spark3-0.1.1-SNAPSHOT.jar,file:/tmp/jars/spark-examples_2.10-1.1.1.jar")
        assert(eventLogContext.sparkConf.get("spark.executor.cores") == "2")
        assert(eventLogContext.sparkConf.get("spark.eventLog.dir") == "/tmp/spark-events")
        assert(eventLogContext.sparkConf.get("spark.app.id") == "app-20231025121456-0004")
        assert(eventLogContext.sparkConf.get("spark.metrics.conf") == "path/to/metrics.properties")
        assert(eventLogContext.sparkConf.get("spark.driver.port") == "40457")
        assert(eventLogContext.sparkConf.get("spark.master") == "spark://spark-master:7077")
        assert(eventLogContext.sparkConf.get("spark.extraListeners") == "com.ucesys.sparkscope.SparkScopeJobListener")
        assert(eventLogContext.sparkConf.get("spark.executor.instances") == "2")
        assert(eventLogContext.sparkConf.get("spark.app.name") == "Spark Pi")
    }
}

