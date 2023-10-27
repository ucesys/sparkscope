
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

import com.ucesys.sparkscope.eventlog.EventLogContextLoader
import com.ucesys.sparkscope.io.{MetricsLoaderFactory, PropertiesLoaderFactory, ReportGeneratorFactory}
import com.ucesys.sparkscope.common.SparkScopeLogger
import org.apache.spark.sql.SparkSession

object SparkScopeApp {

    def main(args: Array[String]): Unit = {
        implicit val logger: SparkScopeLogger = new SparkScopeLogger
        val spark = SparkSession.builder().appName("SparkScope").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")

        runFromEventLog(
            eventLogPath =  args(0),
            spark = spark,
            sparkScopeAnalyzer = new SparkScopeAnalyzer,
            eventLogContextLoader = new EventLogContextLoader,
            sparkScopeConfLoader = new SparkScopeConfLoader,
            propertiesLoaderFactory = new PropertiesLoaderFactory,
            metricsLoaderFactory = new MetricsLoaderFactory,
            reportGeneratorFactory = new ReportGeneratorFactory,
        )
    }

    def runFromEventLog(eventLogPath: String,
                        spark: SparkSession,
                        sparkScopeAnalyzer: SparkScopeAnalyzer,
                        eventLogContextLoader: EventLogContextLoader,
                        sparkScopeConfLoader: SparkScopeConfLoader,
                        propertiesLoaderFactory: PropertiesLoaderFactory,
                        metricsLoaderFactory: MetricsLoaderFactory,
                        reportGeneratorFactory: ReportGeneratorFactory)
                       (implicit logger: SparkScopeLogger): Unit = {
        val eventLogCtx = eventLogContextLoader.load(spark, eventLogPath)

        val sparkScopeRunner = new SparkScopeRunner(
            eventLogCtx.appContext,
            eventLogCtx.sparkConf,
            sparkScopeConfLoader,
            sparkScopeAnalyzer,
            propertiesLoaderFactory,
            metricsLoaderFactory,
            reportGeneratorFactory,
            Seq.empty
        )
        sparkScopeRunner.run()
    }
}
