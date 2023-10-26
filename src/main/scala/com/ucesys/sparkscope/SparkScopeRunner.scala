
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

import com.ucesys.sparklens.common.AppContext
import com.ucesys.sparkscope.SparkScopeRunner.SparkScopeSign
import com.ucesys.sparkscope.eventlog.{EventLogContext, EventLogContextLoader}
import com.ucesys.sparkscope.io.{MetricsLoaderFactory, PropertiesLoaderFactory, ReportGeneratorFactory}
import com.ucesys.sparkscope.utils.SparkScopeLogger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrameReader, SparkSession}

import java.io.FileNotFoundException
import java.nio.file.NoSuchFileException

class SparkScopeRunner(appContext: AppContext,
                       sparkConf: SparkConf,
                       sparkScopeConfLoader: SparkScopeConfLoader,
                       sparkScopeAnalyzer: SparkScopeAnalyzer,
                       propertiesLoaderFactory: PropertiesLoaderFactory,
                       metricsLoaderFactory: MetricsLoaderFactory,
                       reportGeneratorFactory: ReportGeneratorFactory,
                       sparklensResults: Seq[String])
                      (implicit logger: SparkScopeLogger) {
    def run(): Unit = {
        val sparkScopeStart = System.currentTimeMillis()

        logger.info(SparkScopeSign)

        val sparkScopeConf = sparkScopeConfLoader.load(sparkConf, propertiesLoaderFactory)

        val metricsLoader = metricsLoaderFactory.get(sparkScopeConf, appContext)
        val reportGenerator = reportGeneratorFactory.get(sparkScopeConf)

        try {
            val driverExecutorMetrics = metricsLoader.load()
            val sparkScopeResult = sparkScopeAnalyzer.analyze(driverExecutorMetrics, appContext)

            logger.info(sparkScopeResult.stats.executorStats + "\n")
            logger.info(sparkScopeResult.stats.driverStats + "\n")
            logger.info(sparkScopeResult.stats.clusterMemoryStats + "\n")
            logger.info(sparkScopeResult.stats.clusterCPUStats + "\n")

            reportGenerator.generate(sparkScopeResult, sparklensResults)
        } catch {
            case ex: FileNotFoundException => logger.error(s"SparkScope couldn't open a file. SparkScope will now exit.", ex)
            case ex: NoSuchFileException => logger.error(s"SparkScope couldn't open a file. SparkScope will now exit.", ex)
            case ex: IllegalArgumentException => logger.error(s"SparkScope couldn't load metrics. SparkScope will now exit.", ex)
            case ex: Exception => logger.error(s"Unexpected exception occurred, SparkScope will now exit.", ex)
        } finally {
            val durationSparkScope = (System.currentTimeMillis() - sparkScopeStart) * 1f / 1000f
            logger.info(s"SparkScope analysis took ${durationSparkScope}s")
        }
    }
}

object SparkScopeRunner {

    def main(args: Array[String]): Unit = {
        implicit val logger: SparkScopeLogger = new SparkScopeLogger

        runFromEventLog(
            eventLogPath =  args(0),
            sparkScopeAnalyzer = new SparkScopeAnalyzer,
            eventLogContextLoader = new EventLogContextLoader,
            sparkScopeConfLoader = new SparkScopeConfLoader,
            propertiesLoaderFactory = new PropertiesLoaderFactory,
            metricsLoaderFactory = new MetricsLoaderFactory,
            reportGeneratorFactory = new ReportGeneratorFactory,
        )
    }

    def runFromEventLog(eventLogPath: String,
                        sparkScopeAnalyzer: SparkScopeAnalyzer,
                        eventLogContextLoader: EventLogContextLoader,
                        sparkScopeConfLoader: SparkScopeConfLoader,
                        propertiesLoaderFactory: PropertiesLoaderFactory,
                        metricsLoaderFactory: MetricsLoaderFactory,
                        reportGeneratorFactory: ReportGeneratorFactory)
                       (implicit logger: SparkScopeLogger): Unit = {
        val spark = SparkSession.builder().appName("SparkScope").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
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

    val SparkScopeSign =
        """
          |     ____              __    ____
          |    / __/__  ___ _____/ /__ / __/_ ___  ___  ___
          |   _\ \/ _ \/ _ `/ __/  '_/_\ \/_ / _ \/ _ \/__/
          |  /___/ .__/\_,_/_/ /_/\_\/___/\__\_,_/ .__/\___/
          |     /_/                             /_/    spark3-v0.1.1
          |""".stripMargin
}
