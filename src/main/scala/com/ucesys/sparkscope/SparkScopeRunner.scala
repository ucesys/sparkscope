
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
import com.ucesys.sparkscope.io.{EventLogContext, MetricsLoaderFactory, PropertiesLoaderFactory, ReportGeneratorFactory}
import com.ucesys.sparkscope.utils.SparkScopeLogger

import java.io.FileNotFoundException
import java.nio.file.NoSuchFileException

class SparkScopeRunner(appContext: AppContext,
                       sparkScopeConfLoader: SparkScopeConfLoader,
                       metricsLoaderFactory: MetricsLoaderFactory,
                       reportGeneratorFactory: ReportGeneratorFactory,
                       sparklensResults: Seq[String])
                      (implicit logger: SparkScopeLogger) {
    def run(): Unit = {
        logger.info(SparkScopeSign)

        val sparkScopeConf = sparkScopeConfLoader.load()

        val metricsLoader = metricsLoaderFactory.get(sparkScopeConf, appContext)
        val reportGenerator = reportGeneratorFactory.get(sparkScopeConf)

        try {
            val driverExecutorMetrics = metricsLoader.load()
            val executorMetricsAnalyzer = new SparkScopeAnalyzer
            val sparkScopeStart = System.currentTimeMillis()
            val sparkScopeResult = executorMetricsAnalyzer.analyze(driverExecutorMetrics, appContext)
            val durationSparkScope = (System.currentTimeMillis() - sparkScopeStart) * 1f / 1000f

            logger.info(s"SparkScope analysis took ${durationSparkScope}s")

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
        }
    }
}

object SparkScopeRunner {

    implicit val logger = SparkScopeLogger.get

    def main(args: Array[String]): Unit = {

        val eventLogCtx = EventLogContext.load("")
        val sparkScopeRunner = new SparkScopeRunner(
            eventLogCtx.appContext,
            new SparkScopeConfLoader(eventLogCtx.sparkConf, new PropertiesLoaderFactory),
            new MetricsLoaderFactory,
            new ReportGeneratorFactory,
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
