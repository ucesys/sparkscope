
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

import com.ucesys.sparkscope.common.{AppContext, SparkScopeConf, SparkScopeLogger}
import com.ucesys.sparkscope.SparkScopeRunner.SparkScopeSign
import com.ucesys.sparkscope.agg.TaskAggMetrics
import com.ucesys.sparkscope.io.metrics.{MetricReaderFactory, MetricsLoaderFactory}
import com.ucesys.sparkscope.io.property.PropertiesLoaderFactory
import com.ucesys.sparkscope.metrics.SparkScopeResult
import com.ucesys.sparkscope.view.ReportGeneratorFactory
import org.apache.spark.SparkConf

import java.io.FileNotFoundException
import java.nio.file.NoSuchFileException

class SparkScopeRunner(sparkScopeConfLoader: SparkScopeConfLoader,
                       sparkScopeAnalyzer: SparkScopeAnalyzer,
                       propertiesLoaderFactory: PropertiesLoaderFactory,
                       metricsLoaderFactory: MetricsLoaderFactory,
                       reportGeneratorFactory: ReportGeneratorFactory)
                      (implicit val logger: SparkScopeLogger) {
    def run(appContext: AppContext, sparkConf: SparkConf, taskAggMetrics: TaskAggMetrics): Unit = {
        logger.info(SparkScopeSign, this.getClass)

        val sparkScopeStart = System.currentTimeMillis()

        try {
            val sparkScopeConf = sparkScopeConfLoader.load(sparkConf, propertiesLoaderFactory)
            val sparkScopeResult = this.runAnalysis(sparkScopeConf, appContext, taskAggMetrics)

            logger.info(s"${sparkScopeResult.stats.executorStats}\n", this.getClass)
            logger.info(s"${sparkScopeResult.stats.driverStats}\n", this.getClass)
            logger.info(s"${sparkScopeResult.stats.clusterMemoryStats}\n", this.getClass)
            logger.info(s"${sparkScopeResult.stats.clusterCPUStats}\n", this.getClass)

            reportGeneratorFactory.get(sparkScopeConf).generate(sparkScopeResult)
        } catch {
            case ex: FileNotFoundException => logger.error(s"SparkScope couldn't open a file. SparkScope will now exit.", ex, this.getClass)
            case ex: NoSuchFileException => logger.error(s"SparkScope couldn't open a file. SparkScope will now exit.", ex, this.getClass)
            case ex: IllegalArgumentException => logger.error(s"SparkScope couldn't load metrics. SparkScope will now exit.", ex, this.getClass)
            case ex: Exception => logger.error(s"Unexpected exception occurred, SparkScope will now exit.", ex, this.getClass)
        } finally {
            val durationSparkScope = (System.currentTimeMillis() - sparkScopeStart) * 1f / 1000f
            logger.info(s"SparkScope analysis took ${durationSparkScope}s", this.getClass)
        }
    }

    def runAnalysis(sparkScopeConf: SparkScopeConf, appContext: AppContext, taskAggMetrics: TaskAggMetrics): SparkScopeResult = {
        val metricsLoader = metricsLoaderFactory.get(sparkScopeConf, appContext)
        val driverExecutorMetrics = metricsLoader.load(appContext, sparkScopeConf)
        sparkScopeAnalyzer.analyze(driverExecutorMetrics, appContext, taskAggMetrics)
    }
}

object SparkScopeRunner {
    val SparkScopeSign =
        """
          |     ____              __    ____
          |    / __/__  ___ _____/ /__ / __/_ ___  ___  ___
          |   _\ \/ _ \/ _ `/ __/  '_/_\ \/_ / _ \/ _ \/__/
          |  /___/ .__/\_,_/_/ /_/\_\/___/\__\_,_/ .__/\___/
          |     /_/                             /_/    spark3-v0.1.8
          |""".stripMargin

    def apply(): SparkScopeRunner = {
        implicit val logger = new SparkScopeLogger
        new SparkScopeRunner(
            new SparkScopeConfLoader,
            new SparkScopeAnalyzer,
            new PropertiesLoaderFactory,
            new MetricsLoaderFactory(new MetricReaderFactory(offline = false)),
            new ReportGeneratorFactory
        )
    }
}
