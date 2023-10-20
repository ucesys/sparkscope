
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
import com.ucesys.sparkscope.io.{DriverExecutorMetrics, HtmlReportGenerator, MetricsLoader}
import com.ucesys.sparkscope.utils.Logger

import java.io.FileNotFoundException
import java.nio.file.NoSuchFileException

class SparkScopeRunner(appContext: AppContext, sparkScopeConf: SparkScopeConfig, metricsLoader: MetricsLoader, sparklensResults: Seq[String]) {

  val log = new Logger

  def run(): Unit = {
    try {
      val driverExecutorMetrics = metricsLoader.load()
      analyze(sparkScopeConf, driverExecutorMetrics)
    } catch {
      case ex: FileNotFoundException => log.error(s"SparkScope couldn't open a file. SparkScope will now exit.", ex)
      case ex: NoSuchFileException => log.error(s"SparkScope couldn't open a file. SparkScope will now exit.", ex)
      case ex: IllegalArgumentException => log.error(s"SparkScope couldn't load metrics. SparkScope will now exit.", ex)
      case ex: Exception =>  log.error(s"Unexpected exception occurred, SparkScope will now exit.", ex)
    }
  }

  def analyze(sparkScopeConf: SparkScopeConfig, driverExecutorMetrics: DriverExecutorMetrics): Unit = {
    val executorMetricsAnalyzer = new SparkScopeAnalyzer
    val sparkScopeStart = System.currentTimeMillis()
    val sparkScopeResult = executorMetricsAnalyzer.analyze(driverExecutorMetrics, appContext)
    val durationSparkScope = (System.currentTimeMillis() - sparkScopeStart) * 1f / 1000f

    log.info(s"SparkScope analysis took ${durationSparkScope}s")

    log.info(sparkScopeResult.stats.executorStats + "\n")
    log.info(sparkScopeResult.stats.driverStats + "\n")
    log.info(sparkScopeResult.stats.clusterMemoryStats + "\n")
    log.info(sparkScopeResult.stats.clusterCPUStats + "\n")

    HtmlReportGenerator.generateHtml(sparkScopeResult, sparkScopeConf.htmlReportPath, sparklensResults, sparkScopeConf.sparkConf)
  }
}
