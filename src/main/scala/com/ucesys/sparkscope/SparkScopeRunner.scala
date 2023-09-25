
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
import com.ucesys.sparkscope.io.{CsvHadoopMetricsLoader, CsvHadoopReader, DriverExecutorMetrics, HadoopPropertiesLoader, HtmlReportGenerator, MetricsLoader}
import org.apache.spark.SparkConf

import java.io.FileNotFoundException

class SparkScopeRunner(appContext: AppContext, sparkConf: SparkConf, metricsLoader: MetricsLoader, sparklensResults: Seq[String]) {

  def run(): Unit = {
    try {
      val driverExecutorMetrics = metricsLoader.load()
      analyze(driverExecutorMetrics)
    } catch {
      case ex: IllegalArgumentException => {
        println(s"[SparkScope]${ex}, retrying in 5 secs...")
        Thread.sleep(5000)
        val driverExecutorMetrics = metricsLoader.load()
        analyze(driverExecutorMetrics)
      }
      case ex: FileNotFoundException => {
        println(s"[SparkScope]${ex}, retrying in 5 secs...")
        Thread.sleep(5000)
        val driverExecutorMetrics = metricsLoader.load()
        analyze(driverExecutorMetrics)
      }
      case ex: Exception => {
        println(s"[SparkScope]${ex}, exiting...)")
      }
    }

  }
  def analyze(driverExecutorMetrics: DriverExecutorMetrics) = {
    val executorMetricsAnalyzer = new SparkScopeAnalyzer(sparkConf)
    val sparkScopeStart = System.currentTimeMillis()
    val sparkScopeResult = executorMetricsAnalyzer.analyze(driverExecutorMetrics, appContext)

    println("           ____              __    ____")
    println("          / __/__  ___ _____/ /__ / __/_ ___  ___  ___")
    println("         _\\ \\/ _ \\/ _ `/ __/  '_/_\\ \\/_ / _ \\/ _ \\/__/ ")
    println("        /___/ .__/\\_,_/_/ /_/\\_\\/___/\\__\\_,_/ .__/\\___/")
    println("           /_/                             /_/ ")
    println(sparkScopeResult.logs)

    val durationSparkScope = (System.currentTimeMillis() - sparkScopeStart) * 1f / 1000f
    println(s"\n[SparkScope] SparkScope analysis took ${durationSparkScope}s")

    val htmlReportDir = sparkConf.get("spark.sparkscope.html.path", "/tmp/")
    HtmlReportGenerator.generateHtml(sparkScopeResult, htmlReportDir, sparklensResults)
  }
}
