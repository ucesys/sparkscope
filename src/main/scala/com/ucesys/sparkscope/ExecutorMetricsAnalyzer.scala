
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

import com.qubole.sparklens.analyzer.AppAnalyzer
import com.qubole.sparklens.common.AppContext
import com.ucesys.sparkscope.io.{CsvHadoopReader, CsvReader, PropertiesLoader}
import org.apache.spark.SparkConf

import java.io.FileWriter
import java.time.LocalDateTime.ofEpochSecond
import java.time.ZoneOffset.UTC
import scala.collection.mutable

class ExecutorMetricsAnalyzer(sparkConf: SparkConf, reader: CsvReader, propertiesLoader: PropertiesLoader) extends  AppAnalyzer {

  val JvmHeapUsed = "jvm.heap.used" // in bytes
  val JvmHeapUsage= "jvm.heap.usage" // equals used/max
  val JvmHeapMax= "jvm.heap.max" // in bytes
  val JvmNonHeapUsed = "jvm.non-heap.used" // in bytes
  val JvmTotalUsed = "jvm.total.used"  // equals jvm.heap.used + jvm.non-heap.used
//  val JvmNonHeapUsage = "jvm.non-heap.usage" // equals used/max, gives negative number because non-heap.max equals -1
//  val JvmNonHeapMax= "jvm.non-heap.max" //  equals -1
//  val JvmTotalMax = "jvm.total.max" // equals jvm.heap.max

//  val JvmMetrics = Seq(JvmTotalUsed, JvmHeapUsed, JvmNonHeapUsed)
  val JvmMetrics = Seq(JvmHeapUsed, JvmHeapUsage, JvmHeapMax, JvmNonHeapUsed, JvmTotalUsed)

  def analyze(appContext: AppContext, startTime: Long, endTime: Long): String = {
    val startTimeMillis = System.currentTimeMillis()

    val ac = appContext.filterByStartAndEndTime(startTime, endTime)
    val out = new mutable.StringBuilder()

    out.println("           ____              __    ____")
    out.println("          / __/__  ___ _____/ /__ / __/_ ___  ___  ___")
    out.println("         _\\ \\/ _ \\/ _ `/ __/  '_/_\\ \\/_ / _ \\/ _ \\/__/ ")
    out.println("        /___/ .__/\\_,_/_/ /_/\\_\\/___/\\__\\_,_/ .__/\\___/")
    out.println("           /_/                             /_/ ")

    // TODO
    //    1. Find metrics.properties path:
    //      1.1 Check if spark conf contains spark.metrics.conf property
    //      1.2 Otherwise set to $SPARK_HOME/conf/metrics.properties
    //    2. Try to open metrics.properties
    //      2.1 If doesn't exist report warning
    //    3. Try to read CSV_DIR as *.sink.csv.directory or use default /tmp
    //    4. Try to read $CSV_DIR/<APP-ID>-METRIC.csv


    // 1
    sparkConf.getAll.foreach(println)
    val sparkHome = sparkConf.get("spark.home", sys.env.getOrElse("SPARK_HOME", sys.env("PWD")))
    out.println("[SparkScope] Spark home: " + sparkHome)
    val defaultMetricsPropsPath = sparkHome + "/conf/metrics.properties"
    val metricsPropertiesPath = sparkConf.get("spark.metrics.conf", defaultMetricsPropsPath)
    out.println("[SparkScope] Trying to read metrics.properties file from " + metricsPropertiesPath)

    // 2
    val prop = propertiesLoader.load(metricsPropertiesPath)

    // 3
    val csvMetricsDir = prop.getProperty("*.sink.csv.directory", "/tmp/")
    out.println("[SparkScope] Trying to read csv metrics from " + csvMetricsDir)

    // 4
    //    val metricNames = Seq("jvm.total.used", "jvm.total.max", "jvm.heap.usage", "jvm.heap.used", "jvm.non-heap.usage", "jvm.non-heap.used")

    out.println("[SparkScope] Reading driver metrics...")
    val driverMetrics: Seq[DataFrame] = JvmMetrics.map { metric =>
        val metricsFilePath = s"${csvMetricsDir}/${appContext.appInfo.applicationID}.driver.${metric}.csv"
        val csvFileStr = reader.read(metricsFilePath).replace("value", metric)
        out.println(s"[SparkScope] Reading ${metric} metric for driver from " + metricsFilePath)
        DataFrame.fromCsv(metric, csvFileStr, ",")
      }

    out.println("[SparkScope] Reading executor metrics...")
    val executorsMetricsMap: Map[Int, Seq[DataFrame]] = (0 until ac.executorMap.size).map { executorId =>
      val metricTables: Seq[DataFrame] = JvmMetrics.map { metric =>
        val metricsFilePath = s"${csvMetricsDir}/${appContext.appInfo.applicationID}.${executorId}.${metric}.csv"
        val csvFileStr = reader.read(metricsFilePath).replace("value", metric)
        out.println(s"[SparkScope] Reading ${metric} metric for executor=${executorId} from " + metricsFilePath)
        DataFrame.fromCsv(metric, csvFileStr, ",")
      }
      (executorId, metricTables)
    }.toMap

    driverMetrics.foreach { metric =>
      out.println(s"\n[SparkScope] Displaying ${metric.name} metric for driver:")
      out.println(metric)
    }

    var driverMetricsMerged: DataFrame = driverMetrics.head
    driverMetrics.tail.foreach { metric =>
      driverMetricsMerged = driverMetricsMerged.mergeOn("t", metric)
    }

    out.println(s"\n[SparkScope] Displaying merged metrics for driver:")
    out.println(driverMetricsMerged)

    executorsMetricsMap.foreach { case (executorId, metrics) =>
      metrics.foreach { metric =>
        out.println(s"\n[SparkScope] Displaying ${metric.name} metric for executor=${executorId}:")
        out.println(metric)
      }
    }

    val executorsMetricsCombinedMap: Map[Int, DataFrame] = executorsMetricsMap.map { case (executorId, metrics) =>
      var mergedMetrics = metrics.head
      metrics.tail.foreach { metric =>
        mergedMetrics = mergedMetrics.mergeOn("t", metric)
      }
      (executorId, mergedMetrics)
    }

    executorsMetricsCombinedMap.foreach { case (executorId, metrics) =>
      out.println(s"\n[SparkScope] Displaying merged metrics for executor=${executorId}:")
      out.println(metrics)
    }

    val executorsMetricsCombinedMapWithExecId: Map[Int, DataFrame] = executorsMetricsCombinedMap.map { case (executorId, metrics) =>
      val metricsWithExecId = metrics.addConstColumn("executorId", executorId.toString)
      (executorId, metricsWithExecId)
    }

    executorsMetricsCombinedMapWithExecId.foreach { case (executorId, metrics) =>
      out.println(s"\n[SparkScope] Displaying merged metrics with executorId for executor=${executorId}:")
      out.println(metrics)
    }

    var allExecutorsMetrics: DataFrame = executorsMetricsCombinedMapWithExecId.head match {
      case (_, b) => b
    }

    executorsMetricsCombinedMapWithExecId.tail.foreach { case (_, metrics) =>
      allExecutorsMetrics = allExecutorsMetrics.union(metrics)
    }
    out.println(s"\n[SparkScope] Displaying merged metrics for all executors:")
    out.println(allExecutorsMetrics)

    // Driver metrics
    val maxHeapUsedDriver = driverMetricsMerged.select(JvmHeapUsed).max.toLong/ (1024*1024)
    val maxHeapUsageDriverPerc = driverMetricsMerged.select(JvmHeapUsage).max  * 100
    val avgHeapUsedDriver = driverMetricsMerged.select(JvmHeapUsed).avg.toLong / (1024*1024)
    val avgHeapUsageDriverPerc = driverMetricsMerged.select(JvmHeapUsage).avg * 100
    val maxNonHeapUsedDriver = driverMetricsMerged.select(JvmNonHeapUsed).max.toLong / (1024*1024)
    val avgNonHeapUsedDriver = driverMetricsMerged.select(JvmNonHeapUsed).avg.toLong / (1024*1024)

    // Executor metrics Series
    val clusterHeapUsed = allExecutorsMetrics.groupBy("t", JvmHeapUsed).sum
    val clusterHeapMax = allExecutorsMetrics.groupBy("t", JvmHeapMax).sum
    val clusterNonHeapUsed = allExecutorsMetrics.groupBy("t", JvmNonHeapUsed).sum
    val clusterHeapUsage = allExecutorsMetrics.groupBy("t", JvmHeapUsage).avg

    val executorHeapUsedMax = allExecutorsMetrics.groupBy("t", JvmHeapUsed).max
    val executorHeapUsedMin = allExecutorsMetrics.groupBy("t", JvmHeapUsed).min
    val executorHeapUsedAvg = allExecutorsMetrics.groupBy("t", JvmHeapUsed).avg
    val executorHeapAllocation = allExecutorsMetrics.groupBy("t", JvmHeapMax).max

    out.println(clusterHeapUsed)
    out.println(clusterHeapMax)
    out.println(clusterNonHeapUsed)
    out.println(clusterHeapUsage)

    // Executor metrics Aggregations
    val maxHeapUsed = allExecutorsMetrics.select(JvmHeapUsed).max / (1024*1024)
    val maxHeapUsagePerc = allExecutorsMetrics.select(JvmHeapUsage).max * 100
    val maxNonHeapUsed = allExecutorsMetrics.select(JvmNonHeapUsed).max.toLong / (1024*1024)

    val avgHeapUsagePerc = allExecutorsMetrics.select(JvmHeapUsage).avg * 100
    val avgHeapUsed = allExecutorsMetrics.select(JvmHeapUsed).avg.toLong / (1024*1024)
    val avgNonHeapUsed = allExecutorsMetrics.select(JvmNonHeapUsed).avg.toLong / (1024*1024)

    val maxClusterHeapUsed = clusterHeapUsed.select(JvmHeapUsed).max.toLong / (1024*1024)
    val maxClusterHeapUsagePerc = clusterHeapUsage.select(JvmHeapUsage).max * 100

    val avgClusterHeapUsed = clusterHeapUsed.select(JvmHeapUsed).avg.toLong / (1024*1024)

    out.println(s"\n[SparkScope] Cluster stats:")
    out.println(f"Average Cluster heap memory utilization: ${avgHeapUsagePerc}%1.2f%% / ${avgClusterHeapUsed}MB")
    out.println(f"Max Cluster heap memory utilization: ${maxClusterHeapUsagePerc}%1.2f%% / ${maxClusterHeapUsed}MB")

    out.println(s"\n[SparkScope] Executor stats:")
    out.println(f"Max heap memory utilization by executor: ${maxHeapUsed}MB(${maxHeapUsagePerc}%1.2f%%)")
    out.println(f"Average heap memory utilization by executor: ${avgHeapUsed}MB(${avgHeapUsagePerc}%1.2f%%)")
    out.println(s"Max non-heap memory utilization by executor: ${maxNonHeapUsed}MB")
    out.println(f"Average non-heap memory utilization by executor: ${avgNonHeapUsed}MB")

    out.println(s"\n[SparkScope] Driver stats:")
    out.println(f"Max heap memory utilization by driver: ${maxHeapUsedDriver}MB(${maxHeapUsageDriverPerc}%1.2f%%)")
    out.println(f"Average heap memory utilization by driver: ${avgHeapUsedDriver}MB(${avgHeapUsageDriverPerc}%1.2f%%)")
    out.println(s"Max non-heap memory utilization by driver: ${maxNonHeapUsedDriver}MB")
    out.println(f"Average non-heap memory utilization by driver: ${avgNonHeapUsedDriver}MB")

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis)*1f / 1000f
    out.println(s"\n[SparkScope] SparkScope analysis took ${durationSeconds}s")

    val html = out.toString.replace("\n", "<br>\n")
    val fileWriter = new FileWriter("report.html")
    val template = scala.io.Source.fromFile("report-template.html").mkString

    val rendered = template
      .replace("<APP-ID>", ac.appInfo.applicationID)
      .replace("<REPORT-BODY>", html)
      .replace("${chart.jvm.cluster.heap.current}", clusterHeapUsed.select("jvm.heap.used").div(1024*1024).mkString(","))
      .replace("${chart.jvm.cluster.heap.max}", clusterHeapMax.select("jvm.heap.max").div(1024*1024).mkString(","))
      .replace(
        "${chart.jvm.cluster.heap.timestamps}",
        clusterHeapMax.select("t").values.map(ts => s"'${ofEpochSecond(ts.toLong, 0, UTC)}'").mkString(",")
      )
      .replace(
      "${chart.jvm.executor.heap.timestamps}",
        clusterHeapMax.select("t").values.map(ts => s"'${ofEpochSecond(ts.toLong, 0, UTC)}'").mkString(",")
      )
      .replace("${chart.jvm.executor.heap.max}", executorHeapUsedMax.select("jvm.heap.used").div(1024*1024).mkString(","))
      .replace("${chart.jvm.executor.heap.min}", executorHeapUsedMin.select("jvm.heap.used").div(1024*1024).mkString(","))
      .replace("${chart.jvm.executor.heap.avg}", executorHeapUsedAvg.select("jvm.heap.used").div(1024*1024).mkString(","))
      .replace("${chart.jvm.executor.heap.allocation}", executorHeapAllocation.select("jvm.heap.max").div(1024*1024).mkString(","))

    fileWriter.write(rendered)
    fileWriter.close()

    out.toString
  }
}
