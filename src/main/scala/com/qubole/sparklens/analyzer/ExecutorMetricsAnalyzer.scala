
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
package com.qubole.sparklens.analyzer

import scala.collection.mutable
import com.qubole.sparklens.common.AppContext
import com.qubole.sparklens.helper.{CsvReader, DataFrame, PropertiesLoader}
import org.apache.spark.SparkConf

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

    val executorsMetricsMap: Map[Int, Seq[DataFrame]] = (0 until ac.executorMap.size).map { executorId =>
      val metricTables: Seq[DataFrame] = JvmMetrics.map { metric =>
        val metricsFilePath = s"${csvMetricsDir}/${appContext.appInfo.applicationID}.${executorId}.${metric}.csv"
        val csvFileStr = reader.read(metricsFilePath).replace("value", metric)
        out.println(s"[SparkScope] Reading ${metric} metric for executor=${executorId} from " + metricsFilePath)
        DataFrame.fromCsv(metric, csvFileStr, ",")
      }
      (executorId, metricTables)
    }.toMap

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

    // Series
    val clusterHeapUsed = allExecutorsMetrics.groupBySum("t", JvmHeapUsed)
    val clusterHeapMax = allExecutorsMetrics.groupBySum("t", JvmHeapMax)
    val clusterNonHeapUsed= allExecutorsMetrics.groupBySum("t", JvmNonHeapUsed)

    // Aggregations
    val maxHeapUsed = allExecutorsMetrics.columns.find(_.name == JvmHeapUsed).map(_.toInt.max/ (1024*1024)).getOrElse(0)
    val maxHeapUsagePerc = allExecutorsMetrics.columns.find(_.name == JvmHeapUsage).map(_.toFloat.max*100).getOrElse(0f).toDouble
    val maxNonHeapUsed = allExecutorsMetrics.columns.find(_.name == JvmNonHeapUsed).map(_.toInt.max / (1024*1024)).getOrElse(0)
    val avgHeapUsagePerc = allExecutorsMetrics.columns.find(_.name == JvmHeapUsage)
      .map(col => col.toFloat.sum*100/col.values.length).getOrElse(0f).toDouble

    out.println(clusterHeapUsed)
    out.println(clusterHeapMax)
    out.println(clusterNonHeapUsed)

    out.println(s"\n[SparkScope] Displaying summary:")
    out.println(f"Average Cluster heap memory utilization: ${avgHeapUsagePerc}%1.2f%%")
    out.println(f"Max heap memory utilization by single executor: ${maxHeapUsed}MB(${maxHeapUsagePerc}%1.2f%%)")
    out.println(s"Max non-heap memory utilization by single executor: ${maxNonHeapUsed}MB")

    out.toString
  }
}
