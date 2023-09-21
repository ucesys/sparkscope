
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
import com.ucesys.sparkscope.ExecutorMetricsAnalyzer._
import com.ucesys.sparkscope.io.{CsvHadoopReader, CsvReader, PropertiesLoader}
import org.apache.spark.SparkConf

import scala.collection.mutable
case class SparkScopeResult(applicationId: String,
                            executorMetrics: ExecutorMetrics,
                            clusterMetrics: ClusterMetrics,
                            summary: String,
                            logs: String)
case class ExecutorMetrics(heapUsedMax: DataFrame,
                           heapUsedMin: DataFrame,
                           heapUsedAvg: DataFrame,
                           heapAllocation: DataFrame)
case class ClusterMetrics(heapMax: DataFrame,
                          heapUsed: DataFrame,
                          heapUsage: DataFrame)
class ExecutorMetricsAnalyzer(sparkConf: SparkConf, reader: CsvReader, propertiesLoader: PropertiesLoader) {

  def analyze(appContext: AppContext): SparkScopeResult = {
    val startTimeMillis = System.currentTimeMillis()

    val ac = appContext.filterByStartAndEndTime(appContext.appInfo.startTime, appContext.appInfo.endTime)
    val log = new mutable.StringBuilder()
    val summary = new mutable.StringBuilder()

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
    log.println("[SparkScope] Spark home: " + sparkHome)
    val defaultMetricsPropsPath = sparkHome + "/conf/metrics.properties"
    val metricsPropertiesPath = sparkConf.get("spark.metrics.conf", defaultMetricsPropsPath)
    log.println("[SparkScope] Trying to read metrics.properties file from " + metricsPropertiesPath)

    // 2
    val prop = propertiesLoader.load(metricsPropertiesPath)

    // 3
    val csvMetricsDir = prop.getProperty("*.sink.csv.directory", "/tmp/")
    log.println("[SparkScope] Trying to read csv metrics from " + csvMetricsDir)

    // 4
    log.println("[SparkScope] Reading driver metrics...")
    val driverMetrics: Seq[DataFrame] = JvmMetrics.map { metric =>
        val metricsFilePath = s"${csvMetricsDir}/${appContext.appInfo.applicationID}.driver.${metric}.csv"
        val csvFileStr = reader.read(metricsFilePath).replace("value", metric)
        log.println(s"[SparkScope] Reading ${metric} metric for driver from " + metricsFilePath)
        DataFrame.fromCsv(metric, csvFileStr, ",")
      }

    log.println("[SparkScope] Reading executor metrics...")
    val executorsMetricsMap: Map[Int, Seq[DataFrame]] = (0 until ac.executorMap.size).map { executorId =>
      val metricTables: Seq[DataFrame] = JvmMetrics.map { metric =>
        val metricsFilePath = s"${csvMetricsDir}/${appContext.appInfo.applicationID}.${executorId}.${metric}.csv"
        val csvFileStr = reader.read(metricsFilePath).replace("value", metric)
        log.println(s"[SparkScope] Reading ${metric} metric for executor=${executorId} from " + metricsFilePath)
        DataFrame.fromCsv(metric, csvFileStr, ",").distinct("t").sortBy("t")
      }
      (executorId, metricTables)
    }.toMap

    driverMetrics.foreach { metric =>
      log.println(s"\n[SparkScope] Displaying ${metric.name} metric for driver:")
      log.println(metric)
    }

    var driverMetricsMerged: DataFrame = driverMetrics.head
    driverMetrics.tail.foreach { metric =>
      driverMetricsMerged = driverMetricsMerged.mergeOn("t", metric)
    }

    log.println(s"\n[SparkScope] Displaying merged metrics for driver:")
    log.println(driverMetricsMerged)

    executorsMetricsMap.foreach { case (executorId, metrics) =>
      metrics.foreach { metric =>
        log.println(s"\n[SparkScope] Displaying ${metric.name} metric for executor=${executorId}:")
        log.println(metric)
      }
    }

    // Interpolating executor metrics
    val allTimestamps = executorsMetricsMap.flatMap { case (_, metrics) =>
      metrics.flatMap(metric => metric.select("t").values.map(_.toLong))
    }.toSet.toSeq.sorted
    log.println(s"\n[SparkScope] Displaying timestamps for all executors:")
    log.println(allTimestamps)

    val executorsMetricsMapInterpolated = executorsMetricsMap.map { case (executorId, metricsSeq) =>
      val metricsInterpolated: Seq[DataFrame] = metricsSeq.map(metrics => {
        val localTimestamps = metrics.columns.head.values.map(_.toLong)
        val missingTimestamps = allTimestamps.filterNot(localTimestamps.contains)
        val missingTimestampsInRange = missingTimestamps.filter(ts => ts > localTimestamps.min && ts < localTimestamps.max)

        val metricsZipped =  (metrics.columns.head.values.map(_.toLong) zip metrics.columns.last.values)

        val interpolatedRows: Seq[(Long, String)] = missingTimestampsInRange.flatMap(missingTimestamp => {
          val interpolatedValue = metricsZipped.sliding(2)
            .flatMap { case Seq((prevTs, prevVal), (nextTs, nextVal)) =>
              val interpolatedValue: Option[(Long, String)] = missingTimestamp match {
                case ts if (ts > prevTs && ts < nextTs) => Some((ts, prevVal))
                case _ => None
              }
              interpolatedValue
            }.toSeq.headOption
          interpolatedValue
        })

        val missingTsDF = DataFrame("missingTs",
          Seq(
            DataColumn(metrics.columns.head.name, interpolatedRows.map{case (ts, _) => ts.toString}),
            DataColumn(metrics.columns.last.name, interpolatedRows.map{case (_, value) => value})
          ))

        val metricsWithNewTs = metrics.union(missingTsDF).sortBy("t")
        metricsWithNewTs
      })
      (executorId, metricsInterpolated)
    }

    executorsMetricsMapInterpolated.foreach { case (executorId, metrics) =>
      metrics.foreach { metric =>
        log.println(s"\n[SparkScope] Displaying interpolated ${metric.name} metric for executor=${executorId}:")
        log.println(metric)
      }
    }

    val executorsMetricsCombinedMap: Map[Int, DataFrame] = executorsMetricsMapInterpolated.map { case (executorId, metrics) =>
      var mergedMetrics = metrics.head
      metrics.tail.foreach { metric =>
        mergedMetrics = mergedMetrics.mergeOn("t", metric)
      }
      (executorId, mergedMetrics)
    }

    executorsMetricsCombinedMap.foreach { case (executorId, metrics) =>
      log.println(s"\n[SparkScope] Displaying merged metrics for executor=${executorId}:")
      log.println(metrics)
    }

    val executorsMetricsCombinedMapWithExecId: Map[Int, DataFrame] = executorsMetricsCombinedMap.map { case (executorId, metrics) =>
      val metricsWithExecId = metrics.addConstColumn("executorId", executorId.toString)
      (executorId, metricsWithExecId)
    }

    executorsMetricsCombinedMapWithExecId.foreach { case (executorId, metrics) =>
      log.println(s"\n[SparkScope] Displaying merged metrics with executorId for executor=${executorId}:")
      log.println(metrics)
    }

    var allExecutorsMetrics: DataFrame = executorsMetricsCombinedMapWithExecId.head match {
      case (_, b) => b
    }

    executorsMetricsCombinedMapWithExecId.tail.foreach { case (_, metrics) =>
      allExecutorsMetrics = allExecutorsMetrics.union(metrics)
    }
    log.println(s"\n[SparkScope] Displaying merged metrics for all executors:")
    log.println(allExecutorsMetrics)

    // Driver metrics
    val maxHeapUsedDriver = driverMetricsMerged.select(JvmHeapUsed).max.toLong / BytesInMB
    val maxHeapUsageDriverPerc = driverMetricsMerged.select(JvmHeapUsage).max  * 100
    val avgHeapUsedDriver = driverMetricsMerged.select(JvmHeapUsed).avg.toLong / BytesInMB
    val avgHeapUsageDriverPerc = driverMetricsMerged.select(JvmHeapUsage).avg * 100
    val maxNonHeapUsedDriver = driverMetricsMerged.select(JvmNonHeapUsed).max.toLong / BytesInMB
    val avgNonHeapUsedDriver = driverMetricsMerged.select(JvmNonHeapUsed).avg.toLong / BytesInMB

    // Executor metrics Series
    val clusterHeapUsed = allExecutorsMetrics.groupBy("t", JvmHeapUsed).sum.sortBy("t")
    val clusterHeapMax = allExecutorsMetrics.groupBy("t", JvmHeapMax).sum.sortBy("t")
    val clusterNonHeapUsed = allExecutorsMetrics.groupBy("t", JvmNonHeapUsed).sum.sortBy("t")
    val clusterHeapUsage = allExecutorsMetrics.groupBy("t", JvmHeapUsage).avg.sortBy("t")

    val executorMetrics = ExecutorMetrics(
      heapUsedMax = allExecutorsMetrics.groupBy("t", JvmHeapUsed).max.sortBy("t"),
      heapUsedMin = allExecutorsMetrics.groupBy("t", JvmHeapUsed).min.sortBy("t"),
      heapUsedAvg = allExecutorsMetrics.groupBy("t", JvmHeapUsed).avg.sortBy("t"),
      heapAllocation = allExecutorsMetrics.groupBy("t", JvmHeapMax).max.sortBy("t")
    )
    val clusterMetrics = ClusterMetrics(heapMax = clusterHeapMax, heapUsed = clusterHeapUsed, heapUsage = clusterHeapUsage)

    log.println(s"\n[SparkScope] Displaying cluster metrics(aggregated for all executors)")
    log.println(clusterHeapUsed)
    log.println(clusterHeapMax)
    log.println(clusterNonHeapUsed)
    log.println(clusterHeapUsage)

    // Executor metrics Aggregations
    val maxHeapUsed = allExecutorsMetrics.select(JvmHeapUsed).max.toLong / BytesInMB
    val maxHeapUsagePerc = allExecutorsMetrics.select(JvmHeapUsage).max * 100
    val maxNonHeapUsed = allExecutorsMetrics.select(JvmNonHeapUsed).max.toLong / BytesInMB

    val avgHeapUsagePerc = allExecutorsMetrics.select(JvmHeapUsage).avg * 100
    val avgHeapUsed = allExecutorsMetrics.select(JvmHeapUsed).avg.toLong / BytesInMB
    val avgNonHeapUsed = allExecutorsMetrics.select(JvmNonHeapUsed).avg.toLong / BytesInMB

    val maxClusterHeapUsed = clusterHeapUsed.select(JvmHeapUsed).max.toLong / BytesInMB
    val maxClusterHeapUsagePerc = clusterHeapUsage.select(JvmHeapUsage).max * 100

    val avgClusterHeapUsed = clusterHeapUsed.select(JvmHeapUsed).avg.toLong / BytesInMB

    summary.println(s"Cluster stats:")
    summary.println(f"Average Cluster heap memory utilization: ${avgHeapUsagePerc}%1.2f%% / ${avgClusterHeapUsed}MB")
    summary.println(f"Max Cluster heap memory utilization: ${maxClusterHeapUsagePerc}%1.2f%% / ${maxClusterHeapUsed}MB")

    summary.println(s"\nExecutor stats:")
    summary.println(f"Max heap memory utilization by executor: ${maxHeapUsed}MB(${maxHeapUsagePerc}%1.2f%%)")
    summary.println(f"Average heap memory utilization by executor: ${avgHeapUsed}MB(${avgHeapUsagePerc}%1.2f%%)")
    summary.println(s"Max non-heap memory utilization by executor: ${maxNonHeapUsed}MB")
    summary.println(f"Average non-heap memory utilization by executor: ${avgNonHeapUsed}MB")

    summary.println(s"\nDriver stats:")
    summary.println(f"Max heap memory utilization by driver: ${maxHeapUsedDriver}MB(${maxHeapUsageDriverPerc}%1.2f%%)")
    summary.println(f"Average heap memory utilization by driver: ${avgHeapUsedDriver}MB(${avgHeapUsageDriverPerc}%1.2f%%)")
    summary.println(s"Max non-heap memory utilization by driver: ${maxNonHeapUsedDriver}MB")
    summary.println(f"Average non-heap memory utilization by driver: ${avgNonHeapUsedDriver}MB")

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) * 1f / 1000f
    log.println(s"\n[SparkScope] SparkScope analysis took ${durationSeconds}s")

    SparkScopeResult(
      applicationId = ac.appInfo.applicationID,
      executorMetrics = executorMetrics,
      clusterMetrics = clusterMetrics,
      summary = summary.toString,
      logs=log.toString
    )
  }

  implicit class StringBuilderExtensions(sb: StringBuilder) {
    def println(x: Any): StringBuilder = {
      sb.append(x).append("\n")
    }
  }
}

object ExecutorMetricsAnalyzer {
  val BytesInMB: Long = 1024*1024

  val JvmHeapUsed = "jvm.heap.used" // in bytes
  val JvmHeapUsage = "jvm.heap.usage" // equals used/max
  val JvmHeapMax = "jvm.heap.max" // in bytes
  val JvmNonHeapUsed = "jvm.non-heap.used" // in bytes
  val JvmTotalUsed = "jvm.total.used" // equals jvm.heap.used + jvm.non-heap.used

  val JvmMetrics = Seq(JvmHeapUsed, JvmHeapUsage, JvmHeapMax, JvmNonHeapUsed, JvmTotalUsed)

}
