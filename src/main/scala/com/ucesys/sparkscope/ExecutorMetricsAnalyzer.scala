
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

import com.qubole.sparklens.common.AppContext.getExecutorCores
import com.qubole.sparklens.common.{AppContext, ApplicationInfo}
import com.ucesys.sparkscope.ExecutorMetricsAnalyzer._
import com.ucesys.sparkscope.io.{CsvReader, PropertiesLoader}
import com.ucesys.sparkscope.metrics.{ClusterMetrics, ClusterStats, DriverStats, ExecutorStats, ResourceWasteMetrics}
import org.apache.spark.SparkConf

import scala.collection.mutable
import scala.concurrent.duration._

case class SparkScopeResult(appInfo: ApplicationInfo,
                            executorMetrics: ExecutorMetrics,
                            driverMetrics: DataFrame,
                            clusterMetrics: ClusterMetrics,
                            resourceWasteMetrics: ResourceWasteMetrics,
                            stats: Statistics,
                            summary: String,
                            sparkConf: SparkConf,
                            logs: String)
case class ExecutorMetrics(heapUsedMax: DataFrame,
                           heapUsedMin: DataFrame,
                           heapUsedAvg: DataFrame,
                           heapAllocation: DataFrame,
                           nonHeapUsedMax: DataFrame,
                           nonHeapUsedMin: DataFrame,
                           nonHeapUsedAvg: DataFrame)
case class Statistics(clusterStats: ClusterStats, executorStats: ExecutorStats, driverStats: DriverStats)

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
    val driverMetrics: Seq[DataFrame] = DriverCsvMetrics.map { metric =>
        val metricsFilePath = s"${csvMetricsDir}/${appContext.appInfo.applicationID}.driver.${metric}.csv"
        val csvFileStr = reader.read(metricsFilePath).replace("value", metric)
        log.println(s"[SparkScope] Reading ${metric} metric for driver from " + metricsFilePath)
        DataFrame.fromCsv(metric, csvFileStr, ",")
      }

    log.println("[SparkScope] Reading executor metrics...")
    val executorsMetricsMap: Map[Int, Seq[DataFrame]] = (0 until ac.executorMap.size).map { executorId =>
      val metricTables: Seq[DataFrame] = ExecutorCsvMetrics.map { metric =>
        val metricsFilePath = s"${csvMetricsDir}/${appContext.appInfo.applicationID}.${executorId}.${metric}.csv"
        val csvFileStr = reader.read(metricsFilePath).replace("value", metric).replace("count", metric)
        log.println(s"[SparkScope] Reading ${metric} metric for executor=${executorId} from " + metricsFilePath)
        DataFrame.fromCsv(metric, csvFileStr, ",").distinct("t").sortBy("t")
      }
      (executorId, metricTables)
    }.toMap

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

    val startEndTimes = ac.executorMap.map { case (id, timespan) => {
      val end = timespan.endTime match {
        case 0 => ac.appInfo.endTime
        case _ => timespan.endTime
      }
      (id, (timespan.startTime, end, end - timespan.startTime))
    }
    }.toMap
    log.println("[SparkScope] Displaying timelines for executors")

    startEndTimes.toSeq.foreach { case (id, (start, end, duration)) =>
      log.println(s"executorId: ${id}, start: ${start}, end: ${end}, duration: ${duration},")
    }
    val combinedExecutorUptime = startEndTimes.map { case (id, (start, end, duration)) => duration }.sum
    log.println(s"combinedExecutorUptime: ${combinedExecutorUptime}")

    val executorsMetricsMapInterpolated = interpolateExecutorMetrics(executorsMetricsMap)

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
    allExecutorsMetrics = allExecutorsMetrics.addColumn(allExecutorsMetrics.select("t").tsToDt)
    log.println(s"\n[SparkScope] Displaying merged metrics for all executors:")
    log.println(allExecutorsMetrics)

    // Driver metrics
    val maxHeapUsedDriver = driverMetricsMerged.select(JvmHeapUsed).max.toLong / BytesInMB
    val maxHeapUsageDriverPerc = driverMetricsMerged.select(JvmHeapUsage).max  * 100
    val avgHeapUsedDriver = driverMetricsMerged.select(JvmHeapUsed).avg.toLong / BytesInMB
    val avgHeapUsageDriverPerc = driverMetricsMerged.select(JvmHeapUsage).avg * 100
    val maxNonHeapUsedDriver = driverMetricsMerged.select(JvmNonHeapUsed).max.toLong / BytesInMB
    val avgNonHeapUsedDriver = driverMetricsMerged.select(JvmNonHeapUsed).avg.toLong / BytesInMB

    // Executor metrics
    val executorMetrics = ExecutorMetrics(
      heapUsedMax = allExecutorsMetrics.groupBy("t", JvmHeapUsed).max.sortBy("t"),
      heapUsedMin = allExecutorsMetrics.groupBy("t", JvmHeapUsed).min.sortBy("t"),
      heapUsedAvg = allExecutorsMetrics.groupBy("t", JvmHeapUsed).avg.sortBy("t"),
      heapAllocation = allExecutorsMetrics.groupBy("t", JvmHeapMax).max.sortBy("t"),
      nonHeapUsedMax = allExecutorsMetrics.groupBy("t", JvmNonHeapUsed).max.sortBy("t"),
      nonHeapUsedMin = allExecutorsMetrics.groupBy("t", JvmNonHeapUsed).min.sortBy("t"),
      nonHeapUsedAvg = allExecutorsMetrics.groupBy("t", JvmNonHeapUsed).avg.sortBy("t")
    )

    // Cluster metrics Series
    val clusterHeapUsed = allExecutorsMetrics.groupBy("t", JvmHeapUsed).sum.sortBy("t")
    val clusterHeapMax = allExecutorsMetrics.groupBy("t", JvmHeapMax).sum.sortBy("t")
    val clusterHeapUsage = allExecutorsMetrics.groupBy("t", JvmHeapUsage).avg.sortBy("t")
    val clusterCpuTime = allExecutorsMetrics.groupBy("t", CpuTime).sum.sortBy("t")

    val executorCores = getExecutorCores(ac)
    val clusterUsageDf = calculateClusterCpuUsage(allExecutorsMetrics, executorCores)
    val totalCpuUtil: Double = clusterCpuTime.select(CpuTime).max / (combinedExecutorUptime * executorCores * MilliSecondsInSec)
    log.println(clusterUsageDf)

    val clusterMetrics = ClusterMetrics(heapMax = clusterHeapMax, heapUsed = clusterHeapUsed, heapUsage = clusterHeapUsage, clusterUsageDf)
    log.println(clusterMetrics)

    // Executor metrics Aggregations
    val maxHeapUsed = allExecutorsMetrics.select(JvmHeapUsed).max.toLong / BytesInMB
    val maxHeapUsagePerc = allExecutorsMetrics.select(JvmHeapUsage).max * 100
    val maxNonHeapUsed = allExecutorsMetrics.select(JvmNonHeapUsed).max.toLong / BytesInMB

    val avgHeapUsagePerc = allExecutorsMetrics.select(JvmHeapUsage).avg
    val avgHeapUsed = allExecutorsMetrics.select(JvmHeapUsed).avg.toLong / BytesInMB
    val avgNonHeapUsed = allExecutorsMetrics.select(JvmNonHeapUsed).avg.toLong / BytesInMB

    val maxClusterHeapUsed = clusterHeapUsed.select(JvmHeapUsed).max.toLong / BytesInMB
    val maxClusterHeapUsagePerc = clusterHeapUsage.select(JvmHeapUsage).max * 100

    val avgClusterHeapUsed = clusterHeapUsed.select(JvmHeapUsed).avg.toLong / BytesInMB


    val clusterStats = ClusterStats(
        maxHeap = maxClusterHeapUsed,
        maxHeapPerc = maxClusterHeapUsagePerc,
        avgHeap = avgClusterHeapUsed,
        avgHeapPerc = avgHeapUsagePerc,
        totalCpuUtil = totalCpuUtil
      )
    log.println(clusterStats)

    val executorStats = ExecutorStats(
      maxHeap = maxHeapUsed,
      maxHeapPerc = maxHeapUsagePerc,
      maxNonHeap = maxNonHeapUsed,
      avgHeap = avgHeapUsed,
      avgHeapPerc = avgHeapUsagePerc,
      avgNonHeap = avgNonHeapUsed
    )
    log.println(executorStats)

    val driverStats = DriverStats(
      maxHeap = maxHeapUsedDriver,
      maxHeapPerc = maxHeapUsageDriverPerc,
      maxNonHeap = maxNonHeapUsedDriver,
      avgHeap = avgHeapUsedDriver,
      avgHeapPerc = avgHeapUsageDriverPerc,
      avgNonHeap = avgNonHeapUsedDriver
    )
    log.println(driverStats)

    val endTimeMillis = System.currentTimeMillis()
    val durationSeconds = (endTimeMillis - startTimeMillis) * 1f / 1000f
    log.println(s"\n[SparkScope] SparkScope analysis took ${durationSeconds}s")

    val executorHeapMemoryInGb = executorMetrics.heapAllocation.select(JvmHeapMax).toDouble.head / BytesInGB
    val combinedExecutorUptimeSecs = combinedExecutorUptime.milliseconds.toSeconds

    val resourceWasteMetrics = ResourceWasteMetrics(
      executorCores = executorCores,
      combinedExecutorUptimeSecs = combinedExecutorUptimeSecs,
      cpuUtil = totalCpuUtil,
      heapUtil = avgHeapUsagePerc,
      executorHeapSizeInGb = executorHeapMemoryInGb
    )
    log.println(resourceWasteMetrics)

    SparkScopeResult(
      appInfo = ac.appInfo,
      executorMetrics = executorMetrics,
      clusterMetrics = clusterMetrics,
      summary = summary.toString,
      driverMetrics=driverMetricsMerged,
      logs=log.toString,
      sparkConf = sparkConf,
      resourceWasteMetrics = resourceWasteMetrics,
      stats = Statistics(clusterStats = clusterStats, executorStats = executorStats, driverStats = driverStats)
    )
  }

  private def calculateClusterCpuUsage(allExecutorsMetrics: DataFrame, executorCores: Int): DataFrame = {
    val clusterCpuTimeDf = allExecutorsMetrics.groupBy("t", CpuTime).sum.sortBy("t")
    val clusterCpuTime = clusterCpuTimeDf.select(CpuTime).div(NanoSecondsInSec).rename("cpuTime")
    val clusterCpuTimeLag = clusterCpuTime.lag
    val clusterCpuTimeDiff = clusterCpuTime.sub(clusterCpuTimeLag)
    val timeLag = clusterCpuTimeDf.select("t").lag
    val timeElapsed = clusterCpuTimeDf.select("t").sub(timeLag)
    val numExecutors = allExecutorsMetrics.groupBy("t", CpuTime).count.sortBy("t")
    val combinedTimeElapsed = timeElapsed.mul(numExecutors.select("cnt")).mul(executorCores)
    val clusterUsage = clusterCpuTimeDiff.div(combinedTimeElapsed)

    val clusterUsageDf = DataFrame(
      "cpuUsage",
      Seq(
        clusterCpuTimeDf.select("t"),
        timeElapsed.rename("dt"),
        combinedTimeElapsed.rename("dt*executorCnt*coresPerExec"),
        clusterCpuTime,
        clusterCpuTimeDiff.rename("cpuTimeDiff"),
        numExecutors.select("cnt").rename("executorCnt"),
        clusterUsage.rename("cpuUsage")
      ))
    clusterUsageDf
  }
  private def interpolateExecutorMetrics(executorsMetricsMap: Map[Int, Seq[DataFrame]]): Map[Int, Seq[DataFrame]] = {
/*    Interpolating executor metrics to align their timestamps for aggregations

            RAW                      INTERPOLATED
      _______________               _______________
      Executor 1:                   Executor 1:
      t,jvm.heap.used               t,jvm.heap.used
      1695358650,100                1695358650,100
      1695358660,200                1695358655,150
                                    1695358656,200
                            =>
      Executor 2:                   Executor 2:
      t,jvm.heap.used               t,jvm.heap.used
      1695358655,300                1695358655,300
      1695358665,200                1695358660,250
                                    1695358665,200
*/

    val allTimestamps = executorsMetricsMap.flatMap { case (_, metrics) =>
      metrics.flatMap(metric => metric.select("t").values.map(_.toLong))
    }.toSet.toSeq.sorted

    executorsMetricsMap.map { case (executorId, metricsSeq) =>
      val metricsInterpolated: Seq[DataFrame] = metricsSeq.map(metrics => {
        val localTimestamps = metrics.columns.head.values.map(_.toLong)
        val missingTimestamps = allTimestamps.filterNot(localTimestamps.contains)
        val missingTimestampsInRange = missingTimestamps.filter(ts => ts > localTimestamps.min && ts < localTimestamps.max)

        val metricsZipped = (metrics.columns.head.values.map(_.toLong) zip metrics.columns.last.values)

        val interpolatedRows: Seq[(Long, String)] = missingTimestampsInRange.flatMap(missingTimestamp => {
          val interpolatedValue = metricsZipped.sliding(2)
            .flatMap { case Seq((prevTs, prevVal), (nextTs, nextVal)) =>
              val interpolatedValue: Option[(Long, String)] = missingTimestamp match {
                case ts if (ts > prevTs && ts < nextTs) => {
                  val diffPrevToNext = nextTs - prevTs
                  val diffToPrev = ts - prevTs
                  val diffToNext = nextTs - ts
                  val interpolated = (prevVal.toDouble * (diffPrevToNext - diffToPrev) + nextVal.toDouble * (diffPrevToNext - diffToNext)) / diffPrevToNext
                  Some((ts, interpolated.toString))
                }
                case _ => None
              }
              interpolatedValue
            }.toSeq.headOption
          interpolatedValue
        })

        val missingTsDF = DataFrame("missingTs",
          Seq(
            DataColumn(metrics.columns.head.name, interpolatedRows.map { case (ts, _) => ts.toString }),
            DataColumn(metrics.columns.last.name, interpolatedRows.map { case (_, value) => value })
          ))

        val metricsWithNewTs = metrics.union(missingTsDF).sortBy("t")
        metricsWithNewTs
      })
      (executorId, metricsInterpolated)
    }
  }

  implicit class StringBuilderExtensions(sb: StringBuilder) {
    def println(x: Any): StringBuilder = {
      sb.append(x).append("\n")
    }
  }
}

object ExecutorMetricsAnalyzer {
  val BytesInMB: Long = 1024L*1024L
  private val BytesInGB: Long = 1024L*1024L*1024L

  private val NanoSecondsInSec: Long = 1000000000
  private val MilliSecondsInSec: Long = 1000000

  private val JvmHeapUsed = "jvm.heap.used" // in bytes
  private val JvmHeapUsage = "jvm.heap.usage" // equals used/max
  private val JvmHeapMax = "jvm.heap.max" // in bytes
  private val JvmNonHeapUsed = "jvm.non-heap.used" // in bytes
  private val CpuTime = "executor.cpuTime" // CPU time computing tasks in nanoseconds

  private val ExecutorCsvMetrics = Seq(JvmHeapUsed, JvmHeapUsage, JvmHeapMax, JvmNonHeapUsed, CpuTime)
  private val DriverCsvMetrics = Seq(JvmHeapUsed, JvmHeapUsage, JvmHeapMax, JvmNonHeapUsed)
}
