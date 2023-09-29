
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

import com.ucesys.sparklens.common.AppContext.getExecutorCores
import com.ucesys.sparklens.common.AppContext
import com.ucesys.sparklens.timespan.ExecutorTimeSpan
import com.ucesys.sparkscope.SparkScopeAnalyzer._
import com.ucesys.sparkscope.data.{DataColumn, DataFrame}
import com.ucesys.sparkscope.io.{DriverExecutorMetrics, MetricsLoader}
import com.ucesys.sparkscope.metrics._
import com.ucesys.sparkscope.utils.Logger
import com.ucesys.sparkscope.warning.{CPUUtilWarning, HeapUtilWarning, MissingMetricsWarning, Warning}

import scala.collection.mutable
import scala.concurrent.duration._

class SparkScopeAnalyzer {

  def analyze(driverExecutorMetrics: DriverExecutorMetrics, appContext: AppContext): SparkScopeResult = {
    val ac = appContext.filterByStartAndEndTime(appContext.appInfo.startTime, appContext.appInfo.endTime)
    val log = new Logger

    var driverMetricsMerged: DataFrame = driverExecutorMetrics.driverMetrics.head
    driverExecutorMetrics.driverMetrics.tail.foreach { metric =>
      driverMetricsMerged = driverMetricsMerged.mergeOn("t", metric)
    }
    log.println(s"\nDisplaying merged metrics for driver:\n${driverMetricsMerged}")

    driverExecutorMetrics.executorMetricsMap.foreach { case (id, metrics) =>
      metrics.foreach { metric => log.println(s"\nDisplaying ${metric.name} metric for executor=${id}:\n${metric}")
      }
    }

    // Interpolating metrics
    val executorsMetricsMapInterpolated = interpolateExecutorMetrics(ac.executorMap, driverExecutorMetrics.executorMetricsMap)
    executorsMetricsMapInterpolated.foreach { case (id, metrics) =>
      metrics.foreach { metric => log.println(s"\nInterpolated ${metric.name} metric for executor=${id}:\n${metric}")}
    }

    // For each executor, merge it's respective metrics into a single DataFrame(one for each executor)
    val executorsMetricsCombinedMap: Map[Int, DataFrame] = executorsMetricsMapInterpolated.map { case (executorId, metrics) =>
      var mergedMetrics = metrics.head
      metrics.tail.foreach(metric => mergedMetrics = mergedMetrics.mergeOn("t", metric))
      (executorId, mergedMetrics)
    }
    executorsMetricsCombinedMap.foreach { case (id, m) => log.println(s"\nMerged metrics for executor=${id}:\n${m}")}

    // Add cpu usage for each executor
    val executorsMetricsCombinedMapWithCpuUsage: Map[Int, DataFrame] = executorsMetricsCombinedMap.map { case (id, metrics) =>
      val clusterCpuTime = metrics.select(CpuTime).div(NanoSecondsInSec)
      val clusterCpuTimeLag = clusterCpuTime.lag
      val clusterCpuTimeDiff = clusterCpuTime.sub(clusterCpuTimeLag)
      val timeLag = metrics.select("t").lag
      val timeElapsed = metrics.select("t").sub(timeLag)
      val cpuUsageAllCores = clusterCpuTimeDiff.div(timeElapsed).rename("cpuUsageAllCores")
      val cpuUsage = cpuUsageAllCores.div(getExecutorCores(ac)).rename(CpuUsage)
      val metricsWithCpuUsage = metrics.addColumn(cpuUsage).addColumn(cpuUsageAllCores)
      (id, metricsWithCpuUsage)
    }

    // Add executorId column for later union
    val executorsMetricsCombinedMapWithExecId: Map[Int, DataFrame] = executorsMetricsCombinedMapWithCpuUsage.map { case (id, metrics) =>
      val metricsWithExecId = metrics.addConstColumn("executorId", id.toString)
      (id, metricsWithExecId)
    }

    executorsMetricsCombinedMapWithExecId.foreach { case (id, metrics) =>
      log.println(s"\nDisplaying merged metrics with executorId for executor=${id}:\n${metrics}")
    }

    // Union all executors metrics into single DataFrame
    var allExecutorsMetrics: DataFrame = executorsMetricsCombinedMapWithExecId.head match {case (_, b) => b}
    executorsMetricsCombinedMapWithExecId.tail.foreach { case (_, metrics) => allExecutorsMetrics = allExecutorsMetrics.union(metrics)}
    allExecutorsMetrics = allExecutorsMetrics.addColumn(allExecutorsMetrics.select("t").tsToDt)
    log.println(s"\nDisplaying merged metrics for all executors:")
    log.println(allExecutorsMetrics)

    // Executor Start, End, Duration
    val startEndTimes = getExecutorStartEndTimes(ac)
    val startEndTimesWithMetrics = startEndTimes.filter{case (id, _) => driverExecutorMetrics.executorMetricsMap.contains(id.toInt)}
    val combinedExecutorUptime = startEndTimesWithMetrics.map { case (_, (_, _, duration)) => duration }.sum
    val executorTimeSecs = combinedExecutorUptime.milliseconds.toSeconds

    // Metrics
    val executorMemoryMetrics = ExecutorMemoryMetrics(allExecutorsMetrics)
    val clusterMemoryMetrics = ClusterMemoryMetrics(allExecutorsMetrics)
    val clusterCPUMetrics = ClusterCPUMetrics(allExecutorsMetrics, getExecutorCores(ac))
    log.println(clusterMemoryMetrics)
    log.println(clusterCPUMetrics.clusterCpuUsage)

    // Stats
    val driverStats = DriverMemoryStats(driverMetricsMerged)
    val executorStats = ExecutorMemoryStats(allExecutorsMetrics)
    val clusterMemoryStats = ClusterMemoryStats(clusterMemoryMetrics, executorTimeSecs, executorStats)
    val clusterCPUStats = ClusterCPUStats(clusterCPUMetrics.clusterCpuTime, getExecutorCores(ac), executorTimeSecs)
    log.println(executorStats)
    log.println(driverStats)
    log.println(clusterMemoryStats)
    log.println(clusterCPUStats)

    // Warnings
    val warnings: Seq[Option[Warning]] = Seq(
      MissingMetricsWarning(
        allExecutors = ac.executorMap.map { case (id, _) => id.toInt }.toSeq,
        withMetrics = driverExecutorMetrics.executorMetricsMap.map { case (id, _) => id }.toSeq
      ),
      CPUUtilWarning(cpuUtil = clusterCPUStats.cpuUtil, coreHoursWasted = clusterCPUStats.coreHoursWasted, LowCPUUtilizationThreshold),
      HeapUtilWarning(heapUtil = clusterMemoryStats.avgHeapPerc, heapGbHoursWasted = clusterMemoryStats.heapGbHoursWasted, LowHeapUtilizationThreshold)
    )

    SparkScopeResult(
      appInfo = ac.appInfo,
      logs=log.toString,
      stats = SparkScopeStats(
        driverStats = driverStats, 
        executorStats = executorStats,
        clusterMemoryStats = clusterMemoryStats,
        clusterCPUStats = clusterCPUStats
      ),
      metrics = SparkScopeMetrics(
        driverMetrics = driverMetricsMerged,
        executorMemoryMetrics = executorMemoryMetrics, 
        clusterMemoryMetrics = clusterMemoryMetrics, 
        clusterCPUMetrics = clusterCPUMetrics
      ),
      warnings = warnings.flatten
    )
  }
  
  private def getExecutorStartEndTimes(ac: AppContext): Map[String, (Long, Long, Long)] = {
    ac.executorMap.map { case (id, timespan) => {
      val end = timespan.endTime match {
        case 0 => ac.appInfo.endTime
        case _ => timespan.endTime
      }
      (id, (timespan.startTime, end, end - timespan.startTime))
    }
    }.toMap
  }
  
  private def interpolateExecutorMetrics(executorMap: mutable.HashMap[String, ExecutorTimeSpan],
                                         executorsMetricsMap: Map[Int, Seq[DataFrame]]): Map[Int, Seq[DataFrame]] = {
/*    Interpolating executor metrics to align their timestamps for aggregations. Also adds a "zero row" with start values for when executor was added.

            RAW                      INTERPOLATED
      _______________               _______________
      Executor 1:                   Executor 1:
      t,jvm.heap.used               t,jvm.heap.used
      1695358650,100                1695358645,0
      1695358660,200                1695358650,100
                                    1695358655,250
                                    1695358656,200
                            =>
      Executor 2:                   Executor 2:
      t,jvm.heap.used               t,jvm.heap.used
      1695358655,300                1695358650,0
      1695358665,200                1695358655,300
                                    1695358660,250
                                    1695358665,200
*/

    val executorsMetricsMapWithZeroRows: Map[Int, Seq[DataFrame]] = executorsMetricsMap.map { case (id, metrics) =>
      val metricsWithZeroRows = metrics.map{ metric =>

        val executorStartTime = executorMap(id.toString).startTime / MilliSecondsInSec

         val columnsWithZeroCells = metric.columns.map{ col =>
           val colWithZeroCell: DataColumn = col.name match {
             case "t" => col.copy(values=Seq(executorStartTime.toString) ++ col.values)
             case "jvm.heap.max" => col.copy(values=Seq(col.values.head) ++ col.values)
             case _ => col.copy(values=(Seq("0") ++ col.values))
           }
           colWithZeroCell
        }
        metric.copy(columns = columnsWithZeroCells)
      }
      (id, metricsWithZeroRows)
    }

    val allTimestamps = executorsMetricsMapWithZeroRows.flatMap { case (_, metrics) =>
      metrics.flatMap(metric => metric.select("t").values.map(_.toLong))
    }.toSet.toSeq.sorted

    executorsMetricsMapWithZeroRows.map { case (executorId, metricsSeq) =>
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
}

object SparkScopeAnalyzer {
  val BytesInMB: Long = 1024L*1024L
  val NanoSecondsInSec: Long = 1000000000
  private val MilliSecondsInSec: Long = 1000
  val LowCPUUtilizationThreshold: Float = 0.6f
  val LowHeapUtilizationThreshold: Float = 0.6f

  val JvmHeapUsed = "jvm.heap.used" // in bytes
  val JvmHeapUsage = "jvm.heap.usage" // equals used/max
  val JvmHeapMax = "jvm.heap.max" // in bytes
  val JvmNonHeapUsed = "jvm.non-heap.used" // in bytes
  val CpuTime = "executor.cpuTime" // CPU time computing tasks in nanoseconds
  val CpuUsage = "cpuUsage"

  val ExecutorCsvMetrics = Seq(JvmHeapUsed, JvmHeapUsage, JvmHeapMax, JvmNonHeapUsed, CpuTime)
  val DriverCsvMetrics = Seq(JvmHeapUsed, JvmHeapUsage, JvmHeapMax, JvmNonHeapUsed)
}
