
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

import com.ucesys.sparkscope.common.{ExecutorContext, SparkScopeContext, SparkScopeLogger}
import com.ucesys.sparkscope.SparkScopeAnalyzer._
import com.ucesys.sparkscope.data.{DataColumn, DataTable}
import com.ucesys.sparkscope.io.metrics.DriverExecutorMetrics
import com.ucesys.sparkscope.metrics._
import com.ucesys.sparkscope.warning.{CPUUtilWarning, HeapUtilWarning, MissingMetricsWarning, Warning}

import scala.concurrent.duration._

class SparkScopeAnalyzer(implicit logger: SparkScopeLogger) {

    def analyze(driverExecutorMetrics: DriverExecutorMetrics, appContext: SparkScopeContext): SparkScopeResult = {
        var driverMetricsMerged: DataTable = driverExecutorMetrics.driverMetrics.head
        driverExecutorMetrics.driverMetrics.tail.foreach { metric =>
            driverMetricsMerged = driverMetricsMerged.mergeOn("t", metric)
        }
        logger.println(s"\nDisplaying merged metrics for driver:\n${driverMetricsMerged}")

        driverExecutorMetrics.executorMetricsMap.foreach { case (id, metrics) =>
            metrics.foreach { metric => logger.println(s"\nDisplaying ${metric.name} metric for executor=${id}:\n${metric}")
            }
        }

        // Interpolating metrics
        val executorsMetricsMapInterpolated = interpolateExecutorMetrics(appContext.executorMap, driverExecutorMetrics.executorMetricsMap, appContext)
        executorsMetricsMapInterpolated.foreach { case (id, metrics) =>
            metrics.foreach { metric => logger.println(s"\nInterpolated ${metric.name} metric for executor=${id}:\n${metric}") }
        }

        // For each executor, merge it's respective metrics into a single DataFrame(one for each executor)
        val executorsMetricsCombinedMap: Map[String, DataTable] = executorsMetricsMapInterpolated.map { case (executorId, metrics) =>
            var mergedMetrics = metrics.head
            metrics.tail.foreach(metric => mergedMetrics = mergedMetrics.mergeOn("t", metric))
            (executorId, mergedMetrics)
        }
        executorsMetricsCombinedMap.foreach { case (id, m) => logger.println(s"\nMerged metrics for executor=${id}:\n${m}") }

        // Add cpu usage for each executor
        val executorsMetricsCombinedMapWithCpuUsage: Map[String, DataTable] = executorsMetricsCombinedMap.map { case (id, metrics) =>
            val clusterCpuTime = metrics.select(CpuTime).div(NanoSecondsInSec)
            val clusterCpuTimeLag = clusterCpuTime.lag
            val clusterCpuTimeDiff = clusterCpuTime.sub(clusterCpuTimeLag)
            val timeLag = metrics.select("t").lag
            val timeElapsed = metrics.select("t").sub(timeLag)
            val cpuUsageAllCores = clusterCpuTimeDiff.div(timeElapsed).rename("cpuUsageAllCores")
            val cpuUsage = cpuUsageAllCores.div(appContext.executorCores).rename(CpuUsage)
            val metricsWithCpuUsage = metrics.addColumn(cpuUsage).addColumn(cpuUsageAllCores)
            (id, metricsWithCpuUsage)
        }

        // Add executorId column for later union
        val executorsMetricsCombinedMapWithExecId: Map[String, DataTable] = executorsMetricsCombinedMapWithCpuUsage.map { case (id, metrics) =>
            val metricsWithExecId = metrics.addConstColumn("executorId", id.toString)
            (id, metricsWithExecId)
        }

        executorsMetricsCombinedMapWithExecId.foreach { case (id, metrics) =>
            logger.println(s"\nDisplaying merged metrics with executorId for executor=${id}:\n${metrics}")
        }

        // Union all executors metrics into single DataFrame
        var allExecutorsMetrics: DataTable = executorsMetricsCombinedMapWithExecId.head match {
            case (_, b) => b
        }
        executorsMetricsCombinedMapWithExecId.tail.foreach { case (_, metrics) => allExecutorsMetrics = allExecutorsMetrics.union(metrics) }
        allExecutorsMetrics = allExecutorsMetrics.addColumn(allExecutorsMetrics.select("t").tsToDt)
        logger.println(s"\nDisplaying merged metrics for all executors:")
        logger.println(allExecutorsMetrics)

        // Executor Timeline
        val executorMapWithMetrics = appContext.executorMap.filter { case (id, _) => driverExecutorMetrics.executorMetricsMap.contains(id) }

        val executorTimelineRows: Seq[Seq[String]] = executorMapWithMetrics.map {
            case (id, executorContext) =>
                val lastMetricTime = executorsMetricsCombinedMapWithExecId(id).select("t").max.toLong
                val uptime = executorContext.upTime(lastMetricTime)
                Seq(
                    id,
                    executorContext.addTime.toString,
                    executorContext.removeTime.map(_.toString).getOrElse("null"),
                    lastMetricTime.toString,
                    uptime.toString
                )
        }.toSeq

        val executorTimeLine = DataTable.fromRows(
            "executor timeline",
            Seq("executorId", "addTime", "removeTime", "lastMetricTime", "upTimeInMs"),
            executorTimelineRows
        )

        val executorTimeSecs = executorTimeLine.select("upTimeInMs").sum.milliseconds.toSeconds

        logger.println(executorTimeLine)

        val allTimestamps = executorsMetricsCombinedMapWithCpuUsage.flatMap { case (_, dt) =>
            dt.select("t").values.map(_.toLong)
        }.toSet.toSeq.sorted.map(_.toString)

        val executorMetricsAligned = executorsMetricsCombinedMapWithCpuUsage.map{ case (id, dt) =>
            val existingTimestamps = dt.select("t").values
            val newTimestamps = allTimestamps.filterNot(existingTimestamps.contains)
            val aligned = DataTable(
                "heapUsed",
                Seq(
                    DataColumn("t", dt.select("t").values ++ newTimestamps),
                    DataColumn(JvmHeapUsed, dt.select(JvmHeapUsed).values ++ Seq.fill(newTimestamps.length)("null")),
                    DataColumn(JvmNonHeapUsed, dt.select(JvmNonHeapUsed).values ++ Seq.fill(newTimestamps.length)("null")),
                )
            ).sortBy("t")
            (id, aligned)
        }

        // Metrics
        val clusterMemoryMetrics = ClusterMemoryMetrics(allExecutorsMetrics)
        val clusterCPUMetrics = ClusterCPUMetrics(allExecutorsMetrics, appContext.executorCores)
        logger.println(clusterMemoryMetrics)
        logger.println(clusterCPUMetrics)

        // Stats
        val executorStats = ExecutorMemoryStats(allExecutorsMetrics)
        val clusterMemoryStats = ClusterMemoryStats(clusterMemoryMetrics, executorTimeSecs, executorStats)
        val clusterCPUStats = ClusterCPUStats(clusterCPUMetrics, appContext.executorCores, executorTimeSecs)

        // Warnings
        val warnings: Seq[Option[Warning]] = Seq(
            MissingMetricsWarning(
                allExecutors = appContext.executorMap.map { case (id, _) => id }.toSeq,
                withMetrics = driverExecutorMetrics.executorMetricsMap.map { case (id, _) => id }.toSeq
            ),
            CPUUtilWarning(cpuUtil = clusterCPUStats.cpuUtil, coreHoursWasted = clusterCPUStats.coreHoursWasted, LowCPUUtilizationThreshold),
            HeapUtilWarning(heapUtil = clusterMemoryStats.avgHeapPerc, heapGbHoursWasted = clusterMemoryStats.heapGbHoursWasted, LowHeapUtilizationThreshold)
        )

        SparkScopeResult(
            appContext = appContext,
            stats = SparkScopeStats(
                driverStats = DriverMemoryStats(driverMetricsMerged),
                executorStats = executorStats,
                clusterMemoryStats = clusterMemoryStats,
                clusterCPUStats = clusterCPUStats
            ),
            metrics = SparkScopeMetrics(
                driverMetrics = driverMetricsMerged,
                executorMemoryMetrics = ExecutorMemoryMetrics(allExecutorsMetrics, executorMetricsAligned),
                clusterMemoryMetrics = clusterMemoryMetrics,
                clusterCPUMetrics = clusterCPUMetrics,
                stageMetrics = StageMetrics(appContext.stages, allTimestamps, clusterCPUMetrics.clusterCapacity)
            ),
            warnings = warnings.flatten
        )
    }

    private def interpolateExecutorMetrics(executorMap: Map[String, ExecutorContext],
                                           executorsMetricsMap: Map[String, Seq[DataTable]],
                                           appContext: SparkScopeContext): Map[String, Seq[DataTable]] = {
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

        val executorsMetricsMapWithZeroRows: Map[String, Seq[DataTable]] = executorsMetricsMap.map { case (id, metrics) =>
            val metricsWithZeroRows = metrics.map { metric =>

                val executorStartTime = executorMap(id.toString).addTime / MilliSecondsInSec

                if (metric.select("t").values.contains(executorStartTime.toString)) {
                    metric
                } else {
                    val columnsWithZeroCells = metric.columns.map { col =>
                        val colWithZeroCell: DataColumn = col.name match {
                            case "t" => col.copy(values = Seq(executorStartTime.toString) ++ col.values)
                            case "jvm.heap.max" => col.copy(values = Seq(col.values.head) ++ col.values)
                            case _ => col.copy(values = (Seq("0") ++ col.values))
                        }
                        colWithZeroCell
                    }
                    metric.copy(columns = columnsWithZeroCells)
                }
            }
            (id, metricsWithZeroRows)
        }


        val allMetricsTimestamps = executorsMetricsMapWithZeroRows.flatMap { case (_, metrics) =>
            metrics.flatMap(metric => metric.select("t").values.map(_.toLong))
        }

        val allTimestamps = (allMetricsTimestamps ++ appContext.stages.flatMap(_.getTimeline)).toSet.toSeq.sorted

        executorsMetricsMapWithZeroRows.map { case (executorId, metricsSeq) =>
            val metricsInterpolated: Seq[DataTable] = metricsSeq.map(metrics => {
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

                val missingTsDF = DataTable("missingTs",
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
    val BytesInMB: Long = 1024L * 1024L
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

    val ExecutorCsvMetrics: Seq[String] = Seq(JvmHeapUsed, JvmHeapUsage, JvmHeapMax, JvmNonHeapUsed, CpuTime)
    val DriverCsvMetrics: Seq[String] = Seq(JvmHeapUsed, JvmHeapUsage, JvmHeapMax, JvmNonHeapUsed)
}
