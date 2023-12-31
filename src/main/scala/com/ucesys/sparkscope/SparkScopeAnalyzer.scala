
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

import com.ucesys.sparkscope.common.{AppContext, CpuTime, JvmHeapUsed, JvmNonHeapUsed, SparkScopeLogger}
import com.ucesys.sparkscope.SparkScopeAnalyzer._
import com.ucesys.sparkscope.agg.TaskAggMetrics
import com.ucesys.sparkscope.common.MetricUtils.{ColCpuUsage, ColTs}
import com.ucesys.sparkscope.data.{DataColumn, DataTable}
import com.ucesys.sparkscope.io.metrics.DriverExecutorMetrics
import com.ucesys.sparkscope.metrics._
import com.ucesys.sparkscope.stats.{ClusterCPUStats, ClusterMemoryStats, DriverMemoryStats, ExecutorMemoryStats, SparkScopeStats}
import com.ucesys.sparkscope.timeline.ExecutorTimeline
import com.ucesys.sparkscope.warning.{CPUUtilWarning, DiskSpillWarning, GCTimeWarning, HeapUtilWarning, MissingMetricsWarning, Warning}

import scala.concurrent.duration._

class SparkScopeAnalyzer(implicit logger: SparkScopeLogger) {

    def analyze(driverExecutorMetrics: DriverExecutorMetrics, appContext: AppContext, taskAggMetrics: TaskAggMetrics): SparkScopeResult = {
        logger.debug(s"\nDisplaying merged metrics for driver:\n${driverExecutorMetrics.driverMetrics}", this.getClass)

        driverExecutorMetrics.executorMetricsMap.foreach { case (id, metrics) =>
            logger.debug(s"\nDisplaying metrics for executor=${id}:\n${metrics}", this.getClass)
        }

        // Interpolating metrics
        val executorsMetricsMapInterpolated: Map[String, DataTable] = interpolateExecutorMetrics(appContext.executorMap, driverExecutorMetrics.executorMetricsMap, appContext)
        executorsMetricsMapInterpolated.foreach { case (id, metric) => logger.debug(s"\nInterpolated metrics for executor=${id}:\n${metric}", this.getClass)}

        // Add cpu usage for each executor
        val executorsMetricsCombinedMapWithCpuUsage: Map[String, DataTable] = executorsMetricsMapInterpolated.map { case (id, metrics) =>
            val clusterCpuTime = metrics.select(CpuTime.name).div(NanoSecondsInSec)
            val clusterCpuTimeLag = clusterCpuTime.lag
            val clusterCpuTimeDiff = clusterCpuTime.sub(clusterCpuTimeLag)
            val timeLag = metrics.select("t").lag
            val timeElapsed = metrics.select("t").sub(timeLag)
            val cpuUsageAllCores = clusterCpuTimeDiff.div(timeElapsed).rename("cpuUsageAllCores")
            val cpuUsage = cpuUsageAllCores.div(appContext.executorCores).rename(ColCpuUsage)
            val metricsWithCpuUsage = metrics.addColumn(cpuUsage).addColumn(cpuUsageAllCores)
            (id, metricsWithCpuUsage)
        }

        // Add executorId column for later union
        val executorsMetricsCombinedMapWithExecId: Map[String, DataTable] = executorsMetricsCombinedMapWithCpuUsage.map { case (id, metrics) =>
            val metricsWithExecId = metrics.addConstColumn("executorId", id.toString)
            (id, metricsWithExecId)
        }

        executorsMetricsCombinedMapWithExecId.foreach { case (id, metrics) =>
            logger.debug(s"\nDisplaying merged metrics with executorId for executor=${id}:\n${metrics}", this.getClass)
        }

        // Union all executors metrics into single DataFrame
        var allExecutorsMetrics: DataTable = executorsMetricsCombinedMapWithExecId.head match {
            case (_, b) => b
        }
        executorsMetricsCombinedMapWithExecId.tail.foreach { case (_, metrics) => allExecutorsMetrics = allExecutorsMetrics.union(metrics) }
        allExecutorsMetrics = allExecutorsMetrics.addColumn(allExecutorsMetrics.select("t").tsToDt)
        logger.debug(s"\nDisplaying merged metrics for all executors:", this.getClass)
        logger.debug(allExecutorsMetrics, this.getClass)

        // Executor Timeline
        val executorMapWithMetrics = appContext.executorMap.filter { case (id, _) => driverExecutorMetrics.executorMetricsMap.contains(id) }

        val executorTimelineRows: Seq[Seq[String]] = executorMapWithMetrics.map {
            case (id, executorTimeline) =>
                val lastMetricTime = executorsMetricsCombinedMapWithExecId(id).select("t").max.toLong
                val uptime = executorTimeline.duration(lastMetricTime)
                Seq(
                    id,
                    executorTimeline.startTime.toString,
                    executorTimeline.endTime.map(_.toString).getOrElse("null"),
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

        logger.debug(executorTimeLine, this.getClass)

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
                    DataColumn(JvmHeapUsed.name, dt.select(JvmHeapUsed.name).values ++ Seq.fill(newTimestamps.length)("null")),
                    DataColumn(JvmNonHeapUsed.name, dt.select(JvmNonHeapUsed.name).values ++ Seq.fill(newTimestamps.length)("null")),
                )
            ).sortBy("t")
            (id, aligned)
        }

        // Metrics
        val clusterMemoryMetrics = ClusterMemoryMetrics(allExecutorsMetrics)
        val clusterCPUMetrics = ClusterCPUMetrics(allExecutorsMetrics, appContext.executorCores)
        logger.debug(clusterMemoryMetrics, this.getClass)
        logger.debug(clusterCPUMetrics, this.getClass)

        // Stats
        val executorStats = ExecutorMemoryStats(allExecutorsMetrics)
        val clusterMemoryStats = ClusterMemoryStats(clusterMemoryMetrics, executorTimeSecs, executorStats)
        val clusterCPUStats = ClusterCPUStats(clusterCPUMetrics, appContext.executorCores, executorTimeSecs)

        // Warnings
        val warnings: Seq[Warning] = Seq(
            MissingMetricsWarning(
                allExecutors = appContext.executorMap.map { case (id, _) => id }.toSeq,
                withMetrics = driverExecutorMetrics.executorMetricsMap.map { case (id, _) => id }.toSeq
            ),
            CPUUtilWarning(cpuUtil = clusterCPUStats.cpuUtil, coreHoursWasted = clusterCPUStats.coreHoursWasted, LowCPUUtilizationThreshold),
            HeapUtilWarning(heapUtil = clusterMemoryStats.avgHeapPerc, heapGbHoursWasted = clusterMemoryStats.heapGbHoursWasted, LowHeapUtilizationThreshold),
            DiskSpillWarning(taskAggMetrics),
            GCTimeWarning(taskAggMetrics),
        ).flatten

        SparkScopeResult(
            appContext = appContext,
            stats = SparkScopeStats(
                driverStats = DriverMemoryStats(driverExecutorMetrics.driverMetrics),
                executorStats = executorStats,
                clusterMemoryStats = clusterMemoryStats,
                clusterCPUStats = clusterCPUStats
            ),
            metrics = SparkScopeMetrics(
                driver = driverExecutorMetrics.driverMetrics,
                executor = ExecutorMemoryMetrics(allExecutorsMetrics, executorMetricsAligned),
                clusterMemory = clusterMemoryMetrics,
                clusterCpu= clusterCPUMetrics,
                stage = StageMetrics(appContext.stages, allTimestamps)
            ),
            warnings = warnings
        )
    }

    private def interpolateExecutorMetrics(executorMap: Map[String, ExecutorTimeline],
                                           executorsMetricsMap: Map[String, DataTable],
                                           appContext: AppContext): Map[String, DataTable] = {
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

        val executorsMetricsMapWithZeroRows: Map[String, DataTable] = executorsMetricsMap.map { case (id, metrics) =>
            val metricsWithZeroRows = {

                val executorStartTime = executorMap(id).startTime / MilliSecondsInSec

                if (metrics.select("t").values.contains(executorStartTime.toString)) {
                    metrics
                } else {
                    val columnsWithZeroCells: Seq[DataColumn] = metrics.columns.map { col =>
                        val colWithZeroCell: DataColumn = col.name match {
                            case "t" => col.copy(values = Seq(executorStartTime.toString) ++ col.values)
                            case "jvm.heap.max" => col.copy(values = Seq(col.values.head) ++ col.values)
                            case _ => col.copy(values = (Seq("0") ++ col.values))
                        }
                        colWithZeroCell
                    }
                    metrics.copy(columns = columnsWithZeroCells)
                }
            }
            (id, metricsWithZeroRows)
        }


        val allMetricsTimestamps = executorsMetricsMapWithZeroRows.flatMap { case (_, metrics) => metrics.select("t").values.map(_.toLong)}

        val allTimestamps = (allMetricsTimestamps ++ appContext.stages.flatMap(_.getTimeline)).toSet.toSeq.sorted

        executorsMetricsMapWithZeroRows.map { case (executorId, metrics) =>
            val metricsInterpolated: DataTable = {
                val localTimestamps = metrics.columns.head.values.map(_.toLong)
                val missingTimestamps = allTimestamps.filterNot(localTimestamps.contains)
                val missingTimestampsInRange = missingTimestamps.filter(ts => ts > localTimestamps.min && ts < localTimestamps.max)

                val metricsZipped = (localTimestamps zip metrics.toRows.map(_.tail))

                val interpolatedRows: Seq[Seq[String]] = missingTimestampsInRange.flatMap(missingTimestamp => {
                    val interpolatedValue = metricsZipped.sliding(2).flatMap { case Seq((prevTs, prevVals), (nextTs, nextVals)) =>
                        val interpolatedRows = missingTimestamp match {
                            case ts if (ts > prevTs && ts < nextTs) => {
                                val interpolatedRow: Seq[String] = (prevVals zip nextVals).map { case (prevVal, nextVal) =>
                                    val interpolatedValue: String = {
                                        val diffPrevToNext = nextTs - prevTs
                                        val diffToPrev = ts - prevTs
                                        val diffToNext = nextTs - ts
                                        val interpolated = (prevVal.toDouble * (diffPrevToNext - diffToPrev) + nextVal.toDouble * (diffPrevToNext - diffToNext)) / diffPrevToNext
                                        interpolated.toString
                                    }
                                    interpolatedValue
                                }
                                Some(Seq(ts.toString) ++ interpolatedRow)
                            }
                            case _ => None
                        }
                        interpolatedRows
                      }.toSeq.headOption
                    interpolatedValue
                })
                interpolatedRows match {
                    case Seq() => metrics
                    case _ => metrics.union(DataTable.fromRows("missing", metrics.columnsNames, interpolatedRows)).sortBy(ColTs)
                }
            }
            (executorId, metricsInterpolated)
        }
    }
}

object SparkScopeAnalyzer {
    val NanoSecondsInSec: Long = 1000000000
    private val MilliSecondsInSec: Long = 1000
    val LowCPUUtilizationThreshold: Float = 0.6f
    val LowHeapUtilizationThreshold: Float = 0.6f
}
