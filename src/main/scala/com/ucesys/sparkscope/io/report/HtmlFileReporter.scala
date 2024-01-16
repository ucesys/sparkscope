/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.ucesys.sparkscope.io.report

import com.ucesys.sparkscope.SparkScopeRunner.SparkScopeSign
import com.ucesys.sparkscope.common._
import com.ucesys.sparkscope.io.writer.TextFileWriter
import com.ucesys.sparkscope.metrics.SparkScopeResult
import com.ucesys.sparkscope.view.DurationExtensions.FiniteDurationExtensions
import com.ucesys.sparkscope.view.chart.SparkScopeCharts

import java.io.InputStream
import java.nio.file.Paths
import java.time.LocalDateTime.ofEpochSecond
import java.time.ZoneOffset.UTC
import scala.concurrent.duration._

class HtmlFileReporter(appContext: AppContext,
                       sparkScopeConf: SparkScopeConf,
                       htmlFileWriter: TextFileWriter)
                      (implicit logger: SparkScopeLogger) extends Reporter {
    override def report(result: SparkScopeResult): Unit = {
        val stream: InputStream = getClass.getResourceAsStream("/report-template.html")
        val template: String = scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
        val duration: Option[FiniteDuration] = appContext.appEndTime.map(endTime => (endTime - appContext.appStartTime).milliseconds)
        val durationStr: String = duration.map(_.durationStr).getOrElse("In progress")

        val warningsStr: String = result.warnings match {
            case Seq() => ""
            case warnings => "WARNINGS FOUND:\n- " + warnings.mkString("\n- ")
        }

        val rendered = template
          .replace("${sparkScopeSign}", SparkScopeSign)
          .replace("${appInfo.appName}", sparkScopeConf.appName.getOrElse(sparkScopeConf.sparkConf.getOption("spark.app.name").getOrElse("None")))
          .replace("${appInfo.applicationId}", appContext.appId)
          .replace("${appInfo.start}", ofEpochSecond(appContext.appStartTime / 1000, 0, UTC).toString)
          .replace("${appInfo.end}", appContext.appEndTime.map(endTime => ofEpochSecond(endTime/ 1000, 0, UTC).toString).getOrElse("In progress"))
          .replace("${appInfo.duration}", durationStr)
          .replace("${warnings}", warningsStr)
          .replace("${sparkConf}", sparkScopeConf.sparkConf.getAll.map { case (key, value) => s"${key}: ${value}" }.mkString("\n"))

        val renderedCharts = renderCharts(rendered, result.charts)
        val renderedStats = renderStats(renderedCharts, result)

        sparkScopeConf.htmlReportPath.foreach { htmlReportPath =>
            val outputPath = Paths.get(htmlReportPath, s"${appContext.appId}.html")
            htmlFileWriter.write(outputPath.toString, renderedStats)
            logger.info(s"Wrote HTML report file to ${outputPath}", this.getClass)
        }
    }

    def renderCharts(template: String, charts: SparkScopeCharts): String = {
        template
          .replace("${chart.cluster.cpu.util}", charts.cpuUtilChart.values.mkString(","))
          .replace("${chart.cluster.cpu.util.timestamps}", charts.cpuUtilChart.labels.mkString(","))

          .replace("${chart.jvm.cluster.heap.usage}", charts.heapUtilChart.values.mkString(","))
          .replace("${chart.jvm.cluster.heap.usage.timestamps}", charts.heapUtilChart.labels.mkString(","))

          .replace("${chart.cluster.cpu.capacity}", charts.cpuUtilsVsCapacityChart.limits.mkString(","))
          .replace("${chart.cluster.cpu.usage}", charts.cpuUtilsVsCapacityChart.values.mkString(","))
          .replace("${chart.cluster.cpu.usage.timestamps}", charts.cpuUtilsVsCapacityChart.labels.mkString(","))

          .replace("${chart.jvm.cluster.heap.used}", charts.heapUtilVsSizeChart.values.mkString(","))
          .replace("${chart.jvm.cluster.heap.max}", charts.heapUtilVsSizeChart.limits.mkString(","))
          .replace("${chart.jvm.cluster.heap.timestamps}", charts.heapUtilVsSizeChart.labels.mkString(","))

          .replace("${chart.cluster.numExecutors.timestamps}", charts.numExecutorsChart.labels.mkString(","))
          .replace("${chart.cluster.numExecutors}", charts.numExecutorsChart.values.mkString(","))

          .replace("${chart.jvm.driver.heap.timestamps}", charts.driverHeapUtilChart.labels.mkString(","))
          .replace("${chart.jvm.driver.heap.used}", charts.driverHeapUtilChart.values.mkString(","))
          .replace("${chart.jvm.driver.heap.size}", charts.driverHeapUtilChart.limits.mkString(","))

          .replace("${chart.jvm.driver.non-heap.timestamps}", charts.driverNonHeapUtilChart.labels.mkString(","))
          .replace("${chart.jvm.driver.non-heap.used}", charts.driverNonHeapUtilChart.values.mkString(","))
          .replace("${chart.driver.memoryOverhead}", charts.driverNonHeapUtilChart.limits.mkString(","))

          .replace("${chart.tasks.timestamps}", charts.tasksChart.labels.mkString(","))
          .replace("${chart.tasks}", charts.tasksChart.values.mkString(","))
          .replace("${chart.tasks.capacity}", charts.tasksChart.limits.mkString(","))

          .replace("${chart.jvm.executor.heap.timestamps}", charts.executorHeapChart.labels.mkString(","))
          .replace("${chart.jvm.executor.heap}", charts.executorHeapChart.datasets)
          .replace("${chart.jvm.executor.heap.allocation}", charts.executorHeapChart.limits.mkString(","))

          .replace("${chart.jvm.executor.non-heap.timestamps}", charts.executorNonHeapChart.labels.mkString(","))
          .replace("${chart.jvm.executor.non-heap}", charts.executorNonHeapChart.datasets)
          .replace("${chart.executor.memoryOverhead}", charts.executorNonHeapChart.limits.mkString(","))
    }

    def renderStats(template: String, result: SparkScopeResult): String = {
        template
          .replace("${stats.cluster.cpu.util}", f"${result.stats.clusterCPUStats.cpuUtil * 100}%1.2f")
          .replace("${stats.cluster.heap.avg.perc}", f"${result.stats.clusterMemoryStats.avgHeapPerc * 100}%1.2f")
          .replace("${stats.cluster.heap.max.perc}", f"${result.stats.clusterMemoryStats.maxHeapPerc * 100}%1.2f")

          .replace("${resource.alloc.cpu}", f"${result.stats.clusterCPUStats.coreHoursAllocated}%1.4f")
          .replace("${resource.alloc.heap}", f"${result.stats.clusterMemoryStats.heapGbHoursAllocated}%1.4f")
          .replace("${resource.waste.cpu}", f"${result.stats.clusterCPUStats.coreHoursWasted}%1.4f")
          .replace("${resource.waste.heap}", f"${result.stats.clusterMemoryStats.heapGbHoursWasted}%1.4f")

          .replace("${stats.executor.heap.max}", result.stats.executorStats.maxHeap.toString)
          .replace("${stats.executor.heap.max.perc}", f"${result.stats.executorStats.maxHeapPerc * 100}%1.2f")
          .replace("${stats.executor.heap.avg}", result.stats.executorStats.avgHeap.toString)
          .replace("${stats.executor.heap.avg.perc}", f"${result.stats.executorStats.avgHeapPerc * 100}%1.2f")
          .replace("${stats.executor.non-heap.avg}", result.stats.executorStats.avgNonHeap.toString)
          .replace("${stats.executor.non-heap.max}", result.stats.executorStats.maxNonHeap.toString)

          .replace("${stats.driver.heap.max}", result.stats.driverStats.maxHeap.toString)
          .replace("${stats.driver.heap.max.perc}", f"${result.stats.driverStats.maxHeapPerc * 100}%1.2f")
          .replace("${stats.driver.heap.avg}", result.stats.driverStats.avgHeap.toString)
          .replace("${stats.driver.heap.avg.perc}", f"${result.stats.driverStats.avgHeapPerc * 100}%1.2f")
          .replace("${stats.driver.non-heap.avg}", result.stats.driverStats.avgNonHeap.toString)
          .replace("${stats.driver.non-heap.max}", result.stats.driverStats.maxNonHeap.toString)
    }
}

object HtmlFileReporter {
    val MaxChartPoints: Int = 300
    val MaxStageChartPoints: Int = 2000
    val MaxExecutorChartPoints: Int = 2000
}
