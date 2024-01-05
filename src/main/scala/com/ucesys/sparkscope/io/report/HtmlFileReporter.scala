package com.ucesys.sparkscope.io.report

import com.ucesys.sparkscope.SparkScopeRunner.SparkScopeSign
import com.ucesys.sparkscope.common.MemorySize.BytesInMB
import com.ucesys.sparkscope.common.MetricUtils.{ColCpuUsage, ColTs}
import com.ucesys.sparkscope.common._
import com.ucesys.sparkscope.io.writer.TextFileWriter
import com.ucesys.sparkscope.metrics.{SparkScopeMetrics, SparkScopeResult}
import com.ucesys.sparkscope.view.DurationExtensions.FiniteDurationExtensions
import com.ucesys.sparkscope.view.chart.{ExecutorChart, LimitedChart, SimpleChart}

import java.io.InputStream
import java.nio.file.Paths
import java.time.LocalDateTime.ofEpochSecond
import java.time.ZoneOffset.UTC
import scala.concurrent.duration._

class HtmlFileReporter(sparkScopeConf: SparkScopeConf, htmlFileWriter: TextFileWriter, logFileWriter: TextFileWriter)
                      (implicit logger: SparkScopeLogger) extends Reporter {
    override def report(result: SparkScopeResult): Unit = {
        val stream: InputStream = getClass.getResourceAsStream("/report-template.html")
        val template: String = scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
        val duration: Option[FiniteDuration] = result.appContext.appEndTime.map(endTime => (endTime - result.appContext.appStartTime).milliseconds)
        val durationStr: String = duration.map(_.durationStr).getOrElse("In progress")

        val warningsStr: String = result.warnings match {
            case Seq() => ""
            case warnings => "WARNINGS FOUND:\n- " + warnings.mkString("\n- ")
        }

        val rendered = template
          .replace("${sparkScopeSign}", SparkScopeSign)
          .replace("${appInfo.appName}", sparkScopeConf.appName.getOrElse(sparkScopeConf.sparkConf.getOption("spark.app.name").getOrElse("None")))
          .replace("${appInfo.applicationId}", result.appContext.appId)
          .replace("${appInfo.start}", ofEpochSecond(result.appContext.appStartTime / 1000, 0, UTC).toString)
          .replace("${appInfo.end}", result.appContext.appEndTime.map(endTime => ofEpochSecond(endTime/ 1000, 0, UTC).toString).getOrElse("In progress"))
          .replace("${appInfo.duration}", durationStr)
          .replace("${warnings}", warningsStr)
          .replace("${sparkConf}", sparkScopeConf.sparkConf.getAll.map { case (key, value) => s"${key}: ${value}" }.mkString("\n"))

        val renderedCharts = renderCharts(rendered, result.metrics)
        val renderedStats = renderStats(renderedCharts, result)

        val outputPath = Paths.get(sparkScopeConf.htmlReportPath, s"${result.appContext.appId}.html")
        htmlFileWriter.write(outputPath.toString, renderedStats)
        logger.info(s"Wrote HTML report file to ${outputPath}", this.getClass)

        val logPath = Paths.get(sparkScopeConf.logPath, s"${result.appContext.appId}.log")
        logFileWriter.write(logPath.toString, logger.toString)
        logger.info(s"Log saved to ${logPath}", this.getClass)
    }

    def renderCharts(template: String, metrics: SparkScopeMetrics): String = {
        val cpuUtilChart = SimpleChart(
            metrics.clusterCpu.clusterCpuUsage.select(ColTs),
            metrics.clusterCpu.clusterCpuUsage.select(ColCpuUsage).mul(100)
        )
        val heapUtilChart = SimpleChart(
            metrics.clusterMemory.heapUsage.select(ColTs),
            metrics.clusterMemory.heapUsage.select(JvmHeapUsage.name).mul(100)
        )

        val numExecutorsChart = SimpleChart(
            metrics.clusterCpu.numExecutors.select(ColTs),
            metrics.clusterCpu.numExecutors.select("cnt")
        )

        val cpuUtilsVsCapacityChart = LimitedChart(
            metrics.clusterCpu.clusterCpuUsage.select(ColTs),
            metrics.clusterCpu.clusterCpuUsageSum.select("cpuUsageAllCores"),
            metrics.clusterCpu.clusterCapacity.select("totalCores")
        )

        val heapUtilVsSizeChart = LimitedChart(
            metrics.clusterMemory.heapUsed.select(ColTs),
            metrics.clusterMemory.heapUsed.select(JvmHeapUsed.name).div(BytesInMB),
            metrics.clusterMemory.heapMax.select(JvmHeapMax.name).div(BytesInMB)
        )

        val driverHeapUtilChart = LimitedChart(
            metrics.driver.select(ColTs),
            metrics.driver.select(JvmHeapUsed.name).div(BytesInMB),
            metrics.driver.select(JvmHeapMax.name).div(BytesInMB)
        )

        val driverNonHeapUtilChart = LimitedChart(
            metrics.driver.select(ColTs),
            metrics.driver.select("jvm.non-heap.used").div(BytesInMB),
            metrics.driver.addConstColumn("memoryOverhead", sparkScopeConf.driverMemOverhead.toMB.toString).select("memoryOverhead")
        )

        val executorHeapChart = ExecutorChart(
            metrics.executor.heapUsedMax.select(ColTs),
            metrics.executor.heapAllocation.select(JvmHeapMax.name).div(BytesInMB),
            metrics.executor.executorMetricsMap.map { case (id, metrics) => metrics.select(JvmHeapUsed.name).div(BytesInMB).rename(id) }.toSeq
        )

        val executorNonHeapChart = ExecutorChart(
            metrics.executor.nonHeapUsedMax.select(ColTs),
            metrics.executor.nonHeapUsedMax.addConstColumn("memoryOverhead", sparkScopeConf.executorMemOverhead.toMB.toString).select("memoryOverhead"),
            metrics.executor.executorMetricsMap.map { case (id, metrics) => metrics.select(JvmNonHeapUsed.name).div(BytesInMB).rename(id) }.toSeq
        )

        val tasksChart = LimitedChart(
            metrics.clusterCpu.clusterCapacity.select(ColTs),
            metrics.stage.numberOfTasks,
            metrics.clusterCpu.clusterCapacity.select("totalCores")
        )

        template
          .replace("${chart.cluster.cpu.util}", cpuUtilChart.values.mkString(","))
          .replace("${chart.cluster.cpu.util.timestamps}", cpuUtilChart.labels.mkString(","))

          .replace("${chart.jvm.cluster.heap.usage}", heapUtilChart.values.mkString(","))
          .replace("${chart.jvm.cluster.heap.usage.timestamps}", heapUtilChart.labels.mkString(","))

          .replace("${chart.cluster.cpu.capacity}", cpuUtilsVsCapacityChart.limits.mkString(","))
          .replace("${chart.cluster.cpu.usage}", cpuUtilsVsCapacityChart.values.mkString(","))
          .replace("${chart.cluster.cpu.usage.timestamps}", cpuUtilsVsCapacityChart.labels.mkString(","))

          .replace("${chart.jvm.cluster.heap.used}", heapUtilVsSizeChart.values.mkString(","))
          .replace("${chart.jvm.cluster.heap.max}",heapUtilVsSizeChart.limits.mkString(","))
          .replace("${chart.jvm.cluster.heap.timestamps}", heapUtilVsSizeChart.labels.mkString(","))

          .replace("${chart.cluster.numExecutors.timestamps}", numExecutorsChart.labels.mkString(","))
          .replace("${chart.cluster.numExecutors}",numExecutorsChart.values.mkString(","))

          .replace("${chart.jvm.driver.heap.timestamps}", driverHeapUtilChart.labels.mkString(","))
          .replace("${chart.jvm.driver.heap.used}", driverHeapUtilChart.values.mkString(","))
          .replace("${chart.jvm.driver.heap.size}", driverHeapUtilChart.limits.mkString(","))

          .replace("${chart.jvm.driver.non-heap.timestamps}", driverNonHeapUtilChart.labels.mkString(","))
          .replace("${chart.jvm.driver.non-heap.used}", driverNonHeapUtilChart.values.mkString(","))
          .replace("${chart.driver.memoryOverhead}", driverNonHeapUtilChart.limits.mkString(","))

          .replace("${chart.tasks.timestamps}", tasksChart.labels.mkString(","))
          .replace("${chart.tasks}", tasksChart.values.mkString(","))
          .replace("${chart.tasks.capacity}", tasksChart.limits.mkString(","))

          .replace("${chart.jvm.executor.heap.timestamps}", executorHeapChart.labels.mkString(","))
          .replace("${chart.jvm.executor.heap}", executorHeapChart.datasets)
          .replace("${chart.jvm.executor.heap.allocation}", executorHeapChart.limits.mkString(","))

          .replace("${chart.jvm.executor.non-heap.timestamps}", executorNonHeapChart.labels.mkString(","))
          .replace("${chart.jvm.executor.non-heap}",executorNonHeapChart.datasets)
          .replace("${chart.executor.memoryOverhead}", executorNonHeapChart.limits.mkString(","))
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
