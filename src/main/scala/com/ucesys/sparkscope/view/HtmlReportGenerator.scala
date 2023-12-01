package com.ucesys.sparkscope.view

import com.ucesys.sparkscope.SparkScopeAnalyzer.BytesInMB
import com.ucesys.sparkscope.SparkScopeRunner.SparkScopeSign
import com.ucesys.sparkscope.common.{JvmHeapMax, JvmHeapUsage, JvmHeapUsed, JvmNonHeapUsed, SparkScopeConf, SparkScopeLogger}
import com.ucesys.sparkscope.data.{DataColumn, DataTable}
import com.ucesys.sparkscope.io.file.TextFileWriter
import SeriesColor.{Green, Orange, Purple, Red, Yellow}
import com.ucesys.sparkscope.common.MetricUtils.{ColCpuUsage, ColTs}
import com.ucesys.sparkscope.metrics.{SparkScopeMetrics, SparkScopeResult}
import com.ucesys.sparkscope.view.chart.{LimitedChart, SimpleChart}

import java.io.InputStream
import java.nio.file.Paths
import java.time.LocalDateTime.ofEpochSecond
import java.time.ZoneOffset.UTC
import scala.concurrent.duration._

class HtmlReportGenerator(sparkScopeConf: SparkScopeConf, fileWriter: TextFileWriter)
                         (implicit logger: SparkScopeLogger) extends ReportGenerator {
    override def generate(result: SparkScopeResult, sparklensResults: Seq[String]): Unit = {
        val stream: InputStream = getClass.getResourceAsStream("/report-template.html")
        val template: String = scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
        val duration: Option[FiniteDuration] = result.appContext.appEndTime.map(endTime => (endTime - result.appContext.appStartTime).milliseconds)
        val durationStr: String = duration match {
            case Some(duration) if duration < 1.minutes => s"${duration.toSeconds.toString}s"
            case Some(duration) if duration < 1.hours => s"${duration.toMinutes % 60}min ${duration.toSeconds % 60}s"
            case Some(duration) => s"${duration.toHours}h ${duration.toMinutes % 60}min ${duration.toSeconds % 60}s"
            case None => "In progress"
        }

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
          .replace("${logs}", logger.toString)
          .replace("${warnings}", warningsStr)
          .replace("${sparkConf}", sparkScopeConf.sparkConf.getAll.map { case (key, value) => s"${key}: ${value}" }.mkString("\n"))
          .replace("${sparklens}", sparklensResults.mkString("\n"))

        val renderedCharts = renderCharts(rendered, result.metrics)
        val renderedStats = renderStats(renderedCharts, result)

        val outputPath = Paths.get(sparkScopeConf.htmlReportPath, s"${result.appContext.appId}.html")
        fileWriter.write(outputPath.toString, renderedStats)
        logger.info(s"Wrote HTML report file to ${outputPath}")
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

          .replace(
              "${chart.jvm.executor.heap.timestamps}",
              metrics.executor.heapUsedMax.select(ColTs).values.map(ts => s"'${ofEpochSecond(ts.toLong, 0, UTC)}'").mkString(",")
          )
          .replace("${chart.jvm.executor.heap}", generateExecutorHeapCharts(metrics.executor.executorMetricsMap, JvmHeapUsed.name))
          .replace("${chart.jvm.executor.heap.allocation}", metrics.executor.heapAllocation.select(JvmHeapMax.name).div(BytesInMB).toDouble.mkString(","))

          .replace(
              "${chart.jvm.executor.non-heap.timestamps}",
              metrics.executor.nonHeapUsedMax.select(ColTs).values.map(ts => s"'${ofEpochSecond(ts.toLong, 0, UTC)}'").mkString(",")
          )
          .replace("${chart.jvm.executor.non-heap}", generateExecutorHeapCharts(metrics.executor.executorMetricsMap, JvmNonHeapUsed.name))

          .replace("${chart.executor.memoryOverhead}", metrics.executor.nonHeapUsedMax.addConstColumn("memoryOverhead", sparkScopeConf.executorMemOverhead.toMB.toString).select("memoryOverhead").toDouble.mkString(","))

          .replace("${chart.stages.timestamps}", metrics.stage.stageTimeline.select(ColTs).values.map(ts => s"'${ofEpochSecond(ts.toLong, 0, UTC)}'").mkString(","))
          .replace("${chart.stages}", generateStages(metrics.stage.stageTimeline.columns.filter(_.name !=ColTs)))
    }


    def generateStages(stageCol: Seq[DataColumn]): String = {
        stageCol.map(generateStageData).mkString("[", ",", "]")
    }

    def generateStageData(stageCol: DataColumn): String = {
        val color = SeriesColor.randomColorModulo(stageCol.name.toInt, Seq(Green, Red, Yellow, Purple, Orange))
        s"""{
          |             data: [${stageCol.values.mkString(",")}],
          |             label: "stageId=${stageCol.name}",
          |             borderColor: "${color.borderColor}",
          |             backgroundColor: "${color.backgroundColor}",
          |             lineTension: 0.0,
          |             fill: true,
          |             pointRadius: 1,
          |             pointHoverRadius: 8,
          |}""".stripMargin
    }


    def generateExecutorHeapCharts(executorMetricsMap: Map[String, DataTable], metricName: String): String = {
        executorMetricsMap.map{case (id, metrics) =>  generateExecutorChart(id, metrics.select(metricName).div(BytesInMB))}.mkString(",")
    }
    def generateExecutorChart(executorId: String, col: DataColumn): String = {
        val color = SeriesColor.randomColorModulo(executorId.toInt, Seq(Green, Red, Yellow, Purple, Orange))

        s"""
          |{
          |             data: [${col.values.mkString(",")}],
          |             label: "executorId=${executorId}",
          |             borderColor: "${color.borderColor}",
          |             backgroundColor: "${color.backgroundColor}",
          |             fill: false,
          |             pointRadius: 1,
          |             pointHoverRadius: 8,
          |}""".stripMargin
    }

    def renderStats(template: String, result: SparkScopeResult): String = {
        template
          .replace("${stats.cluster.heap.avg.perc}", f"${result.stats.clusterMemoryStats.avgHeapPerc * 100}%1.2f")
          .replace("${stats.cluster.heap.max.perc}", f"${result.stats.clusterMemoryStats.maxHeapPerc * 100}%1.2f")
          .replace("${stats.cluster.cpu.util}", f"${result.stats.clusterCPUStats.cpuUtil * 100}%1.2f")
          .replace("${resource.waste.heap}", f"${result.stats.clusterMemoryStats.heapGbHoursWasted}%1.4f")
          .replace("${resource.waste.cpu}", f"${result.stats.clusterCPUStats.coreHoursWasted}%1.4f")

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

object HtmlReportGenerator {
    val MaxChartPoints: Int = 300
}
