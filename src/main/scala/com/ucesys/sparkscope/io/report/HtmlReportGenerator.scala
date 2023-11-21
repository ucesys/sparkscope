package com.ucesys.sparkscope.io.report

import com.ucesys.sparkscope.SparkScopeAnalyzer.{BytesInMB, JvmHeapUsed, JvmNonHeapUsed}
import com.ucesys.sparkscope.SparkScopeRunner.SparkScopeSign
import com.ucesys.sparkscope.common.{SparkScopeConf, SparkScopeLogger}
import com.ucesys.sparkscope.data.{DataColumn, DataTable}
import com.ucesys.sparkscope.io.file.TextFileWriter
import com.ucesys.sparkscope.io.report.SeriesColor.{Green, Orange, Purple, Red, Yellow}
import com.ucesys.sparkscope.metrics.SparkScopeResult

import java.io.{FileWriter, InputStream}
import java.nio.file.Paths
import java.time.{Instant, LocalDateTime}
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
          .replace("${appInfo.appName}", sparkScopeConf.appName.getOrElse("None"))
          .replace("${appInfo.applicationId}", result.appContext.appId)
          .replace("${appInfo.start}", ofEpochSecond(result.appContext.appStartTime / 1000, 0, UTC).toString)
          .replace("${appInfo.end}", result.appContext.appEndTime.map(endTime => ofEpochSecond(endTime/ 1000, 0, UTC).toString).getOrElse("In progress"))
          .replace("${appInfo.duration}", durationStr)
          .replace("${logs}", logger.toString)
          .replace("${warnings}", warningsStr)
          .replace("${sparkConf}", sparkScopeConf.sparkConf.getAll.map { case (key, value) => s"${key}: ${value}" }.mkString("\n"))
          .replace("${sparklens}", sparklensResults.mkString("\n"))
        //      .replace("${version}", getClass.getPackage.getImplementationVersion)

        val renderedCharts = renderCharts(rendered, result)
        val renderedStats = renderStats(renderedCharts, result)

        val outputPath = Paths.get(sparkScopeConf.htmlReportPath, s"${result.appContext.appId}.html")
        fileWriter.write(outputPath.toString, renderedStats)
        logger.info(s"Wrote HTML report file to ${outputPath}")
    }

    def renderCharts(template: String, result: SparkScopeResult): String = {
        template
          .replace("${chart.cluster.cpu.util}", result.metrics.clusterCPUMetrics.clusterCpuUsage.select("cpuUsage").mul(100).values.mkString(","))
          .replace(
              "${chart.cluster.cpu.util.timestamps}",
              result.metrics.clusterCPUMetrics.clusterCpuUsage.select("t").values.map(ts => s"'${ofEpochSecond(ts.toLong, 0, UTC)}'").mkString(",")
          )
          .replace("${chart.cluster.cpu.capacity}", result.metrics.clusterCPUMetrics.clusterCapacity.select("totalCores").values.mkString(","))
          .replace("${chart.cluster.cpu.usage}", result.metrics.clusterCPUMetrics.clusterCpuUsageSum.select("cpuUsageAllCores").values.mkString(","))
          .replace(
              "${chart.cluster.cpu.usage.timestamps}",
              result.metrics.clusterCPUMetrics.clusterCpuUsage.select("t").values.map(ts => s"'${ofEpochSecond(ts.toLong, 0, UTC)}'").mkString(",")
          )
          .replace("${chart.jvm.cluster.heap.usage}", result.metrics.clusterMemoryMetrics.heapUsage.select("jvm.heap.usage").mul(100).values.mkString(","))
          .replace(
              "${chart.jvm.cluster.heap.usage.timestamps}",
              result.metrics.clusterMemoryMetrics.heapUsage.select("t").values.map(ts => s"'${ofEpochSecond(ts.toLong, 0, UTC)}'").mkString(",")
          )
          .replace("${chart.jvm.cluster.heap.used}", result.metrics.clusterMemoryMetrics.heapUsed.select("jvm.heap.used").div(BytesInMB).toDouble.mkString(","))
          .replace("${chart.jvm.cluster.heap.max}", result.metrics.clusterMemoryMetrics.heapMax.select("jvm.heap.max").div(BytesInMB).toDouble.mkString(","))
          .replace(
              "${chart.jvm.cluster.heap.timestamps}",
              result.metrics.clusterMemoryMetrics.heapUsed.select("t").values.map(ts => s"'${ofEpochSecond(ts.toLong, 0, UTC)}'").mkString(",")
          )
          .replace(
              "${chart.jvm.executor.heap.timestamps}",
              result.metrics.executorMemoryMetrics.heapUsedMax.select("t").values.map(ts => s"'${ofEpochSecond(ts.toLong, 0, UTC)}'").mkString(",")
          )
          .replace("${chart.jvm.executor.heap}", generateExecutorHeapCharts(result.metrics.executorMemoryMetrics.executorMetricsMap, JvmHeapUsed))
          .replace("${chart.jvm.executor.heap.allocation}", result.metrics.executorMemoryMetrics.heapAllocation.select("jvm.heap.max").div(BytesInMB).toDouble.mkString(","))
          .replace(
              "${chart.jvm.executor.non-heap.timestamps}",
              result.metrics.executorMemoryMetrics.nonHeapUsedMax.select("t").values.map(ts => s"'${ofEpochSecond(ts.toLong, 0, UTC)}'").mkString(",")
          )
          .replace("${chart.jvm.executor.non-heap}", generateExecutorHeapCharts(result.metrics.executorMemoryMetrics.executorMetricsMap, JvmNonHeapUsed))
          .replace("${chart.executor.memoryOverhead}", result.metrics.executorMemoryMetrics.nonHeapUsedMax.addConstColumn("memoryOverhead", sparkScopeConf.executorMemOverhead.toMB.toString).select("memoryOverhead").toDouble.mkString(","))
          .replace(
              "${chart.jvm.driver.heap.timestamps}",
              result.metrics.driverMetrics.select("t").values.map(ts => s"'${ofEpochSecond(ts.toLong, 0, UTC)}'").mkString(",")
          )
          .replace("${chart.jvm.driver.heap.size}", result.metrics.driverMetrics.select("jvm.heap.max").div(BytesInMB).toDouble.mkString(","))
          .replace("${chart.jvm.driver.heap.used}", result.metrics.driverMetrics.select("jvm.heap.used").div(BytesInMB).toDouble.mkString(","))
          .replace(
              "${chart.jvm.driver.non-heap.timestamps}",
              result.metrics.driverMetrics.select("t").values.map(ts => s"'${ofEpochSecond(ts.toLong, 0, UTC)}'").mkString(",")
          )
          .replace("${chart.jvm.driver.non-heap.used}", result.metrics.driverMetrics.select("jvm.non-heap.used").div(BytesInMB).toDouble.mkString(","))
          .replace("${chart.driver.memoryOverhead}", result.metrics.driverMetrics.addConstColumn("memoryOverhead", sparkScopeConf.driverMemOverhead.toMB.toString).select("memoryOverhead").toDouble.mkString(","))
          .replace("${chart.cluster.numExecutors.timestamps}", result.metrics.clusterCPUMetrics.numExecutors.select("t").values.map(ts => s"'${ofEpochSecond(ts.toLong, 0, UTC)}'").mkString(","))
          .replace("${chart.cluster.numExecutors}", result.metrics.clusterCPUMetrics.numExecutors.select("cnt").toDouble.mkString(","))
          .replace("${chart.stages.timestamps}", result.metrics.stageTimeline.select("t").values.map(ts => s"'${LocalDateTime.ofInstant(Instant.ofEpochMilli(ts.toLong), UTC)}'").mkString(","))
          .replace("${chart.stages}", generateStages(result.metrics.stageTimeline.columns.filter(_.name !="t")))
    }


    def generateStages(stageCol: Seq[DataColumn]): String = {
        stageCol.map(generateStageData).mkString("[", ",", "]")
    }

    def generateStageData(stageCol: DataColumn): String = {
        val color = SeriesColor.randomColorModulo(stageCol.name.toInt)
        s"""{
          |             data: [${stageCol.toDouble.mkString(",")}],
          |             label: "stageId=${stageCol.name}",
          |             borderColor: "${color.borderColor}",
          |             backgroundColor: "${color.backgroundColor}",
          |             lineTension: 0.0,
          |             fill: true,
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
          |}""".stripMargin
    }

    def renderStats(template: String, result: SparkScopeResult): String = {
        template
          .replace("${stats.cluster.heap.avg.perc}", f"${result.stats.clusterMemoryStats.avgHeapPerc * 100}%1.2f")
          .replace("${stats.cluster.heap.max.perc}", f"${result.stats.clusterMemoryStats.maxHeapPerc * 100}%1.2f")
          .replace("${stats.cluster.heap.waste.perc}", f"${100 - result.stats.clusterMemoryStats.avgHeapPerc * 100}%1.2f")
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
