package com.ucesys.sparkscope.io

import com.ucesys.sparkscope.SparkScopeAnalyzer.BytesInMB
import com.ucesys.sparkscope.metrics.SparkScopeResult
import org.apache.spark.SparkConf

import java.io.{FileWriter, InputStream}
import java.time.LocalDateTime.ofEpochSecond
import java.time.ZoneOffset.UTC
import scala.concurrent.duration._

object HtmlReportGenerator {
  def generateHtml(result: SparkScopeResult, outputDir: String, sparklensResults: Seq[String], sparkConf: SparkConf): Unit = {
    val stream: InputStream = getClass.getResourceAsStream("/report-template.html")
    val template: String = scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
    val duration = (result.appInfo.endTime-result.appInfo.startTime).milliseconds
    val durationStr = duration match {
      case duration if duration < 1.minutes => s"${duration.toSeconds.toString}s"
      case duration if duration < 1.hours => s"${duration.toMinutes%60}min ${duration.toSeconds%60}s"
      case _ => s"${duration.toHours}h ${duration.toMinutes%60}min ${duration.toSeconds%60}s"
    }

    val warningsStr: String = result.warnings match {
      case Seq() => ""
      case warnings => "WARNINGS FOUND:\n- " + warnings.mkString("\n- ")
    }

    val rendered = template
      .replace("${appInfo.applicationId}", result.appInfo.applicationID)
      .replace("${appInfo.start}", ofEpochSecond(result.appInfo.startTime/1000, 0, UTC).toString)
      .replace("${appInfo.end}", ofEpochSecond(result.appInfo.endTime/1000, 0, UTC).toString)
      .replace("${appInfo.duration}", durationStr)
      .replace("${logs}", result.logs)
      .replace("${warnings}", warningsStr)
      .replace("${sparkConf}", sparkConf.getAll.map{case (key, value) => s"${key}: ${value}"}.mkString("\n"))
      .replace("${sparklens}", sparklensResults.mkString("\n"))
//      .replace("${version}", getClass.getPackage.getImplementationVersion)

    val renderedCharts = renderCharts(rendered, result)
    val renderedStats = renderStats(renderedCharts, result)

    val outputPath = s"${outputDir}/${result.appInfo.applicationID}.html"
    val fileWriter = new FileWriter(outputPath)
    fileWriter.write(renderedStats)
    fileWriter.close()
    println(s"Wrote HTML report file to ${outputPath}")
  }

  def renderCharts(template: String, result: SparkScopeResult): String = {
    template
      .replace("${chart.cluster.cpu.usage}", result.metrics.clusterCPUMetrics.clusterCpuUsage.select("cpuUsage").mul(100).values.mkString(","))
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
      .replace("${chart.jvm.executor.heap.max}", result.metrics.executorMemoryMetrics.heapUsedMax.select("jvm.heap.used").div(BytesInMB).toDouble.mkString(","))
      .replace("${chart.jvm.executor.heap.avg}", result.metrics.executorMemoryMetrics.heapUsedAvg.select("jvm.heap.used").div(BytesInMB).toDouble.mkString(","))
      .replace("${chart.jvm.executor.heap.allocation}", result.metrics.executorMemoryMetrics.heapAllocation.select("jvm.heap.max").div(BytesInMB).toDouble.mkString(","))
      .replace(
        "${chart.jvm.executor.non-heap.timestamps}",
        result.metrics.executorMemoryMetrics.nonHeapUsedMax.select("t").values.map(ts => s"'${ofEpochSecond(ts.toLong, 0, UTC)}'").mkString(",")
      )
      .replace("${chart.jvm.executor.non-heap.max}", result.metrics.executorMemoryMetrics.nonHeapUsedMax.select("jvm.non-heap.used").div(BytesInMB).toDouble.mkString(","))
      .replace("${chart.jvm.executor.non-heap.avg}", result.metrics.executorMemoryMetrics.nonHeapUsedAvg.select("jvm.non-heap.used").div(BytesInMB).toDouble.mkString(","))
      .replace(
        "${chart.jvm.driver.heap.timestamps}",
        result.metrics.driverMetrics.select("t").values.map(ts => s"'${ofEpochSecond(ts.toLong, 0, UTC)}'").mkString(",")
      )
      .replace("${chart.jvm.driver.heap.size}", result.metrics.driverMetrics.select("jvm.heap.max").div(BytesInMB).toDouble.mkString(","))
      .replace("${chart.jvm.driver.heap.used}", result.metrics.driverMetrics.select("jvm.heap.used").div(BytesInMB).toDouble.mkString(","))
  }

  def renderStats(template: String, result: SparkScopeResult): String = {
    template
      .replace("${stats.cluster.heap.avg.perc}", f"${result.stats.clusterMemoryStats.avgHeapPerc*100}%1.2f")
      .replace("${stats.cluster.heap.max.perc}", f"${result.stats.clusterMemoryStats.maxHeapPerc*100}%1.2f")
      .replace("${stats.cluster.heap.waste.perc}", f"${100 - result.stats.clusterMemoryStats.avgHeapPerc*100}%1.2f")
      .replace("${stats.cluster.cpu.util}", f"${result.stats.clusterCPUStats.cpuUtil*100}%1.2f")
      .replace("${resource.waste.heap}", f"${result.stats.clusterMemoryStats.heapGbHoursWasted}%1.4f")
      .replace("${resource.waste.cpu}", f"${result.stats.clusterCPUStats.coreHoursWasted}%1.4f")

      .replace("${stats.executor.heap.max}", result.stats.executorStats.maxHeap.toString)
      .replace("${stats.executor.heap.max.perc}", f"${result.stats.executorStats.maxHeapPerc*100}%1.2f")
      .replace("${stats.executor.heap.avg}", result.stats.executorStats.avgHeap.toString)
      .replace("${stats.executor.heap.avg.perc}", f"${result.stats.executorStats.avgHeapPerc*100}%1.2f")
      .replace("${stats.executor.non-heap.avg}", result.stats.executorStats.avgNonHeap.toString)
      .replace("${stats.executor.non-heap.max}", result.stats.executorStats.maxNonHeap.toString)

      .replace("${stats.driver.heap.max}", result.stats.driverStats.maxHeap.toString)
      .replace("${stats.driver.heap.max.perc}", f"${result.stats.driverStats.maxHeapPerc*100}%1.2f")
      .replace("${stats.driver.heap.avg}", result.stats.driverStats.avgHeap.toString)
      .replace("${stats.driver.heap.avg.perc}", f"${result.stats.driverStats.avgHeapPerc*100}%1.2f")
      .replace("${stats.driver.non-heap.avg}", result.stats.driverStats.avgNonHeap.toString)
      .replace("${stats.driver.non-heap.max}", result.stats.driverStats.maxNonHeap.toString)
  }
}
