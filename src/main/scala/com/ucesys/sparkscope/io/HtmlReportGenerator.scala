package com.ucesys.sparkscope.io

import com.ucesys.sparkscope.ExecutorMetricsAnalyzer.BytesInMB
import com.ucesys.sparkscope.SparkScopeResult

import java.io.{FileWriter, InputStream}
import java.time.LocalDateTime.ofEpochSecond
import java.time.ZoneOffset.UTC
import scala.concurrent.duration._
object HtmlReportGenerator {
  def render(result: SparkScopeResult, outputDir: String, sparklensResults: Seq[String]): Unit = {
    val stream: InputStream = getClass.getResourceAsStream("/report-template.html")
    val template: String = scala.io.Source.fromInputStream(stream).getLines().mkString("\n")
    val duration = (result.appInfo.endTime-result.appInfo.startTime).milliseconds
    val durationStr = duration match {
      case duration if duration < 1.minutes => s"${duration.toSeconds.toString}s"
      case duration if duration < 1.hours => s"${duration.toMinutes%60}min ${duration.toSeconds%60}s"
      case _ => s"${duration.toHours}h ${duration.toMinutes%60}min ${duration.toSeconds%60}s"
    }

    val rendered = template
      .replace("${appInfo.applicationId}", result.appInfo.applicationID)
      .replace("${appInfo.start}", ofEpochSecond(result.appInfo.startTime/1000, 0, UTC).toString)
      .replace("${appInfo.end}", ofEpochSecond(result.appInfo.endTime/1000, 0, UTC).toString)
      .replace("${appInfo.duration}", durationStr)
      .replace("${logs}", result.logs)
      .replace("${sparkConf}", result.sparkConf.getAll.map{case (key, value) => s"${key}: ${value}"}.mkString("\n"))
      .replace("${sparklens}", sparklensResults.mkString("\n"))
//      .replace("${version}", getClass.getPackage.getImplementationVersion)

    val renderedCharts = renderCharts(rendered, result)
    val renderedStats = renderStats(renderedCharts, result)

    val outputPath = outputDir + result.appInfo.applicationID + ".html"
    val fileWriter = new FileWriter(outputPath)
    fileWriter.write(renderedStats)
    fileWriter.close()
    println(s"Wrote HTML report file to ${outputPath}")
  }

  def renderCharts(template: String, result: SparkScopeResult): String = {
    template
      .replace("${chart.cluster.cpu.usage}", result.clusterMetrics.cpuUsage.select("cpuUsage").values.mkString(","))
      .replace(
        "${chart.cluster.cpu.usage.timestamps}",
        result.clusterMetrics.cpuUsage.select("t").values.map(ts => s"'${ofEpochSecond(ts.toLong, 0, UTC)}'").mkString(",")
      )
      .replace("${chart.jvm.cluster.heap.usage}", result.clusterMetrics.heapUsage.select("jvm.heap.usage").values.mkString(","))
      .replace(
        "${chart.jvm.cluster.heap.usage.timestamps}",
        result.clusterMetrics.heapUsage.select("t").values.map(ts => s"'${ofEpochSecond(ts.toLong, 0, UTC)}'").mkString(",")
      )
      .replace("${chart.jvm.cluster.heap.used}", result.clusterMetrics.heapUsed.select("jvm.heap.used").div(BytesInMB).toDouble.mkString(","))
      .replace("${chart.jvm.cluster.heap.max}", result.clusterMetrics.heapMax.select("jvm.heap.max").div(BytesInMB).toDouble.mkString(","))
      .replace(
        "${chart.jvm.cluster.heap.timestamps}",
        result.clusterMetrics.heapUsed.select("t").values.map(ts => s"'${ofEpochSecond(ts.toLong, 0, UTC)}'").mkString(",")
      )
      .replace(
        "${chart.jvm.executor.heap.timestamps}",
        result.executorMetrics.heapUsedMax.select("t").values.map(ts => s"'${ofEpochSecond(ts.toLong, 0, UTC)}'").mkString(",")
      )
      .replace("${chart.jvm.executor.heap.max}", result.executorMetrics.heapUsedMax.select("jvm.heap.used").div(BytesInMB).toDouble.mkString(","))
      .replace("${chart.jvm.executor.heap.min}", result.executorMetrics.heapUsedMin.select("jvm.heap.used").div(BytesInMB).toDouble.mkString(","))
      .replace("${chart.jvm.executor.heap.avg}", result.executorMetrics.heapUsedAvg.select("jvm.heap.used").div(BytesInMB).toDouble.mkString(","))
      .replace("${chart.jvm.executor.heap.allocation}", result.executorMetrics.heapAllocation.select("jvm.heap.max").div(BytesInMB).toDouble.mkString(","))
      .replace(
        "${chart.jvm.executor.non-heap.timestamps}",
        result.executorMetrics.nonHeapUsedMax.select("t").values.map(ts => s"'${ofEpochSecond(ts.toLong, 0, UTC)}'").mkString(",")
      )
      .replace("${chart.jvm.executor.non-heap.max}", result.executorMetrics.nonHeapUsedMax.select("jvm.non-heap.used").div(BytesInMB).toDouble.mkString(","))
      .replace("${chart.jvm.executor.non-heap.min}", result.executorMetrics.nonHeapUsedMin.select("jvm.non-heap.used").div(BytesInMB).toDouble.mkString(","))
      .replace("${chart.jvm.executor.non-heap.avg}", result.executorMetrics.nonHeapUsedAvg.select("jvm.non-heap.used").div(BytesInMB).toDouble.mkString(","))
      .replace(
        "${chart.jvm.driver.heap.timestamps}",
        result.driverMetrics.select("t").values.map(ts => s"'${ofEpochSecond(ts.toLong, 0, UTC)}'").mkString(",")
      )
      .replace("${chart.jvm.driver.heap.size}", result.driverMetrics.select("jvm.heap.max").div(BytesInMB).toDouble.mkString(","))
      .replace("${chart.jvm.driver.heap.used}", result.driverMetrics.select("jvm.heap.used").div(BytesInMB).toDouble.mkString(","))
  }

  def renderStats(template: String, result: SparkScopeResult): String = {
    template
      .replace("${stats.cluster.heap.avg.perc}", f"${result.stats.clusterStats.avgHeapPerc*100}%1.2f")
      .replace("${stats.cluster.heap.max.perc}", f"${result.stats.clusterStats.maxHeapPerc}%1.2f")
      .replace("${stats.cluster.heap.waste.perc}", f"${100 - result.stats.clusterStats.avgHeapPerc*100}%1.2f")
      .replace("${stats.cluster.cpu.util}", f"${result.stats.clusterStats.totalCpuUtil*100}%1.2f")
      .replace("${resource.waste.heap}", f"${result.resourceWasteMetrics.heapGbHoursWasted}%1.4f")
      .replace("${resource.waste.cpu}", f"${result.resourceWasteMetrics.coreHoursWasted}%1.4f")

      .replace("${stats.executor.heap.max}", result.stats.executorStats.maxHeap.toString)
      .replace("${stats.executor.heap.max.perc}", f"${result.stats.executorStats.maxHeapPerc}%1.2f")
      .replace("${stats.executor.heap.avg}", result.stats.executorStats.avgHeap.toString)
      .replace("${stats.executor.heap.avg.perc}", f"${result.stats.executorStats.avgHeapPerc}%1.2f")
      .replace("${stats.executor.non-heap.avg}", result.stats.executorStats.avgNonHeap.toString)
      .replace("${stats.executor.non-heap.max}", result.stats.executorStats.maxNonHeap.toString)

      .replace("${stats.driver.heap.max}", result.stats.driverStats.maxHeap.toString)
      .replace("${stats.driver.heap.max.perc}", f"${result.stats.driverStats.maxHeapPerc}%1.2f")
      .replace("${stats.driver.heap.avg}", result.stats.driverStats.avgHeap.toString)
      .replace("${stats.driver.heap.avg.perc}", f"${result.stats.driverStats.avgHeapPerc}%1.2f")
      .replace("${stats.driver.non-heap.avg}", result.stats.driverStats.avgNonHeap.toString)
      .replace("${stats.driver.non-heap.max}", result.stats.driverStats.maxNonHeap.toString)
  }
}
