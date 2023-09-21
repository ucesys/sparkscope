package com.ucesys.sparkscope.io

import com.ucesys.sparkscope.ExecutorMetricsAnalyzer.BytesInMB
import com.ucesys.sparkscope.SparkScopeResult

import java.io.{FileWriter, InputStream}
import java.time.LocalDateTime.ofEpochSecond
import java.time.ZoneOffset.UTC

object HtmlReportRenderer {
  def render(result: SparkScopeResult, outputDir: String, sparklensResults: Seq[String]): Unit = {
    val stream: InputStream = getClass.getResourceAsStream("/report-template.html")
    val template: String = scala.io.Source.fromInputStream(stream).getLines().mkString("\n")

    val rendered = template
      .replace("${applicationId}", result.applicationId)
      .replace("${logs}", result.logs)
      .replace("${sparklens}", sparklensResults.mkString("\n"))
      .replace("${chart.jvm.cluster.heap.usage}", result.clusterMetrics.heapUsage.select("jvm.heap.usage").values.mkString(","))
      .replace(
        "${chart.jvm.cluster.heap.usage.timestamps}",
        result.clusterMetrics.heapUsage.select("t").values.map(ts => s"'${ofEpochSecond(ts.toLong, 0, UTC)}'").mkString(",")
      )
      .replace("${chart.jvm.cluster.heap.used}", result.clusterMetrics.heapUsed.select("jvm.heap.used").div(BytesInMB).mkString(","))
      .replace("${chart.jvm.cluster.heap.max}", result.clusterMetrics.heapMax.select("jvm.heap.max").div(BytesInMB).mkString(","))
      .replace(
        "${chart.jvm.cluster.heap.timestamps}",
        result.clusterMetrics.heapUsed.select("t").values.map(ts => s"'${ofEpochSecond(ts.toLong, 0, UTC)}'").mkString(",")
      )
      .replace(
        "${chart.jvm.executor.heap.timestamps}",
        result.executorMetrics.heapUsedMax.select("t").values.map(ts => s"'${ofEpochSecond(ts.toLong, 0, UTC)}'").mkString(",")
      )
      .replace("${chart.jvm.executor.heap.max}", result.executorMetrics.heapUsedMax.select("jvm.heap.used").div(BytesInMB).mkString(","))
      .replace("${chart.jvm.executor.heap.min}", result.executorMetrics.heapUsedMin.select("jvm.heap.used").div(BytesInMB).mkString(","))
      .replace("${chart.jvm.executor.heap.avg}", result.executorMetrics.heapUsedAvg.select("jvm.heap.used").div(BytesInMB).mkString(","))
      .replace("${chart.jvm.executor.heap.allocation}", result.executorMetrics.heapAllocation.select("jvm.heap.max").div(BytesInMB).mkString(","))
      .replace(
        "${chart.jvm.executor.non-heap.timestamps}",
        result.executorMetrics.nonHeapUsedMax.select("t").values.map(ts => s"'${ofEpochSecond(ts.toLong, 0, UTC)}'").mkString(",")
      )
      .replace("${chart.jvm.executor.non-heap.max}", result.executorMetrics.nonHeapUsedMax.select("jvm.non-heap.used").div(BytesInMB).mkString(","))
      .replace("${chart.jvm.executor.non-heap.min}", result.executorMetrics.nonHeapUsedMin.select("jvm.non-heap.used").div(BytesInMB).mkString(","))
      .replace("${chart.jvm.executor.non-heap.avg}", result.executorMetrics.nonHeapUsedAvg.select("jvm.non-heap.used").div(BytesInMB).mkString(","))

    val renderedStats = renderStats(rendered, result)
    val outputPath = outputDir + result.applicationId + ".html"
    val fileWriter = new FileWriter(outputPath)
    fileWriter.write(renderedStats)
    fileWriter.close()
    println(s"Wrote HTML report file to ${outputPath}")
  }

  def renderStats(template: String, result: SparkScopeResult): String = {
    template
      .replace("${stats.cluster.heap.avg.perc}", f"${result.stats.clusterStats.avgHeapPerc}%1.2f")
      .replace("${stats.cluster.heap.max.perc}", f"${result.stats.clusterStats.maxHeapPerc}%1.2f")
      .replace("${stats.cluster.heap.avg}", result.stats.clusterStats.avgHeap.toString)
      .replace("${stats.cluster.heap.max}", result.stats.clusterStats.maxHeap.toString)

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
