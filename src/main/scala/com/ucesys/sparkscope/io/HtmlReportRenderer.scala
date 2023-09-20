package com.ucesys.sparkscope.io

import com.ucesys.sparkscope.ExecutorMetricsAnalyzer.BytesInMB
import com.ucesys.sparkscope.SparkScopeResult

import java.io.{FileWriter, InputStream}
import java.time.LocalDateTime.ofEpochSecond
import java.time.ZoneOffset.UTC
import scala.io.Source

object HtmlReportRenderer {
  def render(result: SparkScopeResult, outputDir: String): Unit = {
    val summaryHtml = result.summary.replace("\n", "<br>\n")
    val logsHtml = result.logs.replace("\n", "<br>\n")
//    val url = getClass.getResource("report-template.html")
//    val template = Source.fromURL(url).mkString
//    val template = Source.fromFile("report-template.html").mkString
    val stream: InputStream = getClass.getResourceAsStream("/report-template.html")
    val template: String = scala.io.Source.fromInputStream(stream).getLines().mkString


    val rendered = template
      .replace("${applicationId}", result.applicationId)
      .replace("${summary}", summaryHtml)
      .replace("${logs}", logsHtml)
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
    val outputPath = outputDir + result.applicationId + ".html"
    val fileWriter = new FileWriter(outputPath)
    fileWriter.write(rendered)
    fileWriter.close()
    println(s"Wrote HTML report file to ${outputPath}")
  }
}
