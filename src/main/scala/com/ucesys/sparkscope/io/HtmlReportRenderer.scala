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
      .replace("${summary}", result.summary)
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

    val outputPath = outputDir + result.applicationId + ".html"
    val fileWriter = new FileWriter(outputPath)
    fileWriter.write(rendered)
    fileWriter.close()
    println(s"Wrote HTML report file to ${outputPath}")
  }
}
