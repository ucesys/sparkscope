package com.ucesys.sparkscope.view.chart

import com.ucesys.sparkscope.common.MemorySize.BytesInMB
import com.ucesys.sparkscope.common.MetricUtils.ColTs
import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.data.{DataColumn, DataTable}
import com.ucesys.sparkscope.view.SeriesColor
import com.ucesys.sparkscope.view.SeriesColor._

case class ExecutorChart(timestamps: Seq[String], limits: Seq[String], datasets: String) extends Chart {
    def labels: Seq[String] = timestamps.map(tsToDt)
}

object ExecutorChart {
    case class TimePoint(ts: String, value: String, limit: String)

    def apply(tsCol: DataColumn,
              limitsCol: DataColumn,
              executorMetricsMap: Map[String, DataTable],
              metricName: String)
             (implicit logger: SparkScopeLogger): ExecutorChart = {
        val datasetsStr = generateExecutorHeapCharts(executorMetricsMap, metricName)
        ExecutorChart(tsCol.values, limitsCol.values, datasetsStr)
//        if(tsCol.size != valueCol.size) {
//            throw new IllegalArgumentException(s"Series sizes are different: ${tsCol.size} and ${valueCol.size}")
//        } else if (tsCol.size <= MaxChartPoints) {
//            StageChart(tsCol.values, valueCol.values)
//        } else {
//            logger.info(s"Limiting chart points from ${tsCol.size} to ${MaxChartPoints}")
//            val ratio: Float = tsCol.size.toFloat / MaxChartPoints.toFloat
//            val newPoints: Seq[Int] = (0 until MaxChartPoints)
//            val newChart: Seq[TimePoint] = newPoints.map{ id =>
//                val from: Int = (id*ratio).toInt
//                val to: Int = (from + ratio).toInt
//                val values = valueCol.values.slice(from, to).map(_.toDouble.toLong)
//                val avg = values.sum/values.length
//                TimePoint(tsCol.values(from), avg.toString)
//            }
//            StageChart(newChart.map(_.ts), newChart.map(_.value))
//        }
    }

    def generateExecutorHeapCharts(executorMetricsMap: Map[String, DataTable], metricName: String): String = {
        executorMetricsMap.map { case (id, metrics) => generateExecutorChart(id, metrics.select(metricName).div(BytesInMB)) }.mkString(",")
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
}
