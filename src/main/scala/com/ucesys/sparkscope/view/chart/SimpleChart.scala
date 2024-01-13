package com.ucesys.sparkscope.view.chart

import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.data.DataColumn
import com.ucesys.sparkscope.io.report.HtmlFileReporter.MaxChartPoints

case class SimpleChart(timestamps: Seq[String], values: Seq[String]) extends Chart {
    def labels: Seq[String] = timestamps.map(tsToDt)
}

object SimpleChart {
    case class TimePoint(ts: String, value: String)

    def apply(tsCol: DataColumn, valueCol: DataColumn)(implicit logger: SparkScopeLogger): SimpleChart = {
        if(tsCol.size != valueCol.size) {
            throw new IllegalArgumentException(s"Series sizes are different: ${tsCol.size} and ${valueCol.size}")
        } else if (tsCol.size <= MaxChartPoints) {
            SimpleChart(tsCol.values, valueCol.values)
        } else {
            logger.info(s"Limiting chart points from ${tsCol.size} to ${MaxChartPoints}", this.getClass)
            val ratio: Float = tsCol.size.toFloat / MaxChartPoints.toFloat
            val newPoints: Seq[Int] = (0 until MaxChartPoints)
            val newChart: Seq[TimePoint] = newPoints.map{ id =>
                val from: Int = (id*ratio).toInt
                val to: Int = (from + ratio).toInt
                val values = valueCol.values.slice(from, to).map(_.toDouble.toLong)
                val avg = values.sum/values.length
                TimePoint(tsCol.values(from), avg.toString)
            }
            SimpleChart(newChart.map(_.ts), newChart.map(_.value))
        }
    }
}
