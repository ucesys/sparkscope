package com.ucesys.sparkscope.view.chart

import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.data.DataColumn
import com.ucesys.sparkscope.report.HtmlFileReporter.MaxChartPoints

case class LimitedChart(timestamps: Seq[String], values: Seq[String], limits: Seq[String]) extends Chart {
    def labels: Seq[String] = timestamps.map(tsToDt)
}

object LimitedChart {
    case class TimePoint(ts: String, value: String, limit: String)

    def apply(tsCol: DataColumn, valueCol: DataColumn, limitCol: DataColumn)(implicit logger: SparkScopeLogger): LimitedChart = {
        if(tsCol.size != valueCol.size || valueCol.size != limitCol.size) {
            throw new IllegalArgumentException(s"Series sizes are different: [${tsCol.size}, ${valueCol.size}, ${limitCol.size}]")
        } else if (tsCol.size <= MaxChartPoints) {
            LimitedChart(tsCol.values, valueCol.values, limitCol.values)
        } else {
            logger.info(s"Limiting chart points from ${tsCol.size} to ${MaxChartPoints}", this.getClass)
            val ratio: Float = tsCol.size.toFloat / MaxChartPoints.toFloat
            val newPoints: Seq[Int] = (0 until MaxChartPoints)
            val newChart: Seq[TimePoint] = newPoints.map{ id =>
                val from: Int = (id*ratio).toInt
                val to: Int = (from + ratio).toInt
                val values = valueCol.values.slice(from, to).map(_.toDouble.toLong)
                val valuesAvg = values.sum/values.length
                val limits = limitCol.values.slice(from, to).map(_.toDouble.toLong)
                val limitsAvg = limits.sum / limits.length
                TimePoint(tsCol.values(from), valuesAvg.toString, limitsAvg.toString)
            }
            LimitedChart(newChart.map(_.ts), newChart.map(_.value), newChart.map(_.limit))
        }
    }
}
