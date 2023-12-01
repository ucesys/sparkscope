package com.ucesys.sparkscope.view.chart

import com.ucesys.sparkscope.data.DataColumn

case class TimePoint(ts: String, value: String)

case class SimpleChart(timestamps: Seq[String], values: Seq[String]) extends Chart {
    def labels: Seq[String] = timestamps.map(tsToDt)
}

object SimpleChart {
    def apply(tsCol: DataColumn, valueCol: DataColumn, maxPoints: Int=300): SimpleChart = {
        if(tsCol.size != valueCol.size) {
            throw new IllegalArgumentException(s"Series sizes are different: ${tsCol.size} and ${valueCol.size}")
        } else if (tsCol.size <= maxPoints) {
            SimpleChart(tsCol.values, valueCol.values)
        } else {
            val ratio: Float = tsCol.size.toFloat / maxPoints.toFloat
            val newPoints: Seq[Int] = (0 until maxPoints)
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
