package com.ucesys.sparkscope.view.chart

import com.ucesys.sparkscope.common.MetricUtils.ColTs
import com.ucesys.sparkscope.data.DataColumn

object ChartUtils {
    def decreaseDataPoints(tsCol: DataColumn, valueCols: Seq[DataColumn], targetChartPoints: Int): (DataColumn, Seq[DataColumn]) = {
            val originalChartPoints: Long = valueCols.headOption.getOrElse(throw new IllegalArgumentException("Column list cannot be empty!")).size
            val ratio = originalChartPoints.toFloat / targetChartPoints.toFloat
            val newPoints: Seq[Int] = (0 until targetChartPoints)

            val newCols = valueCols.map{ oldCol =>
                val values: Seq[String] = newPoints.map { id =>
                    val from: Int = (id * ratio).toInt
                    val to: Int = (from + ratio).toInt
                    val valuesNotNull = oldCol.values.slice(from, to).filter(_ != "null")

                    valuesNotNull match {
                        case Seq() => "null"
                        case _ => valuesNotNull.map(_.toDouble.toLong).max.toString
                    }
                }
                DataColumn(oldCol.name, values)
            }
        (DataColumn(ColTs, newPoints.map(id => tsCol.values((id * ratio).toInt))), newCols)
    }
}
