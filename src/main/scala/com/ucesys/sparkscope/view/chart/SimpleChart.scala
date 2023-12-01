package com.ucesys.sparkscope.view.chart

import com.ucesys.sparkscope.data.DataColumn

case class SimpleChart(tsCol: DataColumn, valueCol: DataColumn) extends Chart {
    def labels: Seq[String] = tsCol.values.map(tsToDt)
    def values: Seq[String] = valueCol.values
}
