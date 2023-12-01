package com.ucesys.sparkscope.view.chart

import com.ucesys.sparkscope.data.DataColumn

case class LimitedChart(tsCol: DataColumn, valueCol: DataColumn, limitCol: DataColumn) extends Chart {
    def labels: Seq[String] = tsCol.values.map(tsToDt)
    def values: Seq[String] = valueCol.values
    def limits: Seq[String] = limitCol.values
}
