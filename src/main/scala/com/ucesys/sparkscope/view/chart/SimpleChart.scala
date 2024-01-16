/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

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
