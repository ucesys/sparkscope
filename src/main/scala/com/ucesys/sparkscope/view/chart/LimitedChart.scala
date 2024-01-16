/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.ucesys.sparkscope.view.chart

import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.data.DataColumn
import com.ucesys.sparkscope.io.report.HtmlFileReporter.MaxChartPoints

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
