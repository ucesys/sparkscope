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
import com.ucesys.sparkscope.io.report.HtmlFileReporter.MaxExecutorChartPoints
import com.ucesys.sparkscope.view.SeriesColor
import com.ucesys.sparkscope.view.SeriesColor._
import com.ucesys.sparkscope.view.chart.ChartUtils.decreaseDataPoints
import com.ucesys.sparkscope.view.chart.ExecutorChart.generateChart

case class ExecutorChart(timestamps: Seq[String], limits: Seq[String], executorData: Map[Int, Seq[String]]) extends Chart {
    def labels: Seq[String] = timestamps.map(tsToDt)
    def datasets: String = executorData.map{case(executorId, data) => generateChart(executorId, data)}.mkString(",")
}

object ExecutorChart {
    case class TimePoint(ts: String, value: String, limit: String)

    def apply(tsCol: DataColumn, limitsCol: DataColumn, executorCols: Seq[DataColumn])(implicit logger: SparkScopeLogger): ExecutorChart = {
        if (executorCols.exists(_.size != tsCol.size)) {
            throw new IllegalArgumentException(s"Executor series sizes are different: ${Seq(tsCol.size) ++ executorCols.map(_.size)}")
        } else if (executorCols.map(_.size).sum <= MaxExecutorChartPoints) {
            logger.info(s"Number of total executor data points is less than maximum. Rendering all data points. ${executorCols.map(_.size).sum} < ${MaxExecutorChartPoints}", this.getClass)
            ExecutorChart(tsCol.values, limitsCol.values, executorCols.map(col => (col.name.toInt, col.values)).toMap)
        } else {
            logger.info(s"Decreasing total number of rendered data points for all executor charts from ${executorCols.map(_.size).sum} to ${MaxExecutorChartPoints}", this.getClass)
            val desiredSingleChartPoints: Int = MaxExecutorChartPoints / executorCols.length
            logger.info(s"Decreasing number of rendered data points per executor chart from ${executorCols.headOption.map(_.size).getOrElse(0)} to ${desiredSingleChartPoints}", this.getClass)

            decreaseDataPoints(tsCol, executorCols, desiredSingleChartPoints) match {
                case (newTsCol, newCols) => ExecutorChart(
                    newTsCol.values,
                    limitsCol.values,
                    newCols.map(col => (col.name.toInt, col.values)).toMap
                )
            }
        }
    }

    def generateChart(executorId: Int, data: Seq[String]): String = {
        val color = SeriesColor.randomColorModulo(executorId, Seq(Green, Red, Yellow, Purple, Orange))
        s"""{
           |             data: [${data.mkString(",")}],
           |             borderColor: "${color.borderColor}",
           |             backgroundColor: "${color.backgroundColor}",
           |             pointRadius: 1,
           |             pointHoverRadius: 8,
           |             label: "executorId=${executorId}",
           |             fill: false,
           |}""".stripMargin
    }
}
