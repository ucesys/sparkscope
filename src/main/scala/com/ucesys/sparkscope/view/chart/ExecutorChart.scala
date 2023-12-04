package com.ucesys.sparkscope.view.chart

import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.data.DataColumn
import com.ucesys.sparkscope.view.HtmlReportGenerator.MaxExecutorChartPoints
import com.ucesys.sparkscope.view.SeriesColor
import com.ucesys.sparkscope.view.SeriesColor._
import com.ucesys.sparkscope.view.chart.ChartUtils.decreaseDataPoints

case class ExecutorChart(timestamps: Seq[String], limits: Seq[String], datasets: String) extends Chart {
    def labels: Seq[String] = timestamps.map(tsToDt)
}

object ExecutorChart {
    case class TimePoint(ts: String, value: String, limit: String)

    def apply(tsCol: DataColumn, limitsCol: DataColumn, executorCols: Seq[DataColumn])(implicit logger: SparkScopeLogger): ExecutorChart = {
        if (executorCols.exists(_.size != tsCol.size)) {
            throw new IllegalArgumentException(s"Executor series sizes are different: ${Seq(tsCol.size) ++ executorCols.map(_.size)}")
        } else if (executorCols.map(_.size).sum <= MaxExecutorChartPoints) {
            logger.info(s"Number of total executor data points is less than maximum. Rendering all data points. ${executorCols.map(_.size).sum} < ${MaxExecutorChartPoints}")
            ExecutorChart(tsCol.values, limitsCol.values, executorCols.map(generateChart).mkString(","))
        } else {
            logger.info(s"Decreasing total number of rendered data points for all executor charts from ${executorCols.map(_.size).sum} to ${MaxExecutorChartPoints}")
            val desiredSingleChartPoints: Int = MaxExecutorChartPoints / executorCols.length
            logger.info(s"Decreasing number of rendered data points per executor chart from ${executorCols.headOption.map(_.size).getOrElse(0)} to ${desiredSingleChartPoints}")

            decreaseDataPoints(tsCol, executorCols, desiredSingleChartPoints) match {
                case (newTsCol, newCols) => ExecutorChart(newTsCol.values, limitsCol.values, newCols.map(generateChart).mkString(","))
            }
        }
    }

    def generateChart(col: DataColumn): String = {
        val color = SeriesColor.randomColorModulo(col.name.toInt, Seq(Green, Red, Yellow, Purple, Orange))
        s"""{
           |             data: [${col.values.mkString(",")}],
           |             borderColor: "${color.borderColor}",
           |             backgroundColor: "${color.backgroundColor}",
           |             pointRadius: 1,
           |             pointHoverRadius: 8,
           |             label: "executorId=${col.name}",
           |             fill: false,
           |}""".stripMargin
    }
}
