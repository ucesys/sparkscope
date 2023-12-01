package com.ucesys.sparkscope.view.chart

import com.ucesys.sparkscope.common.MetricUtils.ColTs
import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.data.{DataColumn, DataTable}
import com.ucesys.sparkscope.view.HtmlReportGenerator.MaxStageChartPoints
import com.ucesys.sparkscope.view.SeriesColor
import com.ucesys.sparkscope.view.SeriesColor._
import com.ucesys.sparkscope.view.chart.ChartUtils.decreaseDataPoints

case class StageChart(timestamps: Seq[String], datasets: String) extends Chart {
    def labels: Seq[String] = timestamps.map(tsToDt)
}

object StageChart {
    case class TimePoint(ts: String, value: String)

    def apply(stageTimeline: DataTable)(implicit logger: SparkScopeLogger): StageChart = {
        val tsCol = stageTimeline.select(ColTs)
        val stageCols = stageTimeline.columns.filter(_.name != ColTs)

        if(stageCols.exists(_.size != tsCol.size)) {
            throw new IllegalArgumentException(s"Stage series sizes are different: ${Seq(tsCol.size) ++ stageCols.map(_.size)}")
        } else if (stageCols.map(_.size).sum <= MaxStageChartPoints) {
            logger.info(s"Number of total data points is less than maximum. Rendering all data points. ${stageCols.map(_.size).sum} < ${MaxStageChartPoints}")
            StageChart(tsCol.values, stageCols.map(generateChart).mkString("[", ",", "]"))
        } else {
            logger.info(s"Limiting total stage chart points from ${stageCols.map(_.size).sum} to ${MaxStageChartPoints}")
            val desiredSingleChartPoints: Int = MaxStageChartPoints / stageCols.length
            logger.info(s"Limiting every stage chart points from ${stageCols.headOption.map(_.size).getOrElse(0)} to ${desiredSingleChartPoints}")

            decreaseDataPoints(tsCol, stageCols, desiredSingleChartPoints) match {
                case (newTsCol, newStageCols) => StageChart(newTsCol.values, newStageCols.map(generateChart).mkString("[", ",", "]"))
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
           |             label: "stageId=${col.name}",
           |             fill: true,
           |             lineTension: 0.0,
           |}""".stripMargin
    }
}