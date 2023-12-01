package com.ucesys.sparkscope.view.chart

import com.ucesys.sparkscope.common.MetricUtils.ColTs
import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.data.{DataColumn, DataTable}
import com.ucesys.sparkscope.view.HtmlReportGenerator.MaxChartPoints
import com.ucesys.sparkscope.view.SeriesColor
import com.ucesys.sparkscope.view.SeriesColor._

case class StageChart(timestamps: Seq[String], datasets: String) extends Chart {
    def labels: Seq[String] = timestamps.map(tsToDt)
}

object StageChart {
    case class TimePoint(ts: String, value: String, limit: String)

    def apply(stageTimeline: DataTable)(implicit logger: SparkScopeLogger): StageChart = {
        val stageCols = stageTimeline.columns.filter(_.name != ColTs)
        val datasetsStr = generateStages(stageCols)
        StageChart(stageTimeline.select(ColTs).values, datasetsStr)
//        if(tsCol.size != valueCol.size) {
//            throw new IllegalArgumentException(s"Series sizes are different: ${tsCol.size} and ${valueCol.size}")
//        } else if (tsCol.size <= MaxChartPoints) {
//            StageChart(tsCol.values, valueCol.values)
//        } else {
//            logger.info(s"Limiting chart points from ${tsCol.size} to ${MaxChartPoints}")
//            val ratio: Float = tsCol.size.toFloat / MaxChartPoints.toFloat
//            val newPoints: Seq[Int] = (0 until MaxChartPoints)
//            val newChart: Seq[TimePoint] = newPoints.map{ id =>
//                val from: Int = (id*ratio).toInt
//                val to: Int = (from + ratio).toInt
//                val values = valueCol.values.slice(from, to).map(_.toDouble.toLong)
//                val avg = values.sum/values.length
//                TimePoint(tsCol.values(from), avg.toString)
//            }
//            StageChart(newChart.map(_.ts), newChart.map(_.value))
//        }
    }

    def generateStages(stageCols: Seq[DataColumn]): String = {
        stageCols.map(generateStageData).mkString("[", ",", "]")
    }

    def generateStageData(stageCol: DataColumn): String = {
        val color = SeriesColor.randomColorModulo(stageCol.name.toInt, Seq(Green, Red, Yellow, Purple, Orange))
        s"""{
           |             data: [${stageCol.values.mkString(",")}],
           |             label: "stageId=${stageCol.name}",
           |             borderColor: "${color.borderColor}",
           |             backgroundColor: "${color.backgroundColor}",
           |             lineTension: 0.0,
           |             fill: true,
           |             pointRadius: 1,
           |             pointHoverRadius: 8,
           |}""".stripMargin
    }
}
