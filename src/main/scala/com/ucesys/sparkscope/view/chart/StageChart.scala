package com.ucesys.sparkscope.view.chart

import com.ucesys.sparkscope.common.MetricUtils.ColTs
import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.data.{DataColumn, DataTable}
import com.ucesys.sparkscope.view.HtmlReportGenerator.MaxStageChartPoints
import com.ucesys.sparkscope.view.SeriesColor
import com.ucesys.sparkscope.view.SeriesColor._

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
            StageChart(tsCol.values, stageCols.map(generateStageData).mkString("[", ",", "]"))
        } else {
            logger.info(s"Limiting total stage chart points from ${stageCols.map(_.size).sum} to ${MaxStageChartPoints}")
            val singleChartPoints: Int = MaxStageChartPoints / stageCols.length
            logger.info(s"Limiting every stage chart points from ${stageCols.headOption.map(_.size).getOrElse(0)} to ${singleChartPoints}")

            val ratio: Float = tsCol.size.toFloat / singleChartPoints.toFloat
            val newPoints: Seq[Int] = (0 until singleChartPoints)

            val newTimestamps: Seq[String] = newPoints.map(id => tsCol.values((id * ratio).toInt))

            val newStageCols: Seq[DataColumn] = stageCols.map{ stageCol =>
                val values: Seq[String] = newPoints.map { id =>
                    val from: Int = (id * ratio).toInt
                    val to: Int = (from + ratio).toInt
                    val valuesNotNull = stageCol.values.slice(from, to).filter(_ != "null")

                    valuesNotNull match {
                        case Seq() => "null"
                        case _ => valuesNotNull.map(_.toDouble.toLong).max.toString
                    }
                }
                DataColumn(stageCol.name, values)
            }
            StageChart(newTimestamps, newStageCols.map(generateStageData).mkString("[", ",", "]"))
        }
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
