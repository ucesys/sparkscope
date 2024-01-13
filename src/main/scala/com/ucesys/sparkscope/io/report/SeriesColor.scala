package com.ucesys.sparkscope.io.report

case class SeriesColor(borderColor: String, backgroundColor: String)

object SeriesColor {
    val Green = SeriesColor("#3cba9f", "#71d1bd")
    val Blue = SeriesColor("#3e95cd", "#7bb6dd")
    val Yellow = SeriesColor("#f1bd3e", "#f4cd6e")
    val Purple = SeriesColor("#4B0082", "#9370DB")
    val Orange = SeriesColor("#ff8c00", "#ed9121")
    val Red = SeriesColor("#CD5C5C", "#F08080")

    val AllColors: Seq[SeriesColor] = Seq(Green, Blue, Yellow, Red, Purple, Orange)

    def randomColorModulo(id: Int, colors: Seq[SeriesColor] = AllColors): SeriesColor = colors(id % colors.length)
}
