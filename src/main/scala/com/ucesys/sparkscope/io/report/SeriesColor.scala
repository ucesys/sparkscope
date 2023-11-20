package com.ucesys.sparkscope.io.report

case class SeriesColor(borderColor: String, backgroundColor: String)

object SeriesColor {
    val Green = SeriesColor("#3cba9f", "#71d1bd")
    val Blue = SeriesColor("#3e95cd", "#7bb6dd")
    val Yellow = SeriesColor("#ffa500", "#ffc04d")
    val Red = SeriesColor("#CD5C5C", "#F08080")
    val Purple = SeriesColor("#4B0082", "#9370DB")

    val Palette: Seq[SeriesColor] = Seq(Green, Blue, Yellow, Red, Purple)

    def randomColorModulo(id: Int): SeriesColor = Palette(id % Palette.length)
}
