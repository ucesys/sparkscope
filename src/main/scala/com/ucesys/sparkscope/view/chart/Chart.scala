package com.ucesys.sparkscope.view.chart

import java.time.LocalDateTime.ofEpochSecond
import java.time.ZoneOffset.UTC

trait Chart {
    def tsToDt(ts: String): String = s"'${ofEpochSecond(ts.toLong, 0, UTC)}'"
    def labels: Seq[String]
}
