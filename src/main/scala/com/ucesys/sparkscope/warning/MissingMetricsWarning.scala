package com.ucesys.sparkscope.warning

case class MissingMetricsWarning private(allExecutors: Seq[String], withoutMetrics: Seq[String], withMetrics: Seq[String]) extends Warning {
    override def toString: String = {
        s"""Missing metrics for ${withoutMetrics.length} out of ${allExecutors.length} executors. """ +
          s"""Missing metrics for the following executor ids: ${withoutMetrics.mkString(",")}. """ +
          s"""Collected metrics for the following executor ids: ${withMetrics.mkString(",")}."""
    }
}

object MissingMetricsWarning {
    def apply(allExecutors: Seq[String], withMetrics: Seq[String]): Option[MissingMetricsWarning] = {
        val withoutMetrics = allExecutors.filterNot(withMetrics.contains)
        withoutMetrics match {
            case Seq() => None
            case _ => Some(new MissingMetricsWarning(allExecutors, withoutMetrics, withMetrics))
        }
    }
}
