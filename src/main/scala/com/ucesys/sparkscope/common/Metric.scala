package com.ucesys.sparkscope.common

case class Metric(appId: String, instance: String, metricType: MetricType) {
    def name: String = metricType.name
    def fullName: String = s"${appId}.${instance}.${metricType.name}"
}

object Metric {
    def parse(fullMetricName: String): Metric = {
        val nameStripped: String = fullMetricName.replace("\"", "").replace("\'", "")
        val nameSplit: Seq[String] = nameStripped.split("\\.");

        nameSplit match
        {
            case Seq(appId, instance, metricNameTail@_*) if metricNameTail.nonEmpty => Metric(appId, instance, MetricType.fromString(metricNameTail.mkString(".")))
            case _ => throw new IllegalArgumentException(s"Couldn't parse metric: ${fullMetricName}")
        }
    }
}
