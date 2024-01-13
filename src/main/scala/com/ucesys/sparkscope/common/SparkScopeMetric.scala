package com.ucesys.sparkscope.common

case class SparkScopeMetric(appId: String, instance: String, metricType: MetricType, value: String, format: String) {
    def name: String = metricType.name
    def fullName: String = s"${appId}.${instance}.${metricType.name}"
}

object SparkScopeMetric {
    def parse(fullMetricName: String, value: String, format: String): SparkScopeMetric = {
        val nameStripped: String = fullMetricName.replace("\"", "").replace("\'", "")
        val nameSplit: Seq[String] = nameStripped.split("\\.");

        nameSplit match
        {
            case Seq(appId, instance, metricNameTail@_*) if metricNameTail.nonEmpty => SparkScopeMetric(
                appId,
                instance,
                MetricType.fromString(metricNameTail.mkString(".")),
                value,
                format
            )
            case _ => throw new IllegalArgumentException(s"Couldn't parse metric: ${fullMetricName}")
        }
    }
}
