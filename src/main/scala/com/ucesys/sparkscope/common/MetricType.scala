package com.ucesys.sparkscope.common

sealed trait MetricType { def name: String }

case object JvmHeapUsed extends MetricType { val name = "jvm.heap.used" }
case object JvmHeapUsage extends MetricType { val name = "jvm.heap.usage" }
case object JvmHeapMax extends MetricType { val name = "jvm.heap.max" }
case object JvmNonHeapUsed extends MetricType { val name = "jvm.non-heap.used" }
case object CpuTime extends MetricType { val name = "executor.cpuTime" }

object MetricType {
    val AllMetricsExecutor: Seq[MetricType] = Seq(JvmHeapUsed, JvmHeapUsage, JvmHeapMax, JvmNonHeapUsed, CpuTime)
    val AllMetricsDriver: Seq[MetricType] = Seq(JvmHeapUsed, JvmHeapUsage, JvmHeapMax, JvmNonHeapUsed)
    val AllMetrics = (AllMetricsExecutor ++ AllMetricsDriver).toSet

    def fromString(name: String): MetricType = AllMetrics.find(_.name == name).getOrElse(throw new IllegalArgumentException(s"Unknown metric: ${name}"))
}
