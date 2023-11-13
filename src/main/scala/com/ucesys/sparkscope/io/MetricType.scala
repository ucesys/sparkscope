package com.ucesys.sparkscope.io

sealed trait MetricType { def name: String }

case object JvmHeapUsed extends MetricType { val name = "jvm.heap.used" }

case object JvmHeapUsage extends MetricType { val name = "jvm.heap.usage" }

case object JvmHeapMax extends MetricType { val name = "jvm.heap.max" }

case object JvmNonHeapUsed extends MetricType { val name = "jvm.non-heap.used" }

case object CpuTime extends MetricType { val name = "executor.cpuTime" }

object MetricType {
    val AllMetrics: Seq[MetricType] = Seq(JvmHeapUsed, JvmHeapUsage, JvmHeapMax, JvmNonHeapUsed, CpuTime)
    val AllMetricsExecutor: Seq[MetricType] = Seq(JvmHeapUsed, JvmHeapUsage, JvmHeapMax, JvmNonHeapUsed, CpuTime)
    val AllMetricsDriver: Seq[MetricType] = Seq(JvmHeapUsed, JvmHeapUsage, JvmHeapMax, JvmNonHeapUsed)

    val AllNames: Seq[String] = AllMetrics.map(_.name)
    val AllExecutorNames: Seq[String] = AllMetricsExecutor.map(_.name)
    val AllDriverNames: Seq[String] = AllMetricsDriver.map(_.name)
}