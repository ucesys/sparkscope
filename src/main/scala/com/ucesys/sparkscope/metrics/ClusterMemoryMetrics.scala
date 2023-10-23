package com.ucesys.sparkscope.metrics

import com.ucesys.sparkscope.SparkScopeAnalyzer._
import com.ucesys.sparkscope.data.DataFrame

case class ClusterMemoryMetrics(heapMax: DataFrame, heapUsed: DataFrame, heapUsage: DataFrame) {
    override def toString: String = {
        Seq(
            s"\nCluster metrics:",
            heapMax.toString,
            heapUsed.toString,
            heapUsage.toString
        ).mkString("\n")
    }
}

object ClusterMemoryMetrics {
    def apply(allExecutorsMetrics: DataFrame): ClusterMemoryMetrics = {
        ClusterMemoryMetrics(
            heapMax = allExecutorsMetrics.groupBy("t", JvmHeapMax).sum.sortBy("t"),
            heapUsed = allExecutorsMetrics.groupBy("t", JvmHeapUsed).sum.sortBy("t"),
            heapUsage = allExecutorsMetrics.groupBy("t", JvmHeapUsage).avg.sortBy("t")
        )
    }
}
