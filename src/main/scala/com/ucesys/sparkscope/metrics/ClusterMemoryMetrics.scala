package com.ucesys.sparkscope.metrics

import com.ucesys.sparkscope.common.{JvmHeapMax, JvmHeapUsage, JvmHeapUsed}
import com.ucesys.sparkscope.data.DataTable

case class ClusterMemoryMetrics(heapMax: DataTable, heapUsed: DataTable, heapUsage: DataTable) {
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
    def apply(allExecutorsMetrics: DataTable): ClusterMemoryMetrics = {

        ClusterMemoryMetrics(
            heapMax = allExecutorsMetrics.groupBy("t", JvmHeapMax.name).sum.sortBy("t"),
            heapUsed = allExecutorsMetrics.groupBy("t", JvmHeapUsed.name).sum.sortBy("t"),
            heapUsage = allExecutorsMetrics.groupBy("t", JvmHeapUsage.name).avg.sortBy("t")
        )
    }
}
