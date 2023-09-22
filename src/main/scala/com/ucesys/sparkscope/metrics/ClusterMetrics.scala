package com.ucesys.sparkscope.metrics

import com.ucesys.sparkscope.data.DataFrame

case class ClusterMetrics(heapMax: DataFrame, heapUsed: DataFrame, heapUsage: DataFrame, cpuUsage: DataFrame) {
  override def toString: String = {
    Seq(
      s"\nCluster metrics:",
      heapMax.toString,
      heapUsed.toString,
      heapUsage.toString,
      cpuUsage.toString
    ).mkString("\n")
  }
}
