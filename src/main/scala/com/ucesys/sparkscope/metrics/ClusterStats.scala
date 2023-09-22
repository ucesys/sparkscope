package com.ucesys.sparkscope.metrics

case class ClusterStats(maxHeap: Long, avgHeap: Long, maxHeapPerc: Double, avgHeapPerc: Double, totalCpuUtil: Double) {
  override def toString: String = {
    Seq(
      "\nCluster stats: ",
      f"Average Cluster heap memory utilization: ${avgHeapPerc*100}%1.2f%% / ${avgHeap}MB",
      f"Max Cluster heap memory utilization: ${maxHeapPerc}%1.2f%% / ${maxHeap}MB",
      f"Total CPU utilization: ${totalCpuUtil*100}%1.2f%%\n"
    ).mkString("\n")
  }
}

