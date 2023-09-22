package com.ucesys.sparkscope.metrics

case class DriverStats(maxHeap: Long, maxHeapPerc: Double, avgHeap: Long, avgHeapPerc: Double, avgNonHeap: Long, maxNonHeap: Long) {
  override def toString: String = {
    Seq(
      s"\nDriver stats:",
      f"Max heap memory utilization by driver: ${maxHeap}MB(${maxHeapPerc}%1.2f%%)",
      f"Average heap memory utilization by driver: ${avgHeap}MB(${avgHeapPerc}%1.2f%%)",
      s"Max non-heap memory utilization by driver: ${maxNonHeap}MB",
      f"Average non-heap memory utilization by driver: ${avgNonHeap}MB"
    ).mkString("\n")
  }
}

