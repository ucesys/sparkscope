//package com.ucesys.sparkscope.metrics
//
//case class ResourceWasteMetrics(coreHoursAllocated: Double,
//                                coreHoursWasted: Double,
//                                cpuUtil: Double,
//                                executorCores: Int,
//                                executorTimeSecs: Long,
//                                heapGbHoursWasted: Double,
//                                heapGbHoursAllocated: Double,
//                                heapUtil: Double,
//                                executorHeapSizeInGb: Double) {
//  override def toString: String = {
//    Seq(
//      "\nResource Waste Metrics:",
//      s"coreHoursAllocated: ${this.coreHoursAllocated}",
//      s"coreHoursAllocated=(executorCores(${this.executorCores})*combinedExecutorUptimeInSec(${this.executorTimeSecs}s))/3600",
//      f"coreHoursWasted: ${this.coreHoursWasted}%1.4f",
//      f"coreHoursWasted=coreHoursAllocated(${this.coreHoursAllocated})*cpuUtilization(${this.cpuUtil}%1.4f)",
//
//      f"heapGbHoursAllocated: ${this.heapGbHoursAllocated}%1.4f",
//      s"heapGbHoursAllocated=(executorHeapSizeInGb(${this.executorHeapSizeInGb})*combinedExecutorUptimeInSec(${this.executorTimeSecs}s))/3600",
//      f"heapGbHoursWasted: ${this.heapGbHoursWasted}%1.4f",
//      f"heapGbHoursWasted=heapGbHoursAllocated(${this.heapGbHoursAllocated})*heapUtilization(${this.heapUtil}%1.4f)\n"
//    ).mkString("\n")
//  }
//}
//
//object ResourceWasteMetrics {
//  def apply(executorCores: Int,
//            executorTimeSecs: Long,
//            cpuUtil: Double,
//            heapUtil: Double,
//            executorHeapSizeInGb: Double): ResourceWasteMetrics = {
//    val coreHoursAllocated = (executorCores * executorTimeSecs).toDouble / 3600d
//    val coreHoursWasted = coreHoursAllocated * cpuUtil
//
//    val heapGbHoursAllocated = (executorHeapSizeInGb * executorTimeSecs) / 3600d
//    val heapGbHoursWasted = heapGbHoursAllocated * heapUtil
//
//    ResourceWasteMetrics(coreHoursAllocated, coreHoursWasted, heapGbHoursAllocated, heapGbHoursWasted, executorTimeSecs, cpuUtil, heapUtil, executorHeapSizeInGb, executorCores)
//  }
//}
