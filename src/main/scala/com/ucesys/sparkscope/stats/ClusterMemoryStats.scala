/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.ucesys.sparkscope.stats

import com.ucesys.sparkscope.common.MemorySize.BytesInMB
import com.ucesys.sparkscope.common.{JvmHeapUsage, JvmHeapUsed}
import com.ucesys.sparkscope.metrics.ClusterMemoryMetrics

case class ClusterMemoryStats(maxHeap: Long,
                              avgHeap: Long,
                              maxHeapPerc: Double,
                              avgHeapPerc: Double,
                              avgHeapWastedPerc: Double,
                              executorTimeSecs: Long,
                              heapGbHoursAllocated: Double,
                              heapGbHoursWasted: Double,
                              executorHeapSizeInGb: Double) {
    override def toString: String = {
        Seq(
            "Cluster Memory stats: ",
            f"Average Cluster heap memory utilization: ${avgHeapPerc * 100}%1.2f%% / ${avgHeap}MB",
            f"Max Cluster heap memory utilization: ${maxHeapPerc * 100}%1.2f%% / ${maxHeap}MB",
            f"Average Cluster heap memory waste: ${avgHeapWastedPerc * 100}%1.2f%%",
            f"heapGbHoursAllocated: ${this.heapGbHoursAllocated}%1.4f",
            s"heapGbHoursAllocated=(executorHeapSizeInGb(${this.executorHeapSizeInGb})*combinedExecutorUptimeInSec(${this.executorTimeSecs}s))/3600",
            f"heapGbHoursWasted: ${this.heapGbHoursWasted}%1.4f",
            f"heapGbHoursWasted=heapGbHoursAllocated(${this.heapGbHoursAllocated}%1.4f)*heapWaste(${this.avgHeapWastedPerc}%1.4f)"
        ).mkString("\n")
    }
}

object ClusterMemoryStats {
    def apply(clusterMetrics: ClusterMemoryMetrics, executorTimeSecs: Long, executorStats: ExecutorMemoryStats): ClusterMemoryStats = {
        val executorHeapSizeInGb = executorStats.heapSize.toDouble / 1024d
        val heapGbHoursAllocated = (executorHeapSizeInGb * executorTimeSecs) / 3600d
        val avgHeapPerc: Double = f"${executorStats.avgHeapPerc}%1.5f".toDouble.min(1.0d)
        val avgHeapWastedPerc: Double = 1 - avgHeapPerc

        ClusterMemoryStats(
            maxHeap = clusterMetrics.heapUsed.select(JvmHeapUsed.name).max.toLong / BytesInMB,
            avgHeap = clusterMetrics.heapUsed.select(JvmHeapUsed.name).avg.toLong / BytesInMB,
            maxHeapPerc = f"${clusterMetrics.heapUsage.select(JvmHeapUsage.name).max}%1.5f".toDouble,
            avgHeapPerc = avgHeapPerc,
            avgHeapWastedPerc = avgHeapWastedPerc,
            executorTimeSecs = executorTimeSecs,
            heapGbHoursAllocated = f"${heapGbHoursAllocated}%1.5f".toDouble,
            heapGbHoursWasted = f"${heapGbHoursAllocated * avgHeapWastedPerc}%1.5f".toDouble,
            executorHeapSizeInGb = f"${executorHeapSizeInGb}%1.5f".toDouble
        )
    }
}
