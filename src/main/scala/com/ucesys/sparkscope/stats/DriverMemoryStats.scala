/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.ucesys.sparkscope.stats

import com.ucesys.sparkscope.common.MemorySize.BytesInMB
import com.ucesys.sparkscope.common.{JvmHeapMax, JvmHeapUsage, JvmHeapUsed, JvmNonHeapUsed}
import com.ucesys.sparkscope.data.DataTable

case class DriverMemoryStats(heapSize: Long, maxHeap: Long, maxHeapPerc: Double, avgHeap: Long, avgHeapPerc: Double, avgNonHeap: Long, maxNonHeap: Long) {
    override def toString: String = {
        Seq(
            s"Driver stats:",
            s"Driver heap size: ${heapSize}",
            f"Max heap memory utilization by driver: ${maxHeap}MB(${maxHeapPerc * 100}%1.2f%%)",
            f"Average heap memory utilization by driver: ${avgHeap}MB(${avgHeapPerc * 100}%1.2f%%)",
            s"Max non-heap memory utilization by driver: ${maxNonHeap}MB",
            f"Average non-heap memory utilization by driver: ${avgNonHeap}MB"
        ).mkString("\n")
    }
}

object DriverMemoryStats {
    def apply(driverMetricsMerged: DataTable): DriverMemoryStats = {
        DriverMemoryStats(
            heapSize = driverMetricsMerged.select(JvmHeapMax.name).max.toLong / BytesInMB,
            maxHeap = driverMetricsMerged.select(JvmHeapUsed.name).max.toLong / BytesInMB,
            maxHeapPerc = f"${driverMetricsMerged.select(JvmHeapUsage.name).max}%1.5f".toDouble,
            avgHeap = driverMetricsMerged.select(JvmHeapUsed.name).avg.toLong / BytesInMB,
            avgHeapPerc = f"${driverMetricsMerged.select(JvmHeapUsage.name).avg}%1.5f".toDouble,
            avgNonHeap = driverMetricsMerged.select(JvmNonHeapUsed.name).avg.toLong / BytesInMB,
            maxNonHeap = driverMetricsMerged.select(JvmNonHeapUsed.name).max.toLong / BytesInMB
        )
    }
}

