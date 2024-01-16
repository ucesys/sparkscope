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

package com.ucesys.sparkscope.metrics

import com.ucesys.sparkscope.common.{JvmHeapMax, JvmHeapUsed, JvmNonHeapUsed}
import com.ucesys.sparkscope.data.DataTable

case class ExecutorMemoryMetrics(heapUsedMax: DataTable,
                                 heapUsedMin: DataTable,
                                 heapUsedAvg: DataTable,
                                 heapAllocation: DataTable,
                                 nonHeapUsedMax: DataTable,
                                 nonHeapUsedMin: DataTable,
                                 nonHeapUsedAvg: DataTable,
                                 executorMetricsMap: Map[String, DataTable])

object ExecutorMemoryMetrics {
    def apply(allExecutorsMetrics: DataTable, executorMetricsMap: Map[String, DataTable]): ExecutorMemoryMetrics = {
        ExecutorMemoryMetrics(
            heapUsedMax = allExecutorsMetrics.groupBy("t", JvmHeapUsed.name).max.sortBy("t"),
            heapUsedMin = allExecutorsMetrics.groupBy("t", JvmHeapUsed.name).min.sortBy("t"),
            heapUsedAvg = allExecutorsMetrics.groupBy("t", JvmHeapUsed.name).avg.sortBy("t"),
            heapAllocation = allExecutorsMetrics.groupBy("t", JvmHeapMax.name).max.sortBy("t"),
            nonHeapUsedMax = allExecutorsMetrics.groupBy("t", JvmNonHeapUsed.name).max.sortBy("t"),
            nonHeapUsedMin = allExecutorsMetrics.groupBy("t", JvmNonHeapUsed.name).min.sortBy("t"),
            nonHeapUsedAvg = allExecutorsMetrics.groupBy("t", JvmNonHeapUsed.name).avg.sortBy("t"),
            executorMetricsMap
        )
    }
}
