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
