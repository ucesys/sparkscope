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

import com.ucesys.sparkscope.common.CpuTime
import com.ucesys.sparkscope.common.MetricUtils.ColCpuUsage
import com.ucesys.sparkscope.data.DataTable

case class ClusterCPUMetrics(clusterCpuTime: DataTable,
                             clusterCpuUsage: DataTable,
                             clusterCpuUsageSum: DataTable,
                             clusterCapacity: DataTable,
                             cpuTimePerExecutor: DataTable,
                             numExecutors: DataTable) {
    override def toString: String = {
        Seq(
            s"\nCluster metrics:",
            clusterCpuTime.toString,
            clusterCpuUsage.toString,
            clusterCpuUsageSum.toString,
            clusterCapacity.toString,
            cpuTimePerExecutor.toString,
            numExecutors.toString,
        ).mkString("\n")
    }
}

object ClusterCPUMetrics {
    def apply(allExecutorsMetrics: DataTable, executorCores: Int): ClusterCPUMetrics = {
        val clusterCpuTimeDf = allExecutorsMetrics.groupBy("t", CpuTime.name).sum.sortBy("t")
        val groupedTimeCol = clusterCpuTimeDf.select("t")

        val clusterCpuUsageCol = allExecutorsMetrics
          .groupBy("t", ColCpuUsage)
          .avg
          .sortBy("t")
          .select(ColCpuUsage)
          .lt(1.0d)
          .rename(ColCpuUsage)
        val clusterCpuUsageDf = DataTable(name = "clusterCpuUsage", columns = Seq(groupedTimeCol, clusterCpuUsageCol))

        val numExecutors = allExecutorsMetrics.groupBy("t", ColCpuUsage).count.sortBy("t").select(Seq("t", "cnt"))
        val clusterCapacityCol = numExecutors.select("cnt").mul(executorCores)
        val clusterCapacityDf = DataTable(name = "clusterCapacity", columns = Seq(groupedTimeCol, clusterCapacityCol.rename("totalCores")))

        val clusterCpuUsageSumCol = allExecutorsMetrics.groupBy("t", "cpuUsageAllCores")
          .sum
          .sortBy("t")
          .select("cpuUsageAllCores")
          .min(clusterCapacityCol)
          .rename("cpuUsageAllCores")
        val clusterCpuUsageSumDf = DataTable(name = "cpuUsageAllCores", columns = Seq(groupedTimeCol, clusterCpuUsageSumCol))

        ClusterCPUMetrics(
            clusterCpuTime = clusterCpuTimeDf,
            clusterCpuUsage = clusterCpuUsageDf,
            clusterCpuUsageSum = clusterCpuUsageSumDf,
            clusterCapacity = clusterCapacityDf,
            cpuTimePerExecutor = allExecutorsMetrics.groupBy("executorId", CpuTime.name).max,
            numExecutors = numExecutors
        )
    }
}
