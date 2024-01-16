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

package com.ucesys.sparkscope.common

sealed trait MetricType { def name: String }

case object JvmHeapUsed extends MetricType { val name = "jvm.heap.used" }
case object JvmHeapUsage extends MetricType { val name = "jvm.heap.usage" }
case object JvmHeapMax extends MetricType { val name = "jvm.heap.max" }
case object JvmNonHeapUsed extends MetricType { val name = "jvm.non-heap.used" }
case object CpuTime extends MetricType { val name = "executor.cpuTime" }

object MetricType {
    val AllMetricsDriver: Seq[MetricType] = Seq(JvmHeapMax, JvmHeapUsage, JvmHeapUsed, JvmNonHeapUsed)
    val AllMetricsExecutor: Seq[MetricType] = Seq(JvmHeapMax, JvmHeapUsage, JvmHeapUsed, JvmNonHeapUsed, CpuTime)
    val AllMetrics = (AllMetricsExecutor ++ AllMetricsDriver).toSet
    def fromString(name: String): MetricType = AllMetrics.find(_.name == name).getOrElse(throw new IllegalArgumentException(s"Unknown metric: ${name}"))
}
