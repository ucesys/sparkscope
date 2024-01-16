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

package com.ucesys.sparkscope.view.warning

import com.ucesys.sparkscope.agg.TaskAggMetrics
import com.ucesys.sparkscope.view.DurationExtensions.FiniteDurationExtensions

import scala.concurrent.duration.{DurationLong, FiniteDuration}

case class GCTimeWarning(taskDuration: FiniteDuration, jvmGCTime: FiniteDuration, gcTimeFraction: Double) extends Warning {
    override def toString: String = {
        f"${gcTimeFraction*100}%1.2f%% of total task duration was spent in garbage collection. GC time: ${jvmGCTime.durationStr}. Total task time: ${taskDuration.durationStr}."
    }
}

object GCTimeWarning {
    val GcTimeFractionThreshold = 0.05

    def apply(taskAggMetrics: TaskAggMetrics): Option[GCTimeWarning] = {
        val gcTimeFraction = taskAggMetrics.jvmGCTime.sum.toDouble / taskAggMetrics.taskDuration.sum.toDouble
        if(gcTimeFraction >= GcTimeFractionThreshold) {
            Some(new GCTimeWarning(taskAggMetrics.taskDuration.sum.seconds, taskAggMetrics.jvmGCTime.sum.seconds, gcTimeFraction))
        } else {
            None
        }
    }
}