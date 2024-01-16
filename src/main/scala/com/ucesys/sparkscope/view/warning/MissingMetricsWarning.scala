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

package com.ucesys.sparkscope.view.warning

case class MissingMetricsWarning private(allExecutors: Seq[String], withoutMetrics: Seq[String], withMetrics: Seq[String]) extends Warning {
    override def toString: String = {
        s"""Missing metrics for ${withoutMetrics.length} out of ${allExecutors.length} executors. """ +
          s"""Missing metrics for the following executor ids: ${withoutMetrics.mkString(",")}. """ +
          s"""Collected metrics for the following executor ids: ${withMetrics.mkString(",")}."""
    }
}

object MissingMetricsWarning {
    def apply(allExecutors: Seq[String], withMetrics: Seq[String]): Option[MissingMetricsWarning] = {
        val withoutMetrics = allExecutors.filterNot(withMetrics.contains)
        withoutMetrics match {
            case Seq() => None
            case _ => Some(new MissingMetricsWarning(allExecutors, withoutMetrics, withMetrics))
        }
    }
}
