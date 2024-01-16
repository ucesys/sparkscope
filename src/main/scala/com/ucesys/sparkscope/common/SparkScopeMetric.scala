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

package com.ucesys.sparkscope.common

case class SparkScopeMetric(appId: String, instance: String, metricType: MetricType, value: String, format: String) {
    def name: String = metricType.name
    def fullName: String = s"${appId}.${instance}.${metricType.name}"
}

object SparkScopeMetric {
    def parse(fullMetricName: String, value: String, format: String): SparkScopeMetric = {
        val nameStripped: String = fullMetricName.replace("\"", "").replace("\'", "")
        val nameSplit: Seq[String] = nameStripped.split("\\.");

        nameSplit match
        {
            case Seq(appId, instance, metricNameTail@_*) if metricNameTail.nonEmpty => SparkScopeMetric(
                appId,
                instance,
                MetricType.fromString(metricNameTail.mkString(".")),
                value,
                format
            )
            case _ => throw new IllegalArgumentException(s"Couldn't parse metric: ${fullMetricName}")
        }
    }
}
