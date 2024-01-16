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

package com.ucesys.sparkscope.io.metrics

import com.ucesys.sparkscope.common.{SparkScopeConf, AppContext, SparkScopeLogger}
import com.ucesys.sparkscope.data.DataTable
import com.ucesys.sparkscope.timeline.ExecutorTimeline

class CsvMetricsLoader(metricReader: MetricReader)(implicit logger: SparkScopeLogger) extends MetricsLoader {
    def load(appContext: AppContext, sparkScopeConf: SparkScopeConf): DriverExecutorMetrics = {
        logger.info(s"Reading driver metrics from ${sparkScopeConf.driverMetricsDir}, executor metrics from ${sparkScopeConf.executorMetricsDir}", this.getClass)
        val driverMetrics: DataTable = metricReader.readDriver
        // Filter out executorId="driver" which occurs in local mode
        val executorsMetricsMapNonDriver: Map[String, ExecutorTimeline] = appContext.executorMap
          .filter { case (executorId, _) => executorId != "driver" }

        val executorsMetricsMap: Map[String, DataTable] = executorsMetricsMapNonDriver.map { case (executorId, _) =>
            logger.info(s"Reading metrics for executor=${executorId}", this.getClass)
            val metricOpt: Option[DataTable] = try {
                Option(metricReader.readExecutor(executorId))
            }
            catch {
                case ex: Exception => logger.warn(s"Couldn't load metrics for ${executorId}. ${ex}", this.getClass); None
            }

            metricOpt match {
                case Some(metricTable) => (executorId, Some(metricTable))
                case None => logger.warn(s"Missing metrics for executor=${executorId}", this.getClass); (executorId, None)
            }
        }.filter { case (_, opt) => opt.nonEmpty }.map { case (id, opt) => (id, opt.get) }

        if (executorsMetricsMap.isEmpty) {
            throw new IllegalArgumentException("No executor metrics found")
        }

        DriverExecutorMetrics(driverMetrics, executorsMetricsMap)
    }
}
