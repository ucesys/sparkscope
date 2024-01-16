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

package com.ucesys.sparkscope.io.report

import com.ucesys.sparkscope.common.{AppContext, SparkScopeConf}
import com.ucesys.sparkscope.metrics.SparkScopeResult
import com.ucesys.sparkscope.stats.SparkScopeStats
import com.ucesys.sparkscope.view.chart.SparkScopeCharts

case class SparkScopeReport(appContext: AppContext,
                            sparkConf: Map[String, String],
                            stats: SparkScopeStats,
                            charts: SparkScopeCharts,
                            warnings: Seq[String])

object SparkScopeReport {
    def apply(appContext: AppContext, result: SparkScopeResult, conf: SparkScopeConf): SparkScopeReport = {
        SparkScopeReport(
            appContext = appContext,
            sparkConf = conf.sparkConf.getAll.toMap,
            stats = result.stats,
            charts = result.charts,
            warnings = result.warnings.map(_.toString)
        )
    }
}
