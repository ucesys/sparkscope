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

package com.ucesys.sparkscope.io.metrics

import com.ucesys.sparkscope.common.{SparkScopeConf, AppContext, SparkScopeLogger}
import com.ucesys.sparkscope.data.DataTable
import com.ucesys.sparkscope.io.reader.LocalFileReader

import java.nio.file.Paths

class LocalMetricReader(sparkScopeConf: SparkScopeConf, fileReader: LocalFileReader, appContext: AppContext)
                       (implicit logger: SparkScopeLogger)extends MetricReader {
    def readDriver: DataTable = {
        readMetric("driver")
    }

    def readExecutor(executorId: String): DataTable = {
        readMetric(executorId)
    }

    def readMetric(instance: String): DataTable = {
        val metricPath: String = Paths.get(
            sparkScopeConf.driverMetricsDir,
            sparkScopeConf.appName.getOrElse(""),
            appContext.appId,
            s"${instance}.csv"
        ).toString.replace("\\", "/")
        logger.info(s"Reading instance=${instance} metric files from ${metricPath}", this.getClass)
        val metricStr = fileReader.read(metricPath)
        DataTable.fromCsv(instance, metricStr, ",").distinct("t").sortBy("t")
    }
}
