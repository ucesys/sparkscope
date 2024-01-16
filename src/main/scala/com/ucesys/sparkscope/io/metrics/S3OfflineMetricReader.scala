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

import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.ucesys.sparkscope.common.{MetricType, SparkScopeConf, AppContext, SparkScopeLogger}
import com.ucesys.sparkscope.data.DataTable
import com.ucesys.sparkscope.io.reader.S3FileReader

import java.nio.file.Paths

class S3OfflineMetricReader(sparkScopeConf: SparkScopeConf,
                            appContext: AppContext,
                            driverS3Location: S3Location,
                            executorS3Location: S3Location,
                            reader: S3FileReader)
                           (implicit logger: SparkScopeLogger) extends MetricReader {
    def readDriver: DataTable = {
        readMerged(driverS3Location, "driver")
    }

    def readExecutor(executorId: String): DataTable = {
        readMerged(executorS3Location, executorId)
    }

    private def readMerged(s3Location: S3Location, instanceId: String): DataTable = {
        val appDir = Paths.get(s3Location.path, this.sparkScopeConf.appName.getOrElse("")).toString
        val mergedPath: String = Paths.get(appDir, appContext.appId, s"${instanceId}.csv").toString
        logger.info(s"Reading merged ${instanceId} metric file from ${mergedPath}", this.getClass)

        val csvStr = reader.read(S3Location(s3Location.bucketName, mergedPath).getUrl)
        DataTable.fromCsv(instanceId, csvStr, ",").distinct("t").sortBy("t")
    }
}

object S3OfflineMetricReader {
    def apply(sparkScopeConf: SparkScopeConf, appContext: AppContext)(implicit logger: SparkScopeLogger) : S3OfflineMetricReader = {
        val region = sparkScopeConf.region
        val s3: AmazonS3 = AmazonS3ClientBuilder
          .standard
          .withRegion(region.getOrElse(throw new IllegalArgumentException("s3 region is unset!")))
          .build

        val driverS3Location = S3Location(sparkScopeConf.driverMetricsDir)
        val executorS3Location = S3Location(sparkScopeConf.executorMetricsDir)

        new S3OfflineMetricReader(
            sparkScopeConf,
            appContext,
            driverS3Location,
            executorS3Location,
            new S3FileReader(s3)
        )
    }
}
