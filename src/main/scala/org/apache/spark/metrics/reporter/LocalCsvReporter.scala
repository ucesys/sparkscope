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

package org.apache.spark.metrics.reporter

import com.codahale.metrics._
import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.data.DataTable
import com.ucesys.sparkscope.io.writer.LocalFileWriter

import java.io.IOException
import java.util.Locale
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.nio.file.Paths

/**
 * A reporter which creates a comma-separated values file of the measurements for each metric.
 */
class LocalCsvReporter(rootDir: String,
                       registry: MetricRegistry,
                       locale: Locale,
                       separator: String,
                       rateUnit: TimeUnit,
                       durationUnit: TimeUnit,
                       clock: Clock,
                       filter: MetricFilter,
                       executor: ScheduledExecutorService,
                       shutdownExecutorOnStop: Boolean,
                       fileWriter: LocalFileWriter,
                       appName: Option[String] = None)
                      (implicit logger: SparkScopeLogger)
  extends AbstractCsvReporter(registry, locale, separator, rateUnit, durationUnit, clock, filter, executor, shutdownExecutorOnStop) {

    logger.info("Using LocalCsvReporter", this.getClass)

    override protected[reporter] def report(appId: String, instance: String, metrics: DataTable, timestamp: Long): Unit = {
        logger.debug("\n" + metrics.toString, this.getClass)
        val row: String = metrics.toCsvNoHeader(separator)

        val appDir = Paths.get(rootDir, appName.getOrElse(""), appId).toString
        val csvFilePath = Paths.get(appDir, s"${instance}.csv").toString

        try {
            logger.debug(s"Writing to ${csvFilePath}", this.getClass)
            if (!fileWriter.exists(csvFilePath)) {
                fileWriter.makeDir(appDir)
                fileWriter.write(csvFilePath, metrics.header + "\n");
            }

            fileWriter.append(csvFilePath, row);
        } catch {
            case e: IOException => logger.warn(s"Error writing ${metrics.name} to local dir ${csvFilePath}. ${e}", this.getClass)
        }
    }

}
