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

package org.apache.spark.metrics.reporter

import com.codahale.metrics.{Clock, Counter, Gauge, Histogram, Meter, MetricFilter, MetricRegistry, ScheduledReporter, Timer}
import com.ucesys.sparkscope.common.MetricType.{AllMetricsDriver, AllMetricsExecutor}
import com.ucesys.sparkscope.common.{SparkScopeLogger, SparkScopeMetric}
import com.ucesys.sparkscope.data.DataTable

import java.util.Locale
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._

abstract class AbstractCsvReporter(registry: MetricRegistry,
                                   locale: Locale,
                                   separator: String,
                                   rateUnit: TimeUnit,
                                   durationUnit: TimeUnit,
                                   clock: Clock,
                                   filter: MetricFilter)
                                  (implicit logger: SparkScopeLogger) extends ScheduledReporter(registry, "csv-reporter", filter, rateUnit, durationUnit) {

    val DriverHeader: String = (Seq("t") ++ AllMetricsDriver.map(_.name)).mkString(separator)
    val ExecutorHeader: String = (Seq("t") ++ AllMetricsExecutor.map(_.name)).mkString(separator)

    @Override
    def report(gauges: java.util.SortedMap[String, Gauge[_]],
               counters: java.util.SortedMap[String, Counter],
               histograms: java.util.SortedMap[String, Histogram],
               meters: java.util.SortedMap[String, Meter],
               timers: java.util.SortedMap[String, Timer]): Unit = {
        val timestamp: Long  = TimeUnit.MILLISECONDS.toSeconds(clock.getTime)

        report(timestamp, gauges, counters)
    }

    def report(timestamp: Long, gauges: java.util.SortedMap[String, Gauge[_]], counters: java.util.SortedMap[String, Counter]): Unit = {
        val gaugeMetrics: Seq[SparkScopeMetric] = gauges.asScala.map { case (name, value) => SparkScopeMetric.parse(name, value.getValue.toString, "%s")}.toSeq
        val countMetrics: Seq[SparkScopeMetric] = counters.asScala.map { case (name, value) => SparkScopeMetric.parse(name, value.getCount.toString, "%s")}.toSeq
        val allMetrics = gaugeMetrics ++ countMetrics

        val instance: String = gaugeMetrics.head.instance
        val appId: String = gaugeMetrics.head.appId

        val columns: Seq[String] = (Seq("t") ++ allMetrics.map(_.name))
        val header = columns.mkString(separator)

        if(header != DriverHeader && header != ExecutorHeader) {
            throw new IllegalArgumentException(s"Illegal header: ${header}")
        }

        val values: Seq[String] = (Seq(timestamp.toString) ++ allMetrics.map(_.value))
        val formats: String = (Seq("%s") ++ allMetrics.map(_.format)).mkString(separator)
        val formatStr: String = s"%s".formatLocal(locale, formats)
        val row: String = formatStr.formatLocal(locale, values: _*)
        logger.debug(s"header: ${header}, values: ${values}, formats: ${formats}, formatStr: ${formatStr}, row: ${row}", this.getClass)

        val metricsTable = DataTable.fromCsvWithoutHeader(s"${appId}.${instance}", row, separator, columns)
        logger.debug("\n" + metricsTable.toString, this.getClass)

        report(gaugeMetrics.head.appId, gaugeMetrics.head.instance, metricsTable, timestamp)
    }
    protected[reporter] def report(appId: String, instance: String, metrics: DataTable, timestamp: Long): Unit = ???
}
