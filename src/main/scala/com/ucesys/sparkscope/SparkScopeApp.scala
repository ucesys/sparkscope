
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
package com.ucesys.sparkscope

import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.io.reader.FileReaderFactory
import com.ucesys.sparkscope.io.metrics.{MetricReaderFactory, MetricsLoaderFactory}
import com.ucesys.sparkscope.io.property.PropertiesLoaderFactory
import com.ucesys.sparkscope.io.report.ReporterFactory
import org.apache.spark.SparkConf

object SparkScopeApp {
    def main(args: Array[String]): Unit = {
        implicit val logger: SparkScopeLogger = new SparkScopeLogger

        val parsedArgs = SparkScopeArgs(args)

        runFromEventLog(
            sparkScopeArgs = parsedArgs,
            sparkScopeAnalyzer = new SparkScopeAnalyzer,
            sparkScopeConfLoader = new SparkScopeConfLoader,
            fileReaderFactory = new FileReaderFactory(parsedArgs.region),
            propertiesLoaderFactory = new PropertiesLoaderFactory,
            metricsLoaderFactory = new MetricsLoaderFactory(new MetricReaderFactory(offline = true)),
            reportGeneratorFactory = new ReporterFactory,
        )
    }

    def runFromEventLog(sparkScopeArgs: SparkScopeArgs,
                        sparkScopeAnalyzer: SparkScopeAnalyzer,
                        sparkScopeConfLoader: SparkScopeConfLoader,
                        fileReaderFactory: FileReaderFactory,
                        propertiesLoaderFactory: PropertiesLoaderFactory,
                        metricsLoaderFactory: MetricsLoaderFactory,
                        reportGeneratorFactory: ReporterFactory)
                       (implicit logger: SparkScopeLogger): Unit = {
        val sparkScopeRunner = new SparkScopeRunner(
            sparkScopeConfLoader,
            sparkScopeAnalyzer,
            propertiesLoaderFactory,
            metricsLoaderFactory,
            reportGeneratorFactory
        )

        val listener = new SparkScopeJobListener(new SparkConf, sparkScopeRunner)
        val eventLogRunner = new EventLogRunner(listener)

        eventLogRunner.run(fileReaderFactory, sparkScopeArgs)
    }
}
