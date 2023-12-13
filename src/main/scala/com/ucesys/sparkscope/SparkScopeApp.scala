
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

import com.ucesys.sparkscope.event.EventLogContextLoader
import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.io.file.FileReaderFactory
import com.ucesys.sparkscope.io.metrics.{MetricReaderFactory, MetricsLoaderFactory}
import com.ucesys.sparkscope.io.property.PropertiesLoaderFactory
import com.ucesys.sparkscope.view.ReportGeneratorFactory

object SparkScopeApp {
    def main(args: Array[String]): Unit = {
        implicit val logger: SparkScopeLogger = new SparkScopeLogger

        val parsedArgs = SparkScopeArgs(args)

        runFromEventLog(
            sparkScopeArgs = parsedArgs,
            sparkScopeAnalyzer = new SparkScopeAnalyzer,
            eventLogContextLoader = new EventLogContextLoader,
            sparkScopeConfLoader = new SparkScopeConfLoader,
            fileReaderFactory = new FileReaderFactory(parsedArgs.region),
            propertiesLoaderFactory = new PropertiesLoaderFactory,
            metricsLoaderFactory = new MetricsLoaderFactory(new MetricReaderFactory(offline = true)),
            reportGeneratorFactory = new ReportGeneratorFactory,
        )
    }

    def runFromEventLog(sparkScopeArgs: SparkScopeArgs,
                        sparkScopeAnalyzer: SparkScopeAnalyzer,
                        eventLogContextLoader: EventLogContextLoader,
                        sparkScopeConfLoader: SparkScopeConfLoader,
                        fileReaderFactory: FileReaderFactory,
                        propertiesLoaderFactory: PropertiesLoaderFactory,
                        metricsLoaderFactory: MetricsLoaderFactory,
                        reportGeneratorFactory: ReportGeneratorFactory)
                       (implicit logger: SparkScopeLogger): Unit = {
        val eventLogCtx = eventLogContextLoader.load(fileReaderFactory, sparkScopeArgs)

        val sparkScopeRunner = new SparkScopeRunner(
            eventLogCtx.appContext,
            eventLogCtx.sparkConf,
            sparkScopeConfLoader,
            sparkScopeAnalyzer,
            propertiesLoaderFactory,
            metricsLoaderFactory,
            reportGeneratorFactory
        )
        sparkScopeRunner.run()
    }
}
