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

import java.lang.System.exit

case class SparkScopeArgs(eventLog: String,
                          driverMetrics: Option[String],
                          executorMetrics: Option[String] = None,
                          htmlPath: Option[String] = None,
                          jsonPath: Option[String] = None,
                          jsonServer: Option[String] = None,
                          logPath: Option[String] = None,
                          logLevel: Option[String] = None,
                          diagnostics: Option[String] = None,
                          region: Option[String] = None)

object SparkScopeArgs {
    val OptionEventLog = "--event-log"
    val OptionDriverMetrics = "--driver-metrics"
    val OptionExecutorMetrics = "--executor-metrics"
    val OptionHtmlPath = "--html-path"
    val OptionJsonPath = "--json-path"
    val OptionJsonServer = "--json-server"
    val OptionLogPath = "--log-path"
    val OptionLogLevel = "--log-level"
    val OptionDiagnostics = "--diagnostics"
    val OptionRegion = "--region"
    def Usage: String =
        s"""
          |Usage:
          |--event-log        Path to event log of spark application
          |--driver-metrics   Path to directory with driver metrics
          |--executor-metrics Path to directory with executor metrics
          |--html-path        Path to directory where html report will be stored
          |--json-path        Path to directory where json report will be stored
          |--json-server      Url of Server to receive json report
          |--log-path         Path to directory where logs will be stored
          |--log-level        Log level: [DEBUG|INFO|WARN|ERROR]
          |--diagnostics      Whether to send diagnostics data: [true|false]
          |--region           required if reading eventLog from s3
          |""".stripMargin


    def apply(args: Array[String]): SparkScopeArgs = {
        val argsMap: Map[String, String] = args.sliding (2, 2).toList.collect {
            case Array (OptionEventLog, eventLog) => (OptionEventLog, eventLog)
            case Array (OptionDriverMetrics, driverMetrics) => (OptionDriverMetrics, driverMetrics)
            case Array (OptionExecutorMetrics, executorMetrics) => (OptionExecutorMetrics, executorMetrics)
            case Array (OptionHtmlPath, htmlPath) => (OptionHtmlPath, htmlPath)
            case Array (OptionJsonPath, jsonPath) => (OptionJsonPath, jsonPath)
            case Array (OptionJsonServer, jsonServer) => (OptionJsonServer, jsonServer)
            case Array (OptionLogPath, logPath) => (OptionLogPath, logPath)
            case Array (OptionLogLevel, logLevel) => (OptionLogLevel, logLevel)
            case Array (OptionDiagnostics, diagnostics) => (OptionDiagnostics, diagnostics)
            case Array (OptionRegion, region) => (OptionRegion, region)
        }.toMap

        if (args.contains("--help") || args.contains("-h")) {
            println(Usage)
            exit(0)
        } else if (!argsMap.contains(OptionEventLog)) {
            println(Usage)
            throw new IllegalArgumentException("event log path not specified!")
        }

        SparkScopeArgs(
            eventLog=argsMap(OptionEventLog),
            driverMetrics=argsMap.get(OptionDriverMetrics),
            executorMetrics=argsMap.get(OptionExecutorMetrics),
            htmlPath=argsMap.get(OptionHtmlPath),
            jsonPath=argsMap.get(OptionJsonPath),
            jsonServer=argsMap.get(OptionJsonServer),
            logPath=argsMap.get(OptionLogPath),
            logLevel=argsMap.get(OptionLogLevel),
            diagnostics=argsMap.get(OptionDiagnostics),
            region=argsMap.get(OptionRegion)
        )
    }
}
