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

package com.ucesys.sparkscope.io.report

import com.ucesys.sparkscope.common._
import com.ucesys.sparkscope.io.writer.TextFileWriter
import com.ucesys.sparkscope.metrics.SparkScopeResult
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import java.nio.file.Paths

class JsonFileReporter(appContext: AppContext, sparkScopeConf: SparkScopeConf, jsonFileWriter: TextFileWriter)
                      (implicit logger: SparkScopeLogger) extends Reporter {

    implicit val formats = DefaultFormats

    override def report(result: SparkScopeResult): Unit = {
        val report = SparkScopeReport(appContext = appContext, conf = sparkScopeConf, result = result)

        try {
            val jsonReport = Serialization.writePretty(report)

            sparkScopeConf.jsonReportPath.foreach{ path =>
                val outputPath = Paths.get(path, s"${appContext.appId}.json")
                jsonFileWriter.write(outputPath.toString, jsonReport)
                logger.info(s"Wrote JSON report file to ${outputPath}", this.getClass)
            }
        } catch {
            case ex: Exception => logger.error(s"Unexpected exception while trying to send diagnostics: ${ex}", this.getClass, stdout = false)
        }
    }
}
