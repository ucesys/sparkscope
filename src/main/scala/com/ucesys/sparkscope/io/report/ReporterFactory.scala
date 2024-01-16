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

import com.ucesys.sparkscope.common.{AppContext, SparkScopeConf, SparkScopeLogger}
import com.ucesys.sparkscope.io.http.JsonHttpClient
import com.ucesys.sparkscope.io.writer.FileWriterFactory

class ReporterFactory {
    def get(appContext: AppContext, sparkScopeConfig: SparkScopeConf)(implicit logger: SparkScopeLogger): Seq[Reporter] = {

        val diagnosticsReporterOpt = sparkScopeConfig.diagnosticsUrl.map(new JsonHttpDiagnosticsReporter(appContext, new JsonHttpClient, _))

        val htmlReporterOpt = sparkScopeConfig.htmlReportPath.map { path =>
            new HtmlFileReporter(
                appContext,
                sparkScopeConfig,
                (new FileWriterFactory).get(path, sparkScopeConfig.region)
            )
        }

        val jsonReporterOpt = sparkScopeConfig.jsonReportPath.map { path =>
            new JsonFileReporter(
                appContext,
                sparkScopeConfig,
                (new FileWriterFactory).get(path, sparkScopeConfig.region)
            )
        }

        val jsonHttpReporterOpt = sparkScopeConfig.jsonReportServer.map { server =>
            new JsonHttpReporter(
                appContext,
                sparkScopeConfig,
                new JsonHttpClient,
                server
            )
        }

        Seq(diagnosticsReporterOpt, htmlReporterOpt, jsonReporterOpt, jsonHttpReporterOpt).flatten
    }
}
