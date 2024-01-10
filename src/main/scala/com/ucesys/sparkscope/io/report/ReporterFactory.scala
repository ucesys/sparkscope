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
