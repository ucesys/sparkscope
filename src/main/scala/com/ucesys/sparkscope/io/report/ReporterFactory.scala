package com.ucesys.sparkscope.io.report

import com.ucesys.sparkscope.common.{SparkScopeConf, SparkScopeLogger}
import com.ucesys.sparkscope.io.http.JsonHttpClient
import com.ucesys.sparkscope.io.writer.FileWriterFactory

class ReporterFactory {
    def get(sparkScopeConfig: SparkScopeConf)(implicit logger: SparkScopeLogger): Seq[Reporter] = {
        val htmlReporter =
            new HtmlFileReporter(
                sparkScopeConfig,
                (new FileWriterFactory(sparkScopeConfig.region)).get(sparkScopeConfig.htmlReportPath),
                (new FileWriterFactory(sparkScopeConfig.region)).get(sparkScopeConfig.logPath)
            )

        if (sparkScopeConfig.sendDiagnostics) {
            Seq(new JsonHttpDiagnosticsReporter(sparkScopeConfig, new JsonHttpClient), htmlReporter)
        } else {
            Seq(htmlReporter)
        }
    }
}
