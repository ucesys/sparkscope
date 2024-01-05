package com.ucesys.sparkscope.io.report

import com.ucesys.sparkscope.common.{SparkScopeConf, SparkScopeLogger}
import com.ucesys.sparkscope.io.http.JsonHttpClient
import com.ucesys.sparkscope.io.writer.FileWriterFactory

class ReporterFactory {
    def get(sparkScopeConfig: SparkScopeConf)(implicit logger: SparkScopeLogger): Seq[Reporter] = {
        Seq(
            new JsonHttpDiagnosticsReporter(sparkScopeConfig, new JsonHttpClient),
            new HtmlFileReporter(
                sparkScopeConfig,
                (new FileWriterFactory(sparkScopeConfig.region)).get(sparkScopeConfig.htmlReportPath),
                (new FileWriterFactory(sparkScopeConfig.region)).get(sparkScopeConfig.logPath)
            )
        )
    }
}
