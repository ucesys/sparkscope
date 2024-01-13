package com.ucesys.sparkscope.io.report

import com.ucesys.sparkscope.common._
import com.ucesys.sparkscope.io.http.JsonHttpClient
import com.ucesys.sparkscope.metrics.SparkScopeResult

class JsonHttpDiagnosticsReporter(appContext: AppContext, jsonHttpPublisher: JsonHttpClient, endpoint: String)
                                 (implicit logger: SparkScopeLogger) extends Reporter with JsonHttpMethods {
    override def report(result: SparkScopeResult): Unit = {
        logger.info(s"Sending diagnostics data to ${endpoint}", this.getClass, stdout = false)

        val report = DiagnosticsReport(appContext = appContext, stats = result.stats)

        postReport(jsonHttpPublisher, endpoint, report, stdout = false)
    }
}
