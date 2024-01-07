package com.ucesys.sparkscope.io.report

import com.ucesys.sparkscope.common._
import com.ucesys.sparkscope.io.http.JsonHttpClient
import com.ucesys.sparkscope.metrics.SparkScopeResult

class JsonHttpReporter(appContext: AppContext,
                       sparkScopeConf: SparkScopeConf,
                       jsonHttpPublisher: JsonHttpClient,
                       endpoint: String)
                      (implicit logger: SparkScopeLogger) extends Reporter with JsonHttpMethods {

    override def report(result: SparkScopeResult): Unit = {
        logger.info(s"Sending JSON report to ${endpoint}", this.getClass)

        val report = CompleteReport(
            appContext = appContext,
            conf = sparkScopeConf,
            stats = result.stats,
            charts = result.charts,
            warnings = result.warnings.map(_.toString)
        )

        postReport(jsonHttpPublisher, endpoint, report)
    }
}
