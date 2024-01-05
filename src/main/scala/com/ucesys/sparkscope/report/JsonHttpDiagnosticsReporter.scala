package com.ucesys.sparkscope.report

import com.ucesys.sparkscope.common._
import com.ucesys.sparkscope.io.http.JsonHttpClient
import com.ucesys.sparkscope.metrics.SparkScopeResult
import com.ucesys.sparkscope.report.JsonHttpDiagnosticsReporter.DiagnosticsEndpoint
import com.ucesys.sparkscope.stats.SparkScopeStats
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

case class AppInfo(appId: String,
                   sparkAppName: Option[String],
                   sparkScopeAppName: Option[String],
                   startTs: Long,
                   endTs: Option[Long],
                   duration: Option[Long],
                   driverHost: Option[String])

case class DiagnosticsInfo(appInfo: AppInfo, stats: SparkScopeStats)

class JsonHttpDiagnosticsReporter(sparkScopeConf: SparkScopeConf,
                                  jsonHttpPublisher: JsonHttpClient,
                                  endpoint: String = DiagnosticsEndpoint)
                                 (implicit logger: SparkScopeLogger) extends Reporter {

    implicit val formats = DefaultFormats

    override def report(result: SparkScopeResult): Unit = {
        val diagnosticsInfo = DiagnosticsInfo(
            appInfo = AppInfo(
                appId = result.appContext.appId,
                sparkAppName = sparkScopeConf.sparkConf.getOption("spark.app.name"),
                sparkScopeAppName = sparkScopeConf.appName,
                startTs = result.appContext.appStartTime,
                endTs = result.appContext.appEndTime,
                duration = result.appContext.appEndTime.map(endTime => endTime - result.appContext.appStartTime),
                driverHost = sparkScopeConf.sparkConf.getOption("spark.driver.host")
            ),
            stats = result.stats
        )

        val diagnosticsInfoJsonStr = Serialization.write(diagnosticsInfo)

        jsonHttpPublisher.post(endpoint, diagnosticsInfoJsonStr)
    }
}

object JsonHttpDiagnosticsReporter {
    val DiagnosticsEndpoint: String = "https://sparkscope.ucesys.com/diagnostics"
//val DiagnosticsEndpoint: String = "http://localhost"
}
