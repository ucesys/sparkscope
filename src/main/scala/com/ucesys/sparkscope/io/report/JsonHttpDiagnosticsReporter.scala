package com.ucesys.sparkscope.io.report

import com.ucesys.sparkscope.common._
import com.ucesys.sparkscope.io.http.JsonHttpClient
import com.ucesys.sparkscope.metrics.SparkScopeResult
import JsonHttpDiagnosticsReporter.DiagnosticsEndpoint
import org.apache.http.client.HttpResponseException
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import org.apache.http.conn.HttpHostConnectException

import java.net.{SocketTimeoutException, UnknownHostException}

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

        try {
            val diagnosticsInfoJsonStr = Serialization.write(diagnosticsInfo)
            jsonHttpPublisher.post(endpoint, diagnosticsInfoJsonStr)
        } catch {
            case ex: HttpHostConnectException => logger.warn(ex.toString, this.getClass, stdout = false)
            case ex: UnknownHostException => logger.warn(ex.toString, this.getClass, stdout = false)
            case ex: SocketTimeoutException => logger.warn(ex.toString, this.getClass, stdout = false)
            case ex: HttpResponseException => logger.warn(ex.toString, this.getClass, stdout = false)
            case ex: Exception => logger.warn(s"Unexpected exception while trying to send diagnostics: ${ex}", this.getClass, stdout = false)
        }
    }
}

object JsonHttpDiagnosticsReporter {
    val DiagnosticsEndpoint: String = "http://sparkscope.ai/diagnostics"
}
