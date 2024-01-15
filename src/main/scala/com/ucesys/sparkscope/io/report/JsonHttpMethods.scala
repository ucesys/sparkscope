package com.ucesys.sparkscope.io.report

import com.ucesys.sparkscope.common._
import com.ucesys.sparkscope.io.http.JsonHttpClient
import org.apache.http.client.HttpResponseException
import org.apache.http.conn.HttpHostConnectException
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import java.net.{SocketTimeoutException, UnknownHostException}

trait JsonHttpMethods {

    implicit val formats: DefaultFormats = DefaultFormats

    def postReport(jsonHttpClient: JsonHttpClient, endpoint: String, report: Object, stdout: Boolean = true)
                  (implicit logger: SparkScopeLogger): Unit = {
        try {
            val jsonReport = Serialization.write(report)
            jsonHttpClient.post(endpoint, jsonReport)
        } catch {
            case ex: HttpHostConnectException => logger.warn(ex.toString, this.getClass, stdout)
            case ex: UnknownHostException => logger.warn(ex.toString, this.getClass, stdout)
            case ex: SocketTimeoutException => logger.warn(ex.toString, this.getClass, stdout)
            case ex: HttpResponseException => logger.warn(ex.toString, this.getClass, stdout)
            case ex: Exception => logger.warn(s"Unexpected exception while trying to send json via post: ${ex}", this.getClass)
        }
    }
}
