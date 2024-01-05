package com.ucesys.sparkscope.io.http

import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.io.http.JsonHttpClient.DefaultTimeoutMillis
import org.apache.http.client.HttpResponseException
import org.apache.http.client.config.{CookieSpecs, RequestConfig}
import org.apache.http.{HttpHeaders, HttpStatus}
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder

class JsonHttpClient(implicit logger: SparkScopeLogger) {
    def post(url: String, jsonStr: String, timeoutMillis: Int = DefaultTimeoutMillis): Unit = {
        val requestConfig = RequestConfig
          .custom
          .setConnectTimeout(timeoutMillis)
          .setConnectionRequestTimeout(timeoutMillis)
          .setSocketTimeout(timeoutMillis)
          .setCookieSpec(CookieSpecs.STANDARD)
          .build();

        val client = HttpClientBuilder
          .create()
          .setDefaultRequestConfig(requestConfig)
          .build()

        val post = new HttpPost(url)
        post.addHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        post.setEntity(new StringEntity(jsonStr))

        val statusLine = client.execute(post).getStatusLine
        if (statusLine.getStatusCode != HttpStatus.SC_OK) {
            throw new HttpResponseException(
                statusLine.getStatusCode,
                s"Status code: ${statusLine.getStatusCode}, message: ${statusLine.getReasonPhrase}"
            )
        }
    }
}

object JsonHttpClient {
    val DefaultTimeoutMillis: Int = 5000
}