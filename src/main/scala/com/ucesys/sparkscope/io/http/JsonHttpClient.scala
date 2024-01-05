package com.ucesys.sparkscope.io.http

import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.io.http.JsonHttpClient.DefaultTimeoutMillis
import org.apache.http.client.config.RequestConfig
import org.apache.http.{HttpHeaders, HttpStatus}
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils

class JsonHttpClient(implicit logger: SparkScopeLogger) {
    def post(url: String, jsonStr: String, timeoutMillis: Int = DefaultTimeoutMillis): Unit = {
        logger.info(s"Posting json to ${url} with timeoutMillis=${timeoutMillis}. Json:\n${jsonStr}", this.getClass)

        val requestConfig = RequestConfig
          .custom
          .setConnectTimeout(timeoutMillis)
          .setConnectionRequestTimeout(timeoutMillis)
          .setSocketTimeout(timeoutMillis)
          .build();

        val client = HttpClientBuilder
          .create()
          .setDefaultRequestConfig(requestConfig)
          .build()

        val post = new HttpPost(url)
        post.addHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        post.setEntity(new StringEntity(jsonStr))

        val response = client.execute(post)
        if (response.getStatusLine.getStatusCode != HttpStatus.SC_OK) {
            logger.warn(s"Response had ${response.getStatusLine.getStatusCode} status code and ${EntityUtils.toString(response.getEntity, "UTF-8")}", this.getClass)
        }
    }
}

object JsonHttpClient {
    val DefaultTimeoutMillis: Int = 5000
}