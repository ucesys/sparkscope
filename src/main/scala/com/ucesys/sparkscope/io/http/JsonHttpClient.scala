package com.ucesys.sparkscope.io.http

import com.ucesys.sparkscope.common.SparkScopeLogger
import org.apache.http.{HttpHeaders, HttpStatus}
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.util.EntityUtils

class JsonHttpClient(implicit logger: SparkScopeLogger) {
    def post(url: String, jsonStr: String): Unit = {
        logger.info(s"Posting json to ${url}: ${jsonStr}", this.getClass)
        println(s"Posting json to ${url}: ${jsonStr}", this.getClass)

        val client = HttpClientBuilder.create().build()

        val post = new HttpPost(url)
        post.addHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        post.setEntity(new StringEntity(jsonStr))

        val response = client.execute(post)
        if (response.getStatusLine.getStatusCode != HttpStatus.SC_OK) {
            logger.warn(s"Response had ${response.getStatusLine.getStatusCode} status code and ${EntityUtils.toString(response.getEntity, "UTF-8")}", this.getClass)
        }
    }
}