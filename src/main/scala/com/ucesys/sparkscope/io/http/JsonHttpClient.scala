/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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