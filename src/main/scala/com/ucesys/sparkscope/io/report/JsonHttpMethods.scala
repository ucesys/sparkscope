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
