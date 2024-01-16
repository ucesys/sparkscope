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

package com.ucesys.sparkscope.io.metrics

case class S3Location(bucketName: String, path: String) {
    def getUrl: String = s"s3://${bucketName}/${path}"
}

object S3Location {
    def apply(s3Path: String): S3Location = s3Path.split("/").toSeq.filter(_.nonEmpty) match {
        case Seq(fs, bucketName, metricsDirTail @_*) if metricsDirTail.nonEmpty => S3Location(bucketName, metricsDirTail.mkString("/"))
        case _ => throw new IllegalArgumentException(s"Couldn't parse s3 directory path: ${s3Path}")
    }
}
