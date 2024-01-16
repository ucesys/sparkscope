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

package com.ucesys.sparkscope.io.reader

import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.amazonaws.services.s3.model.S3Object
import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.io.metrics.S3Location

import scala.io.{BufferedSource, Source}

class S3FileReader(s3: AmazonS3)(implicit logger: SparkScopeLogger)  extends FileReader {
    def read(url: String): String = {
        val s3Location = S3Location(url)
        println(s"Reading url: ${url}, bucket: ${s3Location.bucketName}, key: ${s3Location.path}")
        try {
            val s3Object: S3Object = s3.getObject(s3Location.bucketName, s3Location.path)
            val eventLogSource: BufferedSource = Source.fromInputStream(s3Object.getObjectContent)
            eventLogSource.getLines().mkString("\n")
        } catch {
            case ex: Exception =>
                logger.error(s"Error while reading file from ${url}", this.getClass)
                throw ex
        }
    }
}

object S3FileReader {
    def apply(region: String)(implicit logger: SparkScopeLogger) : S3FileReader = {
        logger.info(s"Creating s3 client for ${region} region", this.getClass)
        val s3: AmazonS3 = AmazonS3ClientBuilder
          .standard
          .withRegion(region)
          .build

        new S3FileReader(s3)
    }
}

