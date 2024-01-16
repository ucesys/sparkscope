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

package com.ucesys.sparkscope.common

import com.ucesys.sparkscope.common.MemorySize.BytesInMB

case class MemorySize(sizeInBytes: Long) {
    def multiply(by: Float): MemorySize = MemorySize((this.sizeInBytes*by).toLong)

    def toMB: Long = sizeInBytes/BytesInMB

    def max(other: MemorySize): MemorySize = {
        if (this.sizeInBytes > other.sizeInBytes) {
            this
        } else {
            other
        }
    }

    def >(other: MemorySize): Boolean = {
        if (this.sizeInBytes > other.sizeInBytes) {
            true
        } else {
            false
        }
    }
}

object MemorySize {
    val BytesInMB: Long = 1024*1024
    val BytesInGB: Long = 1024*1024*1024
    def fromMegaBytes(megaBytes: Long): MemorySize = MemorySize(megaBytes*BytesInMB)
    def fromGigaBytes(gigaBytes: Long): MemorySize = MemorySize(gigaBytes*BytesInGB)

    def fromStr(sizeStr: String)(implicit logger: SparkScopeLogger): MemorySize = {
        try {
            sizeStr match {
                case s if s.contains("m") => MemorySize.fromMegaBytes(s.replace("m", "").toFloat.toLong)
                case s if s.contains("g") => MemorySize.fromGigaBytes(s.replace("g", "").toFloat.toLong)
                case _ => MemorySize.fromMegaBytes(sizeStr.toFloat.toLong)
            }
        } catch {
            case ex: Exception => logger.warn(s"Could not parse ${sizeStr} into memory size. " + ex, this.getClass); MemorySize(0)
        }
    }
}
