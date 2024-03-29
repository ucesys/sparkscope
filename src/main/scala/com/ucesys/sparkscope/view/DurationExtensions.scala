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

package com.ucesys.sparkscope.view

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.DurationInt

object DurationExtensions {
    implicit class FiniteDurationExtensions(duration: FiniteDuration) {
        def durationStr: String = {
            duration match {
                case duration if duration < 1.minutes => s"${duration.toSeconds.toString}s"
                case duration if duration < 1.hours => s"${duration.toMinutes % 60}min ${duration.toSeconds % 60}s"
                case _ => s"${duration.toHours}h ${duration.toMinutes % 60}min ${duration.toSeconds % 60}s"
            }
        }
    }

}
