/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.ucesys.sparkscope.timeline

trait Timeline {
    private var startTime: Option[Long] = None
    private var endTime: Option[Long] = None

    def getEndTime: Option[Long] = endTime
    def getStartTime: Option[Long] = startTime

    private[sparkscope] def setEndTime(time: Long): Unit = {
        endTime = Some(time)
    }

    private[sparkscope] def setEndTime(time: Option[Long]): Unit = {
        endTime = time
    }

    private[sparkscope] def setStartTime(time: Long): Unit = {
        startTime = Some(time)
    }

    def duration: Option[Long] = endTime.flatMap(end => startTime.map(start => end - start))

    def getStartEndTime: Map[String, Long] = Map("startTime" -> startTime.getOrElse(0L), "endTime" -> endTime.getOrElse(0L))

    def getMap: Map[String, _ <: Any]
}
