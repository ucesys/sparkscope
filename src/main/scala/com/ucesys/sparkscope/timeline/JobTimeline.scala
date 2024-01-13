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

import org.apache.spark.scheduler.{SparkListenerJobEnd, SparkListenerJobStart}

case class JobTimeline(jobID: Long, startTime: Long, stages: Seq[Int], endTime: Option[Long] = None) {
    def duration: Option[Long] = endTime.map(end => end - startTime)

    def end(jobEnd: SparkListenerJobEnd): JobTimeline = {
        this.copy(endTime = Some(jobEnd.time))
    }
}

object JobTimeline {
    def apply(jobStart: SparkListenerJobStart): JobTimeline = {
        new JobTimeline(
            jobStart.jobId,
            jobStart.time,
            jobStart.stageInfos.map(_.stageId)
        )
    }
}
