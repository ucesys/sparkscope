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

package com.ucesys.sparkscope.agg

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.TaskInfo

case class TaskAggMetrics(shuffleWriteTime: AggValue = AggValue.empty,
                          shuffleWriteBytesWritten: AggValue = AggValue.empty,
                          shuffleWriteRecordsWritten: AggValue = AggValue.empty,
                          shuffleReadFetchWaitTime: AggValue = AggValue.empty,
                          shuffleReadBytesRead: AggValue = AggValue.empty,
                          shuffleReadRecordsRead: AggValue = AggValue.empty,
                          shuffleReadLocalBlocks: AggValue = AggValue.empty,
                          shuffleReadRemoteBlocks: AggValue = AggValue.empty,
                          inputBytesRead: AggValue = AggValue.empty,
                          outputBytesWritten: AggValue = AggValue.empty,
                          jvmGCTime: AggValue = AggValue.empty,
                          executorRuntime: AggValue = AggValue.empty,
                          resultSize: AggValue = AggValue.empty,
                          memoryBytesSpilled: AggValue = AggValue.empty,
                          diskBytesSpilled: AggValue = AggValue.empty,
                          peakExecutionMemory: AggValue = AggValue.empty,
                          taskDuration: AggValue = AggValue.empty) {

    def aggregate(tm: TaskMetrics, ti: TaskInfo): Unit = {
        this.shuffleWriteTime.aggregate(tm.shuffleWriteMetrics.writeTime)
        this.shuffleWriteBytesWritten.aggregate(tm.shuffleWriteMetrics.bytesWritten)
        this.shuffleWriteRecordsWritten.aggregate(tm.shuffleWriteMetrics.recordsWritten)

        this.shuffleReadFetchWaitTime.aggregate(tm.shuffleReadMetrics.fetchWaitTime)
        this.shuffleReadBytesRead.aggregate(tm.shuffleReadMetrics.totalBytesRead)
        this.shuffleReadRecordsRead.aggregate(tm.shuffleReadMetrics.recordsRead)
        this.shuffleReadLocalBlocks.aggregate(tm.shuffleReadMetrics.localBlocksFetched)
        this.shuffleReadRemoteBlocks.aggregate(tm.shuffleReadMetrics.remoteBlocksFetched)

        this.inputBytesRead.aggregate(tm.inputMetrics.bytesRead)
        this.outputBytesWritten.aggregate(tm.outputMetrics.bytesWritten)

        this.jvmGCTime.aggregate(tm.jvmGCTime)
        this.executorRuntime.aggregate(tm.executorRunTime)
        this.resultSize.aggregate(tm.resultSize)
        this.memoryBytesSpilled.aggregate(tm.memoryBytesSpilled)
        this.diskBytesSpilled.aggregate(tm.diskBytesSpilled)
        this.peakExecutionMemory.aggregate(tm.peakExecutionMemory)

        this.taskDuration.aggregate(ti.duration)
    }
}
