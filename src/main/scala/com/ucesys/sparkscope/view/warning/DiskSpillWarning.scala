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

package com.ucesys.sparkscope.view.warning

import com.ucesys.sparkscope.agg.TaskAggMetrics
import com.ucesys.sparkscope.common.MemorySize

case class DiskSpillWarning(diskBytesSpilled: MemorySize, memoryBytesSpilled: MemorySize) extends Warning {
    override def toString: String = {
        f"Data spills from memory to disk occured. Total size spilled to disk: ${diskBytesSpilled.toMB}MB(in-memory size: ${memoryBytesSpilled.toMB}MB)"
    }
}

object DiskSpillWarning {
    def apply(taskAggMetrics: TaskAggMetrics): Option[DiskSpillWarning] = {
        if(taskAggMetrics.diskBytesSpilled.sum > 0L || taskAggMetrics.memoryBytesSpilled.sum > 0L) {
            Some(new DiskSpillWarning(MemorySize(taskAggMetrics.diskBytesSpilled.sum), MemorySize(taskAggMetrics.memoryBytesSpilled.sum)))
        } else {
            None
        }
    }
}