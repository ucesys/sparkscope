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

case class AggValue(var sum: Long, var min: Long, var max: Long, var mean: Double, var count: Long) {
    def aggregate(newValue: Long): Unit = {
        if (count == 0L) {
            this.min = newValue
            this.max = newValue
            this.mean = newValue
        } else {
            val delta: Double = newValue - this.mean

            this.min = math.min(this.min, newValue)
            this.max = math.max(this.max, newValue)
            this.mean += delta / (count + 1)
        }
        this.sum += newValue
        this.count += 1
    }
}

object AggValue {
    def empty: AggValue = AggValue(0, 0, 0, 0, 0)
}
