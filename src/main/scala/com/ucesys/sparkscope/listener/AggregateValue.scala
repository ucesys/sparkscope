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

package com.ucesys.sparkscope.listener

import org.json4s.DefaultFormats
import org.json4s.JsonAST.JValue

/*
Keeps track of min max sum mean and variance for any metric at any level
Reference to incremental updates of variance:
https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_Online_algorithm
 */

class AggregateValue {
    var value: Long = 0L
    var min: Long = Long.MaxValue
    var max: Long = Long.MinValue
    var mean: Double = 0.0
    var variance: Double = 0.0
    var m2: Double = 0.0

    override def toString(): String = {
        s"""{
           | "value": ${value},
           | "min": ${min},
           | "max": ${max},
           | "mean": ${mean},
           | "m2": ${m2}
           | "variance": ${variance}
       }""".stripMargin
    }

    def getMap(): Map[String, Any] = {
        Map("value" -> value,
            "min" -> min,
            "max" -> max,
            "mean" -> mean,
            "m2" -> m2,
            "variance" -> variance)
    }
}

object AggregateValue {
    def getValue(json: JValue): AggregateValue = {
        implicit val formats = DefaultFormats

        val value = new AggregateValue
        value.value = (json \ "value").extract[Long]
        value.min = (json \ "min").extract[Long]
        value.max = (json \ "max").extract[Long]
        value.mean = (json \ "mean").extract[Double]
        value.variance = (json \ "variance").extract[Double]
        //making it optional for backward compatibility with sparklens.json files
        value.m2 = (json \ "m2").extractOrElse[Double](0.0)
        value
    }
}
