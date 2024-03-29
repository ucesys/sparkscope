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

package com.ucesys.sparkscope.view.chart

import com.ucesys.sparkscope.common.MetricUtils.ColTs
import com.ucesys.sparkscope.data.DataColumn

object ChartUtils {
    def decreaseDataPoints(tsCol: DataColumn, valueCols: Seq[DataColumn], targetChartPoints: Int): (DataColumn, Seq[DataColumn]) = {
            val originalChartPoints: Long = valueCols.headOption.getOrElse(throw new IllegalArgumentException("Column list cannot be empty!")).size
            val ratio = originalChartPoints.toFloat / targetChartPoints.toFloat
            val newPoints: Seq[Int] = (0 until targetChartPoints)

            val newCols = valueCols.map{ oldCol =>
                val values: Seq[String] = newPoints.map { id =>
                    val from: Int = (id * ratio).toInt
                    val to: Int = (from + ratio).toInt
                    val valuesNotNull = oldCol.values.slice(from, to).filter(_ != "null")

                    valuesNotNull match {
                        case Seq() => "null"
                        case _ => valuesNotNull.map(_.toDouble.toLong).max.toString
                    }
                }
                DataColumn(oldCol.name, values)
            }
        (DataColumn(ColTs, newPoints.map(id => tsCol.values((id * ratio).toInt))), newCols)
    }
}
