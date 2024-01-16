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

case class SeriesColor(borderColor: String, backgroundColor: String)

object SeriesColor {
    val Green = SeriesColor("#3cba9f", "#71d1bd")
    val Blue = SeriesColor("#3e95cd", "#7bb6dd")
    val Yellow = SeriesColor("#f1bd3e", "#f4cd6e")
    val Purple = SeriesColor("#4B0082", "#9370DB")
    val Orange = SeriesColor("#ff8c00", "#ed9121")
    val Red = SeriesColor("#CD5C5C", "#F08080")

    val AllColors: Seq[SeriesColor] = Seq(Green, Blue, Yellow, Red, Purple, Orange)

    def randomColorModulo(id: Int, colors: Seq[SeriesColor] = AllColors): SeriesColor = colors(id % colors.length)
}
