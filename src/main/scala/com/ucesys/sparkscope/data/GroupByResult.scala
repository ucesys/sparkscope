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

package com.ucesys.sparkscope.data

case class GroupByResult(groupCol: String, aggColName: String, result: Map[String, Seq[String]]) {
    def sum: DataTable = {
        val aggregated = this.result.map { case (groupCol, aggCol) => (groupCol, aggCol.map(_.toDouble).sum) }
        toDataFrame(s"groupBy${groupCol}.avg(${aggColName})", aggregated)
    }

    def max: DataTable = {
        val aggregated = this.result.map { case (groupCol, aggCol) => (groupCol, aggCol.map(_.toDouble).max) }
        toDataFrame(s"groupBy${groupCol}.avg(${aggColName})", aggregated)
    }

    def min: DataTable = {
        val aggregated = this.result.map { case (groupCol, aggCol) => (groupCol, aggCol.map(_.toDouble).min) }
        toDataFrame(s"groupBy${groupCol}.avg(${aggColName})", aggregated)
    }

    def avg: DataTable = {
        val aggregated = this.result.map { case (groupCol, seq) => (groupCol, seq.map(_.toDouble).sum / seq.length) }
        toDataFrame(s"groupBy${groupCol}.avg(${aggColName})", aggregated)
    }

    def count: DataTable = {
        val aggregated = this.result.map { case (groupCol, seq) => (groupCol, seq.length.toDouble) }
        DataTable(s"groupBy${groupCol}.cnt", columns = Seq(DataColumn(groupCol, aggregated.keys.toSeq), DataColumn("cnt", aggregated.values.toSeq.map(x => f"${x}%.4f"))))
    }

    def toDataFrame(name: String, aggResult: Map[String, Double]): DataTable = {
        DataTable(name, columns = Seq(DataColumn(groupCol, aggResult.keys.toSeq), DataColumn(aggColName, aggResult.values.toSeq.map(x => f"${x}%.4f"))))
    }
}