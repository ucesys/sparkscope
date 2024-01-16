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

package com.ucesys.sparkscope.view.warning

import com.ucesys.sparkscope.TestHelpers._
import com.ucesys.sparkscope.agg.{AggValue, TaskAggMetrics}
import com.ucesys.sparkscope.common.MemorySize
import com.ucesys.sparkscope.io.writer.LocalFileWriter
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FunSuite, GivenWhenThen}

import java.nio.file.{Files, Paths}

class DiskSpillWarningSuite extends FunSuite with MockFactory with BeforeAndAfterAll with GivenWhenThen {
    override def beforeAll(): Unit = Files.createDirectories(Paths.get(TestDir))
    val fileWriter = new LocalFileWriter

    val taskDurationInSec = 29058
    val gcTimeInSecLow = 1149
    val gcTimeInSecHigh = 11490

    test("DiskSpillWarning no spill") {
        Given("TaskAggMetrics with no data spills")
        val taskAggMetrics = TaskAggMetrics(diskBytesSpilled = AggValue(0, 0, 0, 0, 0), memoryBytesSpilled = AggValue(0, 0, 0, 0, 0))

        When("creating warning")
        val warning = DiskSpillWarning(taskAggMetrics)

        Then("no warning should be returned")
        assert(warning.isEmpty)
    }

    test("DiskSpillWarning spill") {
        Given("TaskAggMetrics with data spills")
        val taskAggMetrics = TaskAggMetrics(
            diskBytesSpilled = AggValue(MemorySize.fromMegaBytes(500).sizeInBytes, 0, 0, 0, 0),
            memoryBytesSpilled = AggValue(MemorySize.fromGigaBytes(5).sizeInBytes, 0, 0, 0, 0)
        )

        When("creating warning")
        val warning = DiskSpillWarning(taskAggMetrics)

        Then("warning should be returned")
        assert(warning.nonEmpty)

        And("warning message should be correct")
        assert(warning.get.toString == "Data spills from memory to disk occured. Total size spilled to disk: 500MB(in-memory size: 5120MB)")

    }
}
