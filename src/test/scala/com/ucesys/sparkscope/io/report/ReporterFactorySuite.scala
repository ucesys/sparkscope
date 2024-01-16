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

package com.ucesys.sparkscope.io.report

import com.ucesys.sparkscope.common.{AppContext, MemorySize, SparkScopeConf, SparkScopeLogger}
import com.ucesys.sparkscope.{SuiteDirectoryUtils}
import org.apache.spark.SparkConf
import org.scalamock.scalatest.MockFactory
import org.scalatest.matchers.must.Matchers.{a, convertToAnyMustWrapper}
import org.scalatest.{BeforeAndAfterAll, FunSuite, GivenWhenThen}

class ReporterFactorySuite extends FunSuite with MockFactory with BeforeAndAfterAll with GivenWhenThen with SuiteDirectoryUtils {
    implicit val logger: SparkScopeLogger = new SparkScopeLogger

    val emptySparkScopeConf = SparkScopeConf(
        "",
        "",
        None,
        None,
        None,
        None,
        "",
        None,
        None,
        mock[MemorySize],
        mock[MemorySize],
        mock[SparkConf]
    )


    test("ReporterFactory empty") {
        Given("ReporterFactory and 0 reporters configured")
        val reporterFactory = new ReporterFactory

        When("calling reporterFactory.get")
        val reporters = reporterFactory.get(mock[AppContext], emptySparkScopeConf)

        Then("0 reporters are returned")
        assert(reporters.isEmpty)
    }

    test("ReporterFactory diagnostics reporter") {
        Given("ReporterFactory and diagnostics reporter configured")
        val reporterFactory = new ReporterFactory

        When("calling reporterFactory.get")
        val reporters = reporterFactory.get(
            mock[AppContext],
            emptySparkScopeConf.copy(diagnosticsUrl = Some("myhost.com"))
        )

        Then("diagnostics reporter is returned")
        assert(reporters.length == 1)
        reporters.head mustBe a[JsonHttpDiagnosticsReporter]
    }

    test("ReporterFactory json reporter") {
        Given("ReporterFactory and json reporter configured")
        val reporterFactory = new ReporterFactory

        When("calling reporterFactory.get")
        val reporters = reporterFactory.get(
            mock[AppContext],
            emptySparkScopeConf.copy(jsonReportPath = Some("/path/to/json/report"))
        )

        Then("json reporter is returned")
        assert(reporters.length == 1)
        reporters.head mustBe a[JsonFileReporter]
    }

    test("ReporterFactory html reporter") {
        Given("ReporterFactory and html reporter configured")
        val reporterFactory = new ReporterFactory

        When("calling reporterFactory.get")
        val reporters = reporterFactory.get(
            mock[AppContext],
            emptySparkScopeConf.copy(htmlReportPath = Some("/path/to/html/report"))
        )

        Then("html reporter is returned")
        assert(reporters.length == 1)
        reporters.head mustBe a[HtmlFileReporter]
    }

    test("ReporterFactory json http reporter") {
        Given("ReporterFactory and json http reporter configured")
        val reporterFactory = new ReporterFactory

        When("calling reporterFactory.get")
        val reporters = reporterFactory.get(
            mock[AppContext],
            emptySparkScopeConf.copy(jsonReportServer = Some("myHost.com"))
        )

        Then("json http reporter is returned")
        assert(reporters.length == 1)
        reporters.head mustBe a[JsonHttpReporter]
    }

    test("ReporterFactory 4 reporters") {
        Given("ReporterFactory and 4 reporters configured")
        val reporterFactory = new ReporterFactory

        When("calling reporterFactory.get")
        val reporters = reporterFactory.get(
            mock[AppContext],
            emptySparkScopeConf.copy(
                diagnosticsUrl = Some("myHost.com"),
                jsonReportPath = Some("/path/to/json/report"),
                htmlReportPath = Some("/path/to/html/report"),
                jsonReportServer = Some("myHost.com")
            )
        )

        Then("4 reporters are returned")
        assert(reporters.length == 4)
    }
}
