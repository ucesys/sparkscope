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

package com.ucesys.sparkscope

import com.ucesys.sparklens.QuboleJobListener
import com.ucesys.sparklens.analyzer.AppAnalyzer
import com.ucesys.sparklens.common.AppContext
import com.ucesys.sparkscope.io.{MetricsLoaderFactory, PropertiesLoaderFactory, ReportGeneratorFactory}
import com.ucesys.sparkscope.utils.SparkScopeLogger
import org.apache.spark.SparkConf
import org.apache.spark.scheduler._

class SparkScopeJobListener(sparkConf: SparkConf) extends QuboleJobListener(sparkConf: SparkConf) {

    implicit val logger = SparkScopeLogger.get

    override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
        //println(s"Application ${appInfo.applicationID} ended at ${applicationEnd.time}")
        appInfo.endTime = applicationEnd.time

        //Set end times for the jobs for which onJobEnd event was missed
        jobMap.foreach(x => {
            if (jobMap(x._1).endTime == 0) {
                //Lots of computations go wrong if we don't have
                //application end time
                //set it to end time of the stage that finished last
                if (!x._2.stageMap.isEmpty) {
                    jobMap(x._1).setEndTime(x._2.stageMap.map(y => y._2.endTime).max)
                } else {
                    //no stages? set it to endTime of the app
                    jobMap(x._1).setEndTime(appInfo.endTime)
                }
            }
        })

        val appContext = new AppContext(
            appInfo,
            appMetrics,
            hostMap,
            executorMap,
            jobMap,
            jobSQLExecIDMap,
            stageMap,
            stageIDToJobID
        )

        val sparklensResults: Seq[String] = try {
            AppAnalyzer.startAnalyzers(appContext)
        } catch {
            case ex: Exception =>
                println("Sparklens has thrown an exception" + ex, ex)
                Seq.empty
        }

        val sparkScopeRunner = new SparkScopeRunner(
            appContext,
            new SparkScopeConfLoader(sparkConf, new PropertiesLoaderFactory),
            new MetricsLoaderFactory,
            new ReportGeneratorFactory,
            sparklensResults
        )
        sparkScopeRunner.run()
    }
}