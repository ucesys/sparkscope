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

import com.ucesys.sparklens.{QuboleJobListener, asyncReportingEnabled, dumpDataEnabled}
import com.ucesys.sparklens.analyzer.AppAnalyzer
import com.ucesys.sparklens.common.AppContext
import com.ucesys.sparklens.helper.EmailReportHelper
import com.ucesys.sparkscope.io.{CsvHadoopMetricsLoader, CsvHadoopReader, HadoopPropertiesLoader}
import org.apache.spark.SparkConf
import org.apache.spark.scheduler._

class SparkScopeJobListener(sparkConf: SparkConf) extends QuboleJobListener(sparkConf: SparkConf) {

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
          }else {
            //no stages? set it to endTime of the app
            jobMap(x._1).setEndTime(appInfo.endTime)
          }
        }
      })
    val appContext = new AppContext(appInfo,
      appMetrics,
      hostMap,
      executorMap,
      jobMap,
      jobSQLExecIDMap,
      stageMap,
      stageIDToJobID)

    asyncReportingEnabled(sparkConf) match {
      case true => {
        println("Reporting disabled. Will save sparklens data file for later use.")
        dumpData(appContext)
      }
      case false => {
        if (dumpDataEnabled(sparkConf)) {
          dumpData(appContext)
        } else {
          EmailReportHelper.generateReport(appContext.toString(), sparkConf)
        }
        val startSparkLens = System.currentTimeMillis()
        val sparklensResults = AppAnalyzer.startAnalyzers(appContext)
        val durationSparkLens = (System.currentTimeMillis() - startSparkLens) * 1f / 1000f
        sparklensResults.foreach(println)
        println(s"\nSparklens analysis took ${durationSparkLens}s")

        val metricsLoader = new CsvHadoopMetricsLoader(new CsvHadoopReader, appContext, sparkConf, new HadoopPropertiesLoader)
        val sparkScopeRunner = new SparkScopeRunner(appContext, sparkConf, metricsLoader, sparklensResults)
        sparkScopeRunner.run()
      }
    }
  }
}