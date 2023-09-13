
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
package com.qubole.sparklens.analyzer

import com.qubole.sparklens.common.AppContext
import com.qubole.sparklens.helper.HDFSConfigHelper
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf

import java.io.InputStreamReader
import java.net.URI
import java.util.Properties
import scala.collection.mutable

class ExecutorMetricsAnalyzer(sparkConf: SparkConf) extends  AppAnalyzer {

  def analyze(appContext: AppContext, startTime: Long, endTime: Long): String = {
    val ac = appContext.filterByStartAndEndTime(startTime, endTime)
    val out = new mutable.StringBuilder()

    out.println("           ____              __    ____")
    out.println("          / __/__  ___ _____/ /__ / __/_ ___  ___  ___")
    out.println("         _\\ \\/ _ \\/ _ `/ __/  '_/_\\ \\/_ / _ \\/ _ \\/__/ ")
    out.println("        /___/ .__/\\_,_/_/ /_/\\_\\/___/\\__\\_,_/ .__/\\___/")
    out.println("           /_/                             /_/ ")

    // TODO
    //    1. Find metrics.properties path:
    //      1.1 Check if spark conf contains spark.metrics.conf property
    //      1.2 Otherwise set to $SPARK_HOME/conf/metrics.properties
    //    2. Try to open metrics.properties
    //      2.1 If doesn't exist report warning
    //    3. Try to read CSV_DIR as *.sink.csv.directory or use default /tmp
    //    4. Try to read $CSV_DIR/<APP-ID>-METRIC.csv


    // 1
    sparkConf.getAll.foreach(println)
    val sparkHome = sparkConf.get("spark.home", sys.env.getOrElse("SPARK_HOME", sys.env("PWD")))
    out.println("[SparkScope] Spark home: " + sparkHome)
    val defaultMetricsPropsPath = sparkHome + "/conf/metrics.properties"
    val metricsPropertiesPath = sparkConf.get("spark.metrics.conf", defaultMetricsPropsPath)
    out.println("[SparkScope] Trying to read metrics.properties file from " + metricsPropertiesPath)

    // 2
    val fs = FileSystem.get(new URI(metricsPropertiesPath), HDFSConfigHelper.getHadoopConf(None))
    val path = new Path(metricsPropertiesPath)
    val fis = new InputStreamReader(fs.open(path))
    val prop = new Properties();
    prop.load(fis);

    // 3
    val csvMetricsDir = prop.getProperty("*.sink.csv.directory","/tmp/")
    out.println("[SparkScope] Trying to read csv metrics from " + csvMetricsDir)

    // 4
    val metrics = Seq("jvm.total.used", "jvm.total.max", "jvm.heap.usage", "jvm.heap.used", "jvm.non-heap.usage", "jvm.non-heap.used")

    metrics.foreach{ metric =>
      for (executorId <- 0 until ac.executorMap.size) {
        val metricsFilePath = s"${csvMetricsDir}/${appContext.appInfo.applicationID}.${executorId}.${metric}.csv"
        val csvFileStr = readStr(metricsFilePath)
        out.println(s"[SparkScope] Reading ${metric} metric for executor=${executorId} from " + metricsFilePath)
        out.println(csvFileStr)
      }
    }

    out.toString()
  }

  def readStr(pathStr: String): String = {
    println(pathStr)
    val fs = FileSystem.get(new URI(pathStr), HDFSConfigHelper.getHadoopConf(None))
    val path = new Path(pathStr)
    val byteArray = new Array[Byte](fs.getFileStatus(path).getLen.toInt)
    fs.open(path).readFully(byteArray)
    (byteArray.map(_.toChar)).mkString
  }
}
