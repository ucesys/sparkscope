
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

case class CSVMetric(name: String, csvFileStr: String)
case class CSVExecutorMetrics(executorId: Int, metrics: Seq[CSVMetric])

case class CSVExecutorJoinedMetrics(executorId: Int, metrics: String)

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


    val executorCsvMetrics: Seq[CSVExecutorMetrics] = ( 0 until ac.executorMap.size).map { executorId =>
       val csvMetrics = metrics.map { metric =>
        val metricsFilePath = s"${csvMetricsDir}/${appContext.appInfo.applicationID}.${executorId}.${metric}.csv"
        val csvFileStr = readStr(metricsFilePath).replace("value", metric)
        out.println(s"[SparkScope] Reading ${metric} metric for executor=${executorId} from " + metricsFilePath)
        CSVMetric(metric, csvFileStr)
      }
      CSVExecutorMetrics(executorId, csvMetrics)
    }
    executorCsvMetrics.foreach{ executorMetrics => executorMetrics.metrics.foreach{ metric =>
      out.println(s"[SparkScope] Displaying ${metric.name} metric for executor=${executorMetrics.executorId}:")
      out.println(metric.csvFileStr)
    }}

    val joinedExecutorMetrics: Seq[CSVExecutorJoinedMetrics] = executorCsvMetrics.map { executorMetrics =>
      CSVExecutorJoinedMetrics(executorMetrics.executorId, joinCsvMetrics(executorMetrics.metrics, ","))
    }

    joinedExecutorMetrics.foreach { joinedExecutorMetrics =>
      out.println(s"\n[SparkScope] Displaying joined metrics for executor=${joinedExecutorMetrics.executorId}")
      out.println(joinedExecutorMetrics.metrics)
    }

    val joinedExecutorMetricsWithExecId = joinedExecutorMetrics.map { joinedExecutorMetrics =>
      addExecIdColumn(joinedExecutorMetrics.metrics, joinedExecutorMetrics.executorId, ",")
    }

    joinedExecutorMetricsWithExecId.foreach { joinedExecutorMetrics =>
      out.println(s"\n[SparkScope] Displaying joined metrics with Executor Id for executor")
      out.println(joinedExecutorMetrics)
    }

    val unionedCsvMetrics = unionCsvMetrics(joinedExecutorMetricsWithExecId)
    out.println(s"\n[SparkScope] Displaying unioned metrics for all executors")
    out.println(unionedCsvMetrics)

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

  def joinCsvMetrics(executorCsvMetrics: Seq[CSVMetric], delimeter: String): String = {
    var outMergedFileRows: Seq[String] = executorCsvMetrics.head.csvFileStr.split("\n")
    val baselineTimeCol = outMergedFileRows.map(_.split(delimeter).toSeq.head)

    executorCsvMetrics.tail.foreach { executorCsvMetric =>
      val inMergedFileRows = executorCsvMetric.csvFileStr.split("\n")
      val timeCol = inMergedFileRows.map(_.split(delimeter).toSeq.head)
      val metricCol = inMergedFileRows.map(_.split(delimeter).toSeq.last)
      if (baselineTimeCol.containsSlice(timeCol)) {
        timeCol.foreach(println)
      }
      for (rowId <- 0 until outMergedFileRows.length) {
        val mergedRow: String = outMergedFileRows(rowId).concat(delimeter).concat(metricCol(rowId))
        outMergedFileRows = outMergedFileRows.updated(rowId, mergedRow)
      }
    }
    val mergedCsvStr = outMergedFileRows.mkString("\n")
    mergedCsvStr
  }
  
  def unionCsvMetrics(joinedExecutorMetricsWithExecId: Seq[String]): String = {
    // TODO CHECK HEADERS
    val header = joinedExecutorMetricsWithExecId.head.split("\n").head
    (Seq(header) ++ joinedExecutorMetricsWithExecId.flatMap(_.split("\n").tail)).mkString("\n")
  }

  def addExecIdColumn(metricsStr: String, executorId: Int, delimeter: String): String = {
    val rows =  metricsStr.split("\n")
    val header = "executorId" + delimeter + rows.head
    val values = rows.tail.map(row => executorId + delimeter + row)
    (Seq(header) ++ values).mkString("\n")
  }
}
