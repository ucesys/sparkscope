package com.ucesys.sparkscope.io

import com.ucesys.sparklens.common.{AppContext, ApplicationInfo}
import com.ucesys.sparklens.timespan.ExecutorTimeSpan
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col

import scala.collection.mutable

case class EventLogContext(sparkConf: SparkConf, appContext: AppContext)

object EventLogContext {

    val Events = Seq("SparkListenerEnvironmentUpdate", "SparkListenerApplicationStart")
    def load(eventLogPath: String): EventLogContext = {
        val spark = SparkSession.builder().master("local").getOrCreate()
        val eventLogDF = spark.read.json(eventLogPath)
        val eventLogDFNoTasks = eventLogDF.filter(!col("Event").isin("SparkListenerTaskStart", "SparkListenerTaskEnd")).cache()
        eventLogDFNoTasks.printSchema()

        // Spark Conf
        val sparkConfDF = eventLogDFNoTasks.filter(col("Event").isin("SparkListenerEnvironmentUpdate")).select("Spark Properties.*")
        val sparkConfMap: Map[String, Any] = sparkConfDF.collect.map(r => Map(sparkConfDF.columns.zip(r.toSeq):_*)).last
        val sparkConf = new SparkConf(false)
        sparkConfMap.foreach{case (key, value) => sparkConf.set(key, value.toString)}
        sparkConf.getAll.foreach(println)

        // ApplicationInfo
        eventLogDFNoTasks.select("App ID", "Event", "Timestamp").show(false)
        val appInfoDF: DataFrame = eventLogDFNoTasks
          .filter(col("Event").isin("SparkListenerApplicationStart"))
          .select("App ID", "Timestamp")

        val appInfoMap: Map[String, Any] = appInfoDF
          .collect
          .map(r => Map(appInfoDF.columns.zip(r.toSeq):_*))
          .last
        appInfoMap.foreach(println)

        val appEndDF: DataFrame = eventLogDFNoTasks
          .filter(col("Event").isin("SparkListenerApplicationEnd"))
          .select("Timestamp")

        val appEndMap: Map[String, Any] = appEndDF
          .collect
          .map(r => Map(appEndDF.columns.zip(r.toSeq): _*))
          .last

        appEndMap.foreach(println)

        val appInfo = new ApplicationInfo(appInfoMap("App ID").toString, appInfoMap("Timestamp").toString.toLong, appEndMap("Timestamp").toString.toLong)
        println(appInfo)

        // ExecutorMap
        // TODO
        val executorMap: mutable.HashMap[String, ExecutorTimeSpan] = ???

        // AppContext
        val appContext = new AppContext(
            appInfo,
            null,
            null,
            executorMap,
            null,
            null,
            null,
            null
        )
        ???

        EventLogContext(sparkConf, appContext)
    }
}