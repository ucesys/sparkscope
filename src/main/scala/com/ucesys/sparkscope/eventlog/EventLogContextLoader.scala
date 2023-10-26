package com.ucesys.sparkscope.eventlog

import com.ucesys.sparklens.common.{AppContext, ApplicationInfo}
import com.ucesys.sparklens.timespan.ExecutorTimeSpan
import com.ucesys.sparkscope.eventlog.EventLogContextLoader.{AllEvents, EventAppEnd, EventAppStart, EventEnvUpdate, EventExecutorAdded, EventExecutorRemoved}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession

import scala.collection.mutable


class EventLogContextLoader {
    def load(spark: SparkSession, eventLogPath: String): EventLogContext = {
        import spark.implicits._

        val eventLogDF = spark.read.json(eventLogPath)
        val eventLogDFNoTasks = eventLogDF.filter(col("Event").isin(AllEvents:_*)).cache()

        // Spark Conf
        val sparkConfDF = eventLogDFNoTasks.filter(col("Event") === EventEnvUpdate).select("Spark Properties.*")
        val sparkConfMap: Map[String, Any] = sparkConfDF.collect.map(r => Map(sparkConfDF.columns.zip(r.toSeq):_*)).last
        val sparkConf = new SparkConf(false)
        sparkConfMap.foreach{case (key, value) => sparkConf.set(key, value.toString)}

        // ApplicationInfo
        val appStart: Option[ApplicationEvent] = eventLogDFNoTasks
          .filter(col("Event") === EventAppStart)
          .select(col("App ID").as("appId"), col("Timestamp").as("ts"))
          .as[ApplicationEvent]
          .collect
          .toSeq
          .headOption

        val appEnd: Option[ApplicationEvent] = eventLogDFNoTasks
          .filter(col("Event") === EventAppEnd)
          .select(col("App ID").as("appId"), col("Timestamp").as("ts"))
          .as[ApplicationEvent]
          .collect
          .toSeq
          .headOption

        val appInfo = new ApplicationInfo(appStart.get.appId, appStart.get.ts, appEnd.map(_.ts).getOrElse(0))

        // ExecutorMap
        val executorAddedSeq: Seq[ExecutorEvent] = eventLogDFNoTasks
          .filter(col("Event") === EventExecutorAdded)
          .select(col("Executor ID").as("executorId"), col("Timestamp").as("ts"))
          .as[ExecutorEvent]
          .collect()
          .toSeq

        val executorRemovedSeq: Seq[ExecutorEvent] = eventLogDFNoTasks
          .filter(col("Event") === EventExecutorRemoved)
          .select(col("Executor ID").as("executorId"), col("Timestamp").as("ts"))
          .as[ExecutorEvent]
          .collect()
          .toSeq

        val executorMap: mutable.HashMap[String, ExecutorTimeSpan] = mutable.Map(executorAddedSeq.map { execAdded =>
            (execAdded.executorId, ExecutorTimeSpan(execAdded.executorId, "", 0, execAdded.ts,  0))
        }: _*).asInstanceOf[mutable.HashMap[String, ExecutorTimeSpan]]

        executorRemovedSeq.foreach{ execRemoved =>
            executorMap(execRemoved.executorId).setEndTime(execRemoved.ts)
        }

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

        EventLogContext(sparkConf, appContext)
    }
}

object EventLogContextLoader {
    val EventEnvUpdate = "SparkListenerEnvironmentUpdate"
    val EventAppStart = "SparkListenerApplicationStart"
    val EventAppEnd = "SparkListenerApplicationEnd"
    val EventExecutorAdded = "SparkListenerExecutorAdded"
    val EventExecutorRemoved = "SparkListenerExecutorRemoved"

    val AllEvents = Seq(EventEnvUpdate, EventAppStart, EventAppEnd,  EventExecutorAdded, EventExecutorRemoved)
}