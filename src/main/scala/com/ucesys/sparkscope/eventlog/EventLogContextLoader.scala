package com.ucesys.sparkscope.eventlog

import com.ucesys.sparklens.common.{AppContext, ApplicationInfo}
import com.ucesys.sparklens.timespan.ExecutorTimeSpan
import com.ucesys.sparkscope.eventlog.EventLogContextLoader._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SparkSession

import scala.collection.mutable


class EventLogContextLoader {
    def load(spark: SparkSession, eventLogPath: String): EventLogContext = {
        import spark.implicits._

        val eventLogDF = spark.read.json(eventLogPath)
        val eventLogDFNoTasks = eventLogDF
          .where(col(ColEvent).isin(AllEvents:_*))
          .select(AllCols.head, AllCols.tail:_*)
          .cache()

        // Spark Conf
        val sparkConfDF = eventLogDFNoTasks
          .where(col(ColEvent) === EventEnvUpdate)
          .select(s"${ColSparkProps}.*")
        val sparkConfMap: Map[String, Any] = sparkConfDF.collect.map(r => Map(sparkConfDF.columns.zip(r.toSeq):_*)).last
        val sparkConf = new SparkConf(false)
        sparkConfMap.foreach{case (key, value) => sparkConf.set(key, value.toString)}

        // ApplicationInfo
        val appStart: Option[ApplicationEvent] = eventLogDFNoTasks
          .filter(col(ColEvent) === EventAppStart)
          .select(col(ColAppId).as("appId"), col(ColTimeStamp).as("ts"))
          .as[ApplicationEvent]
          .collect
          .toSeq
          .headOption

        val appEnd: Option[ApplicationEvent] = eventLogDFNoTasks
          .filter(col(ColEvent) === EventAppEnd)
          .select(col(ColAppId).as("appId"), col(ColTimeStamp).as("ts"))
          .as[ApplicationEvent]
          .collect
          .toSeq
          .headOption

        val appInfo = new ApplicationInfo(appStart.get.appId, appStart.get.ts, appEnd.map(_.ts).getOrElse(0))

        // ExecutorMap
        val executorAddedSeq: Seq[ExecutorEvent] = eventLogDFNoTasks
          .filter(col(ColEvent) === EventExecutorAdded)
          .select(col(ColExecutorId).as("executorId"), col(ColTimeStamp).as("ts"), col(ColExecutorCores).as("cores"))
          .as[ExecutorEvent]
          .collect()
          .toSeq

        val executorRemovedSeq: Seq[ExecutorEvent] = eventLogDFNoTasks
          .filter(col(ColEvent) === EventExecutorRemoved)
          .select(col(ColExecutorId).as("executorId"), col(ColTimeStamp).as("ts"), col(ColExecutorCores).as("cores"))
          .as[ExecutorEvent]
          .collect()
          .toSeq

        val executorMap: mutable.HashMap[String, ExecutorTimeSpan] = mutable.Map(executorAddedSeq.map { execAdded =>
            (execAdded.executorId, ExecutorTimeSpan(execAdded.executorId, "", execAdded.cores.get.toInt, execAdded.ts,  0))
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

    val ColEvent = "Event"
    val ColAppId = "App ID"
    val ColTimeStamp = "Timestamp"
    val ColSparkProps = "Spark Properties"
    val ColExecutorId = "Executor ID"
    val ColExecutorInfo = "Executor Info"
    val ColExecutorCores = "Total cores"
    val ColExecutorInfoCores = s"${ColExecutorInfo}.${ColExecutorCores}"

    val AllCols = Seq(ColEvent, ColAppId, ColTimeStamp, ColSparkProps, ColExecutorId, ColExecutorInfoCores)

}