package com.ucesys.sparkscope.eventlog

import com.ucesys.sparkscope.common.{ExecutorContext, SparkScopeContext, SparkScopeLogger}
import com.ucesys.sparkscope.eventlog.EventLogContextLoader._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

class EventLogContextLoader(implicit logger: SparkScopeLogger) {
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


        val sparkConfMap: Map[String, Any] = try {
            sparkConfDF.collect.map(r => Map(sparkConfDF.columns.zip(r.toSeq):_*)).last
        } catch {
            case ex: Exception =>
                logger.error(s"Error while trying to read spark properties from event log file: ${eventLogPath}" + ex)
                throw new IllegalArgumentException(s"Error while trying to read spark properties from event log file: ${eventLogPath}" + ex, ex)
        }
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

        if (appStart.isEmpty) {
            logger.error("Could not read application start event from eventLog")
            throw new IllegalArgumentException("Could not read application start event from eventLog")
        }

        val appEnd: Option[ApplicationEvent] = eventLogDFNoTasks
          .filter(col(ColEvent) === EventAppEnd)
          .select(col(ColAppId).as("appId"), col(ColTimeStamp).as("ts"))
          .as[ApplicationEvent]
          .collect
          .toSeq
          .headOption

        if (appEnd.isEmpty) {
            logger.info("Could not read application end event from eventLog, app might still be running.")
        }

        // ExecutorMap
        val executorsAddedDF: DataFrame = eventLogDFNoTasks
          .filter(col(ColEvent) === EventExecutorAdded)
          .select(col(ColExecutorId).as("executorId"), col(ColTimeStamp).as("addTime"), col(ColExecutorCores).as("cores"))

        val executorRemovedDF: DataFrame = eventLogDFNoTasks
          .filter(col(ColEvent) === EventExecutorRemoved)
          .select(col(ColExecutorId).as("executorId"), col(ColTimeStamp).as("removeTime"))

        val executorMap: Map[String, ExecutorContext] = executorsAddedDF.join(executorRemovedDF, Seq("executorId"), "left")
          .as[ExecutorContext]
          .collect()
          .toSeq
          .map(x => (x.executorId, x))
          .toMap

        // App Context
        val appContext = SparkScopeContext(
            appId=appStart.get.appId,
            appStartTime=appStart.get.ts,
            appEndTime=appEnd.map(_.ts),
            executorMap
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