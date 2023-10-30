package com.ucesys.sparkscope.event

import com.ucesys.sparkscope.SparkScopeArgs
import com.ucesys.sparkscope.SparkScopeConfLoader._
import com.ucesys.sparkscope.common.{ExecutorContext, SparkScopeContext, SparkScopeLogger}
import com.ucesys.sparkscope.event.EventLogContextLoader._
import com.ucesys.sparkscope.io.FileReaderFactory
import org.apache.spark.SparkConf

import scala.util.parsing.json.JSON

class EventLogContextLoader(implicit logger: SparkScopeLogger) {
    def load(fileReaderFactory: FileReaderFactory, args: SparkScopeArgs): EventLogContext = {
        val fileReader = fileReaderFactory.getFileReader(args.eventLog)
        val eventLogJsonStrSeq: Seq[String] = fileReader.read(args.eventLog).split("\n").toSeq
        val eventLogJsonSeq = eventLogJsonStrSeq.flatMap(JSON.parseFull(_).map(_.asInstanceOf[Map[String, Any]]))
        val eventLogJsonSeqFiltered = eventLogJsonSeq
          .filter(mapObj => AllEvents.contains(mapObj(ColEvent).asInstanceOf[String]))

        val appStartEvent = eventLogJsonSeqFiltered.find(_(ColEvent) == EventAppStart).map(ApplicationStartEvent(_))
        val appEndEvent = eventLogJsonSeqFiltered.find(_(ColEvent) == EventAppEnd).map(ApplicationEndEvent(_))
        val envUpdateEvent = eventLogJsonSeqFiltered.find(_(ColEvent) == EventEnvUpdate).map(EnvUpdateEvent(_))
        val execAddedEvents = eventLogJsonSeqFiltered.filter(_(ColEvent) == EventExecutorAdded).map(ExecutorAddedEvent(_))
        val execRemovedEvents = eventLogJsonSeqFiltered.filter(_(ColEvent) == EventExecutorRemoved).map(ExecutorRemovedEvent(_))

        val executorMap: Map[String, ExecutorContext] = execAddedEvents.map { execAddedEvent =>
            (
                execAddedEvent.executorId,
                ExecutorContext(
                    executorId=execAddedEvent.executorId,
                    cores=execAddedEvent.cores,
                    addTime=execAddedEvent.ts,
                    removeTime=execRemovedEvents.find(_.executorId == execAddedEvent.executorId).map(_.ts)
                )
            )
        }.toMap

        if (appStartEvent.isEmpty || appStartEvent.get.appId.isEmpty || appStartEvent.get.ts.isEmpty) {
            logger.error(s"Error during parsing of Application Start Event(${appStartEvent})")
            throw new IllegalArgumentException(s"Error during parsing of Application Start Event(${appStartEvent})")
        }

        if (appEndEvent.nonEmpty && appEndEvent.get.ts.isEmpty) {
            logger.error(s"Error during parsing of Application End Event(${appEndEvent})")
            throw new IllegalArgumentException(s"Error during parsing of Application End Event(${appEndEvent})")
        }

        if (envUpdateEvent.isEmpty || envUpdateEvent.get.sparkConf.isEmpty) {
            logger.error(s"Error during parsing of Environment Update Event(${envUpdateEvent})")
            throw new IllegalArgumentException(s"Error during parsing of Environment Update Event(${envUpdateEvent})")
        }

        if (appEndEvent.isEmpty) {
            logger.info("Could not read application end event from eventLog, app might still be running.")
        }

        val sparkConf = new SparkConf(false)
        envUpdateEvent.get.sparkConf.get.foreach { case (key, value) => sparkConf.set(key, value) }

        // Overriding SparkConf with input args if specified
        args.driverMetrics.map(sparkConf.set(SparkScopePropertyDriverMetricsDir, _))
        args.executorMetrics.map(sparkConf.set(SparkScopePropertyExecutorMetricsDir, _))
        args.htmlPath.map(sparkConf.set(SparkScopePropertyHtmlPath, _))

        // App Context
        val appContext = SparkScopeContext(
            appId=appStartEvent.get.appId.get,
            appStartTime=appStartEvent.get.ts.get,
            appEndTime=appEndEvent.flatMap(_.ts),
            executorMap
        )

        EventLogContext(sparkConf, appContext)
    }
}

object EventLogContextLoader {
    val EventAppStart = "SparkListenerApplicationStart"
    val EventAppEnd = "SparkListenerApplicationEnd"
    val EventEnvUpdate = "SparkListenerEnvironmentUpdate"
    val EventExecutorAdded = "SparkListenerExecutorAdded"
    val EventExecutorRemoved = "SparkListenerExecutorRemoved"

    val AllEvents = Seq(EventEnvUpdate, EventAppStart, EventAppEnd,  EventExecutorAdded, EventExecutorRemoved)

    val ColEvent = "Event"
    val ColAppId = "App ID"
    val ColTimeStamp = "Timestamp"
    val ColSparkProps = "Spark Properties"
    val ColExecutorId = "Executor ID"
    val ColExecutorInfo = "Executor Info"
    val ColExecutorCores = "Total Cores"
    val ColExecutorInfoCores = s"${ColExecutorInfo}.${ColExecutorCores}"
}