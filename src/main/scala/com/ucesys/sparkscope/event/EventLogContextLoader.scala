package com.ucesys.sparkscope.event

import com.ucesys.sparkscope.common.{ExecutorContext, SparkScopeContext, SparkScopeLogger}
import com.ucesys.sparkscope.event.EventLogContextLoader._
import com.ucesys.sparkscope.io.FileReaderFactory

import scala.util.parsing.json.JSON

class EventLogContextLoader(implicit logger: SparkScopeLogger) {
    def load(fileReaderFactory: FileReaderFactory, eventLogPath: String): EventLogContext = {
        val fileReader = fileReaderFactory.getFileReader(eventLogPath)
        val eventLogJsonStrSeq: Seq[String] = fileReader.read(eventLogPath).split("\n").toSeq
        val eventLogJsonSeq = eventLogJsonStrSeq.map(JSON.parseFull(_).get.asInstanceOf[Map[String, Any]])
        val eventLogJsonSeqFiltered = eventLogJsonSeq
          .filter(mapObj => AllEvents.contains(mapObj(ColEvent).asInstanceOf[String]))

        val appStartEvent = eventLogJsonSeqFiltered.find(_(ColEvent) == EventAppStart).map(ApplicationStartEvent(_)).head
        val appEndEvent = eventLogJsonSeqFiltered.find(_(ColEvent) == EventAppEnd).map(ApplicationEndEvent(_))
        val envUpdateEvent = eventLogJsonSeqFiltered.find(_(ColEvent) == EventEnvUpdate).map(EnvUpdateEvent(_)).last
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


        if (appEndEvent.isEmpty) {
            logger.info("Could not read application end event from eventLog, app might still be running.")
        }

        // App Context
        val appContext = SparkScopeContext(
            appId=appStartEvent.appId,
            appStartTime=appStartEvent.ts,
            appEndTime=appEndEvent.map(_.ts),
            executorMap
        )

        EventLogContext(envUpdateEvent.sparkConf, appContext)
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