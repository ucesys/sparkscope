package com.ucesys.sparkscope.event

import com.ucesys.sparkscope.SparkScopeArgs
import com.ucesys.sparkscope.SparkScopeConfLoader._
import com.ucesys.sparkscope.common.{ExecutorContext, SparkScopeContext, SparkScopeLogger, StageContext}
import com.ucesys.sparkscope.event.EventLogContextLoader._
import com.ucesys.sparkscope.io.file.FileReaderFactory
import org.apache.spark.SparkConf

import scala.util.parsing.json.JSON

class EventLogContextLoader(implicit logger: SparkScopeLogger) {
    def load(fileReaderFactory: FileReaderFactory, args: SparkScopeArgs): EventLogContext = {
        val fileReader = fileReaderFactory.getFileReader(args.eventLog)
        val eventLogJsonStrSeq: Seq[String] = fileReader.read(args.eventLog).split("\n").toSeq
        logger.info(s"Loaded ${eventLogJsonStrSeq.length} events")

        val eventLogJsonSeq = eventLogJsonStrSeq.flatMap(JSON.parseFull(_).map(_.asInstanceOf[Map[String, Any]]))
        logger.info(s"Parsed ${eventLogJsonSeq.length} events")

        val eventLogJsonSeqFiltered = eventLogJsonSeq.filter(mapObj => AllEvents.contains(mapObj(ColEvent).asInstanceOf[String]))
        logger.info(s"Filtered ${eventLogJsonSeqFiltered.length} events")

        val appStartEvent = eventLogJsonSeqFiltered.find(_(ColEvent) == EventAppStart).map(ApplicationStartEvent(_))
        val appEndEvent = eventLogJsonSeqFiltered.find(_(ColEvent) == EventAppEnd).map(ApplicationEndEvent(_))
        val envUpdateEvent = eventLogJsonSeqFiltered.find(_(ColEvent) == EventEnvUpdate).map(EnvUpdateEvent(_))
        val execAddedEvents = eventLogJsonSeqFiltered.filter(_(ColEvent) == EventExecutorAdded).map(ExecutorAddedEvent(_))
        val execRemovedEvents = eventLogJsonSeqFiltered.filter(_(ColEvent) == EventExecutorRemoved).map(ExecutorRemovedEvent(_))
        val stageSubmittedEvents = eventLogJsonSeqFiltered.filter(_(ColEvent) == EventStageSubmitted).map(StageSubmittedEvent(_))
        val stageCompletedEvents = eventLogJsonSeqFiltered.filter(_(ColEvent) == EventStageCompleted).map(StageCompletedEvent(_))

        val stages: Seq[StageContext] = stageSubmittedEvents.flatMap { stageSubmission =>
            val stageCompletion = stageCompletedEvents.find(_.stageId == stageSubmission.stageId)
            stageCompletion match {
                case Some(stageCompletion) => Some(StageContext(
                    stageSubmission.stageId,
                    stageSubmission.submissionTime,
                    stageCompletion.completionTime,
                    stageSubmission.numberOfTasks,
                ))
                case None => None
            }
        }

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
            executorMap,
            stages = stages
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
    val EventStageSubmitted = "SparkListenerStageSubmitted"
    val EventStageCompleted = "SparkListenerStageCompleted"

    val AllEvents = Seq(
        EventEnvUpdate,
        EventAppStart,
        EventAppEnd,
        EventExecutorAdded,
        EventExecutorRemoved,
        EventStageSubmitted,
        EventStageCompleted
    )

    val ColEvent = "Event"
    val ColTimeStamp = "Timestamp"
    val ColExecutorId = "Executor ID"
}