package com.ucesys.sparkscope.event

import com.ucesys.sparkscope.SparkScopeArgs
import com.ucesys.sparkscope.SparkScopeConfLoader._
import com.ucesys.sparkscope.common.{SparkScopeContext, SparkScopeLogger}
import com.ucesys.sparkscope.event.EventLogContextLoader._
import com.ucesys.sparkscope.io.file.FileReaderFactory
import com.ucesys.sparkscope.timeline.{ExecutorTimeline, StageTimeline}
import org.apache.spark.SparkConf
import org.apache.spark.SparkEventParser
import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerApplicationStart, SparkListenerEnvironmentUpdate, SparkListenerExecutorAdded, SparkListenerExecutorRemoved, SparkListenerStageCompleted, SparkListenerStageSubmitted}
import org.json4s.DefaultFormats

import org.json4s._
import org.json4s.jackson.JsonMethods

class EventLogContextLoader(implicit logger: SparkScopeLogger) {
    def load(fileReaderFactory: FileReaderFactory, args: SparkScopeArgs): EventLogContext = {
        val fileReader = fileReaderFactory.getFileReader(args.eventLog)
        val eventLogJsonStrSeq: Seq[String] = fileReader.read(args.eventLog).split("\n").toSeq
        logger.info(s"Loaded ${eventLogJsonStrSeq.length} events")

        val eventLogJsonSeqPreFiltered: Seq[String] = eventLogJsonStrSeq.filter(event => AllEvents.exists(event.contains))
        logger.info(s"Prefiltered ${eventLogJsonSeqPreFiltered.length} events")

        implicit val formats = DefaultFormats
        val sparkEvents = eventLogJsonSeqPreFiltered.map(JsonMethods.parse(_)).flatMap(SparkEventParser.parse(_))
        logger.info(s"Parsed ${sparkEvents.length} events")

        val appStartEvent = sparkEvents.collectFirst { case e: SparkListenerApplicationStart => e }
        val appEndEvent = sparkEvents.collectFirst { case e: SparkListenerApplicationEnd => e }
        val envUpdateEvent = sparkEvents.collectFirst { case e: SparkListenerEnvironmentUpdate => e }
        val execAddedEvents = sparkEvents.collect { case e: SparkListenerExecutorAdded => e }
        val execRemovedEvents = sparkEvents.collect { case e: SparkListenerExecutorRemoved => e }
        val stageSubmittedEvents = sparkEvents.collect { case e: SparkListenerStageSubmitted => e }
        val stageCompletedEvents = sparkEvents.collect { case e: SparkListenerStageCompleted => e }

        val stages: Seq[StageTimeline] = stageSubmittedEvents.flatMap { stageSubmission =>
            val stageCompletion = stageCompletedEvents.find(_.stageInfo.stageId == stageSubmission.stageInfo.stageId)
            stageCompletion match {
                case Some(stageCompletion) => Some(StageTimeline(stageSubmission, stageCompletion))
                case None => None
            }
        }

        val executorMap: Map[String, ExecutorTimeline] = execAddedEvents.map { execAddedEvent =>
            (
                execAddedEvent.executorId,
                ExecutorTimeline(
                    executorId=execAddedEvent.executorId,
                    cores=execAddedEvent.executorInfo.totalCores,
                    startTime=execAddedEvent.time,
                    endTime=execRemovedEvents.find(_.executorId == execAddedEvent.executorId).map(_.time)
                )
            )
        }.toMap

        if (appStartEvent.isEmpty || appStartEvent.get.appId.isEmpty) {
            logger.error(s"Error during parsing of Application Start Event(${appStartEvent})")
            throw new IllegalArgumentException(s"Error during parsing of Application Start Event(${appStartEvent})")
        }

        if (envUpdateEvent.isEmpty || envUpdateEvent.get.environmentDetails.isEmpty) {
            logger.error(s"Error during parsing of Environment Update Event(${envUpdateEvent})")
            throw new IllegalArgumentException(s"Error during parsing of Environment Update Event(${envUpdateEvent})")
        }

        if (appEndEvent.isEmpty) {
            logger.info("Could not read application end event from eventLog, app might still be running.")
        }

        val sparkConf = new SparkConf(false)
        envUpdateEvent.get.environmentDetails.get("Spark Properties").foreach(_.foreach { case (key, value) => sparkConf.set(key, value) })

        // Overriding SparkConf with input args if specified
        args.driverMetrics.map(sparkConf.set(SparkScopePropertyDriverMetricsDir, _))
        args.executorMetrics.map(sparkConf.set(SparkScopePropertyExecutorMetricsDir, _))
        args.htmlPath.map(sparkConf.set(SparkScopePropertyHtmlPath, _))

        // App Context
        val appContext = SparkScopeContext(
            appId=appStartEvent.get.appId.get,
            appStartTime=appStartEvent.get.time,
            appEndTime=appEndEvent.map(_.time),
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