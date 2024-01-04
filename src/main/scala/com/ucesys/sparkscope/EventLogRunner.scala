package com.ucesys.sparkscope

import com.ucesys.sparkscope.EventLogRunner._
import com.ucesys.sparkscope.SparkScopeConfLoader._
import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.io.reader.FileReaderFactory
import org.apache.spark.scheduler._
import org.apache.spark.{SparkConf, SparkEventParser}
import org.json4s._
import org.json4s.jackson.JsonMethods

class EventLogRunner(listener: SparkScopeJobListener)(implicit logger: SparkScopeLogger) {
    def run(fileReaderFactory: FileReaderFactory, args: SparkScopeArgs): Unit = {
        val fileReader = fileReaderFactory.getFileReader(args.eventLog)
        val eventLogJsonStrSeq: Seq[String] = fileReader.read(args.eventLog).split("\n").toSeq
        logger.info(s"Loaded ${eventLogJsonStrSeq.length} events", this.getClass)

        val eventLogJsonSeqPreFiltered: Seq[String] = eventLogJsonStrSeq.filter(event => AllEvents.exists(event.contains))
        logger.info(s"Prefiltered ${eventLogJsonSeqPreFiltered.length} events", this.getClass)

        val sparkEvents = eventLogJsonSeqPreFiltered.flatMap(JsonMethods.parseOpt(_)).flatMap(SparkEventParser.parse(_))
        logger.info(s"Parsed ${sparkEvents.length} events", this.getClass)

        val envUpdateEventWithOverrides = {
           val sparkConf = new SparkConf(false)

            sparkEvents
              .collectFirst { case e: SparkListenerEnvironmentUpdate => e }
              .getOrElse(throw new IllegalArgumentException(s"Error during parsing of Environment Update Event)") )
              .environmentDetails
              .get("Spark Properties")
              .foreach(_.foreach { case (key, value) => sparkConf.set(key, value) })

            // Overriding SparkConf with input args if specified
            args.driverMetrics.map(sparkConf.set(SparkScopePropertyDriverMetricsDir, _))
            args.executorMetrics.map(sparkConf.set(SparkScopePropertyExecutorMetricsDir, _))
            args.htmlPath.map(sparkConf.set(SparkScopePropertyHtmlPath, _))
            args.logPath.map(sparkConf.set(SparkScopePropertyLogPath, _))
            args.logLevel.map(sparkConf.set(SparkScopePropertyLogLevel, _))

            val environmentDetails: Map[String, Seq[(String, String)]] = Map("Spark Properties" -> sparkConf.getAll.toSeq)
            SparkListenerEnvironmentUpdate(environmentDetails)
        }

        listener.onEnvironmentUpdate(envUpdateEventWithOverrides)

        sparkEvents.foreach {
            case e: SparkListenerApplicationStart => listener.onApplicationStart(e)
            case e: SparkListenerExecutorAdded => listener.onExecutorAdded(e)
            case e: SparkListenerExecutorRemoved => listener.onExecutorRemoved(e)
            case e: SparkListenerJobStart => listener.onJobStart(e)
            case e: SparkListenerJobEnd => listener.onJobEnd(e)
            case e: SparkListenerStageSubmitted => listener.onStageSubmitted(e)
            case e: SparkListenerStageCompleted => listener.onStageCompleted(e)
            case e: SparkListenerTaskEnd => listener.onTaskEnd(e)
            case _ =>
        }

        val appEndEvent: Option[SparkListenerApplicationEnd] = sparkEvents.collectFirst { case event: SparkListenerApplicationEnd => event }

        listener.runSparkScopeAnalysis(appEndEvent.map(_.time))
    }
}

object EventLogRunner {
    val EventAppStart = "SparkListenerApplicationStart"
    val EventAppEnd = "SparkListenerApplicationEnd"
    val EventEnvUpdate = "SparkListenerEnvironmentUpdate"
    val EventExecutorAdded = "SparkListenerExecutorAdded"
    val EventExecutorRemoved = "SparkListenerExecutorRemoved"
    val EventStageSubmitted = "SparkListenerStageSubmitted"
    val EventStageCompleted = "SparkListenerStageCompleted"
    val EventJobStart = "SparkListenerJobStart"
    val EventJobEnd = "SparkListenerJobEnd"
    val EventTaskEnd = "SparkListenerTaskEnd"

    val AllEvents = Seq(
        EventEnvUpdate,
        EventAppStart,
        EventAppEnd,
        EventExecutorAdded,
        EventExecutorRemoved,
        EventStageSubmitted,
        EventStageCompleted,
        EventJobStart,
        EventJobEnd,
        EventTaskEnd
    )
}