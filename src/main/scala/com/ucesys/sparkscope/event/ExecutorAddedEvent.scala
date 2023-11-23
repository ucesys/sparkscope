package com.ucesys.sparkscope.event

import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.event.EventLogContextLoader._

case class ExecutorAddedEvent(executorId: String, ts: Long, cores: Long)

object ExecutorAddedEvent {
    val ColExecutorInfo = "Executor Info"
    val ColExecutorCores = "Total Cores"
    def apply(eventMap: Map[String, Any])(implicit logger: SparkScopeLogger): Option[ExecutorAddedEvent] = {
        try {
            Some(ExecutorAddedEvent(
                eventMap(ColExecutorId).asInstanceOf[String],
                eventMap(ColTimeStamp).asInstanceOf[Double].doubleValue.toLong,
                eventMap(ColExecutorInfo).asInstanceOf[Map[String, Any]](ColExecutorCores).asInstanceOf[Double].toLong
            ))
        } catch {
            case ex: Exception => logger.warn("Couldn't parse ExecutorAddedEvent. " + ex);None
        }
    }
}
