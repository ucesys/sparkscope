package com.ucesys.sparkscope.event

import com.ucesys.sparkscope.common.SparkScopeLogger
import com.ucesys.sparkscope.event.EventLogContextLoader._

case class ExecutorRemovedEvent(executorId: String, ts: Long)

object ExecutorRemovedEvent {
    def apply(eventMap: Map[String, Any])(implicit logger: SparkScopeLogger): Option[ExecutorRemovedEvent] = {
        try {
            Some(ExecutorRemovedEvent(
                eventMap(ColExecutorId).asInstanceOf[String],
                eventMap(ColTimeStamp).asInstanceOf[Double].doubleValue.toLong
            ))
        } catch {
            case ex: Exception => logger.warn("Couldn't parse ExecutorRemovedEvent. " + ex); None
        }
    }
}
