package com.ucesys.sparkscope.event

import com.ucesys.sparkscope.event.EventLogContextLoader._

case class ExecutorRemovedEvent(executorId: String, ts: Long)

object ExecutorRemovedEvent {
    def apply(eventMap: Map[String, Any]): ExecutorRemovedEvent = {
        ExecutorRemovedEvent(
            eventMap(ColExecutorId).asInstanceOf[String],
            eventMap(ColTimeStamp).asInstanceOf[Double].doubleValue.toLong
        )
    }
}
