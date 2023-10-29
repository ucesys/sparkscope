package com.ucesys.sparkscope.event

import com.ucesys.sparkscope.event.EventLogContextLoader._
//import collection.JavaConverters._

case class ApplicationStartEvent(appId: String, ts: Long)

object ApplicationStartEvent {
    def apply(eventMap: Map[String, Any]): ApplicationStartEvent = {
        ApplicationStartEvent(eventMap(ColAppId).asInstanceOf[String], eventMap(ColTimeStamp).asInstanceOf[Double].doubleValue.toLong)
    }
}
