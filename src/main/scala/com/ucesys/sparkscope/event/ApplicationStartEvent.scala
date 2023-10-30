package com.ucesys.sparkscope.event

import com.ucesys.sparkscope.event.EventLogContextLoader._

case class ApplicationStartEvent(appId: Option[String], ts: Option[Long])

object ApplicationStartEvent {
    def apply(eventMap: Map[String, Any]): ApplicationStartEvent = {
        ApplicationStartEvent(
            eventMap.get(ColAppId).map(_.asInstanceOf[String]),
            eventMap.get(ColTimeStamp).map(_.asInstanceOf[Double].doubleValue.toLong)
        )
    }
}
