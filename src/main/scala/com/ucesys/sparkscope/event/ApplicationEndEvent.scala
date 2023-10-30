package com.ucesys.sparkscope.event

import com.ucesys.sparkscope.event.EventLogContextLoader._

case class ApplicationEndEvent(ts: Option[Long])

object ApplicationEndEvent {
    def apply(eventMap: Map[String, Any]): ApplicationEndEvent = {
        ApplicationEndEvent(eventMap.get(ColTimeStamp).map(_.asInstanceOf[Double].doubleValue.toLong))
    }
}
