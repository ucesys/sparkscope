package com.ucesys.sparkscope.event

import com.ucesys.sparkscope.event.EventLogContextLoader._
//import collection.JavaConverters._

case class ApplicationEndEvent(ts: Long)

object ApplicationEndEvent {
    def apply(eventMap: Map[String, Any]): ApplicationEndEvent = {
        ApplicationEndEvent(eventMap(ColTimeStamp).asInstanceOf[Double].doubleValue.toLong)
    }
}
