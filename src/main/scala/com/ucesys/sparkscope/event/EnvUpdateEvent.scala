package com.ucesys.sparkscope.event

import com.ucesys.sparkscope.event.EventLogContextLoader._

case class EnvUpdateEvent(sparkConf: Option[Map[String, String]])

object EnvUpdateEvent {
    def apply(eventMap: Map[String, Any]): EnvUpdateEvent = {
        EnvUpdateEvent(eventMap.get(ColSparkProps).map(_.asInstanceOf[Map[String, String]]))
    }
}
