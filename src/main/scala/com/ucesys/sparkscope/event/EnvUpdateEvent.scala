package com.ucesys.sparkscope.event

case class EnvUpdateEvent(sparkConf: Option[Map[String, String]])

object EnvUpdateEvent {
    val ColSparkProps = "Spark Properties"

    def apply(eventMap: Map[String, Any]): EnvUpdateEvent = {
        EnvUpdateEvent(eventMap.get(ColSparkProps).map(_.asInstanceOf[Map[String, String]]))
    }
}
