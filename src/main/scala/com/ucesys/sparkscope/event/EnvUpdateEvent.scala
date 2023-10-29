package com.ucesys.sparkscope.event

import com.ucesys.sparkscope.event.EventLogContextLoader._
import org.apache.spark.SparkConf

case class EnvUpdateEvent(sparkConf: SparkConf)

object EnvUpdateEvent {
    def apply(eventMap: Map[String, Any]): EnvUpdateEvent = {
        val sparkPropsMap = eventMap(ColSparkProps).asInstanceOf[Map[String, Any]]
        val sparkConf = new SparkConf(false)
        sparkPropsMap.foreach { case (key, value) => sparkConf.set(key, value.toString) }
        EnvUpdateEvent(sparkConf)
    }
}
