package org.apache.spark

import com.ucesys.sparkscope.common.SparkScopeLogger
import org.apache.spark.scheduler.SparkListenerEvent
import org.apache.spark.util.JsonProtocol
import org.json4s.JsonAST.JValue


object SparkEventParser {
    def parse(json: JValue)(implicit logger: SparkScopeLogger): Option[SparkListenerEvent] = {
        try {
            Some(JsonProtocol.sparkEventFromJson(json))
        } catch {
            case ex: Exception => logger.warn("Couldn't parse Spark Event. " + ex); None
        }
    }
}
