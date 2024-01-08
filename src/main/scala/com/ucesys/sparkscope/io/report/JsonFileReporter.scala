package com.ucesys.sparkscope.io.report

import com.ucesys.sparkscope.common._
import com.ucesys.sparkscope.io.writer.TextFileWriter
import com.ucesys.sparkscope.metrics.SparkScopeResult
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

import java.nio.file.Paths

class JsonFileReporter(appContext: AppContext, sparkScopeConf: SparkScopeConf, jsonFileWriter: TextFileWriter)
                      (implicit logger: SparkScopeLogger) extends Reporter {

    implicit val formats = DefaultFormats

    override def report(result: SparkScopeResult): Unit = {
        val report = SparkScopeReport(
            appContext = appContext,
            conf = sparkScopeConf,
            stats = result.stats,
            charts = result.charts,
            warnings = result.warnings.map(_.toString)
        )

        try {
            val jsonReport = Serialization.writePretty(report)

            sparkScopeConf.jsonReportPath.foreach{ path =>
                val outputPath = Paths.get(path, s"${appContext.appId}.json")
                jsonFileWriter.write(outputPath.toString, jsonReport)
                logger.info(s"Wrote JSON report file to ${outputPath}", this.getClass)
            }
        } catch {
            case ex: Exception => logger.error(s"Unexpected exception while trying to send diagnostics: ${ex}", this.getClass, stdout = false)
        }
    }
}
