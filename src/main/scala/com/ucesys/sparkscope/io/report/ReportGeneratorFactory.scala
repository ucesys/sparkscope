package com.ucesys.sparkscope.io.report

import com.ucesys.sparkscope.common.{SparkScopeConf, SparkScopeLogger}
import com.ucesys.sparkscope.io.file.FileWriterFactory

class ReportGeneratorFactory {
    def get(sparkScopeConfig: SparkScopeConf)(implicit logger: SparkScopeLogger): ReportGenerator = {
        new HtmlReportGenerator(
            sparkScopeConfig,
            (new FileWriterFactory(sparkScopeConfig.region)).get(sparkScopeConfig.htmlReportPath)
        )
    }
}
