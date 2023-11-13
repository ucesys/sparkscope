package com.ucesys.sparkscope.io.report

import com.ucesys.sparkscope.common.{SparkScopeConf, SparkScopeLogger}

class ReportGeneratorFactory {
    def get(sparkScopeConfig: SparkScopeConf)(implicit logger: SparkScopeLogger): ReportGenerator = {
        new HtmlReportGenerator(sparkScopeConfig)
    }
}
