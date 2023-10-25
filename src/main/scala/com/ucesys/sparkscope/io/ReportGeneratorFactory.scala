package com.ucesys.sparkscope.io

import com.ucesys.sparkscope.SparkScopeConf
import com.ucesys.sparkscope.utils.SparkScopeLogger

class ReportGeneratorFactory {
    def get(sparkScopeConfig: SparkScopeConf)(implicit logger: SparkScopeLogger): ReportGenerator = {
        new HtmlReportGenerator(sparkScopeConfig)
    }
}
