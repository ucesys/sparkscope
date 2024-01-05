package com.ucesys.sparkscope.io.report

case class AppInfo(appId: String,
                   sparkAppName: Option[String],
                   sparkScopeAppName: Option[String],
                   startTs: Long,
                   endTs: Option[Long],
                   duration: Option[Long],
                   driverHost: Option[String])