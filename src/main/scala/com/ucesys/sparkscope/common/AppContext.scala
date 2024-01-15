package com.ucesys.sparkscope.common

import com.ucesys.sparkscope.timeline.{ExecutorTimeline, StageTimeline}
import com.ucesys.sparkscope.view.DurationExtensions.FiniteDurationExtensions

import scala.concurrent.duration.DurationLong
import scala.concurrent.duration.FiniteDuration

case class AppContext(appId: String,
                      appName: String,
                      appStartTime: Long,
                      appEndTime: Option[Long],
                      driverHost: Option[String],
                      executorMap: Map[String, ExecutorTimeline],
                      stages: Seq[StageTimeline]) {
  def executorCores: Int = executorMap.values.headOption.map(_.cores).getOrElse(0)

  def duration: Option[FiniteDuration] = {
    appEndTime.map(endTime => (endTime - appStartTime).milliseconds)
  }
}
