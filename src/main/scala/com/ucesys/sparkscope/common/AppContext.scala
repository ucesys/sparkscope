package com.ucesys.sparkscope.common

import com.ucesys.sparkscope.timeline.{ExecutorTimeline, StageTimeline}

case class AppContext(appId: String,
                      appStartTime: Long,
                      appEndTime: Option[Long],
                      executorMap: Map[String, ExecutorTimeline],
                      stages: Seq[StageTimeline]) {
  def executorCores: Int = executorMap.values.headOption.map(_.cores).getOrElse(0)
}
