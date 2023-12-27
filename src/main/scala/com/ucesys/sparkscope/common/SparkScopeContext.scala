package com.ucesys.sparkscope.common

import com.ucesys.sparkscope.timeline.{ExecutorTimeline, StageTimeline}

case class SparkScopeContext(appId: String,
                             appStartTime: Long,
                             appEndTime: Option[Long],
                             executorMap: Map[String, ExecutorTimeline],
                             stages: Seq[StageTimeline]) {
  def executorCores: Int = executorMap.values.headOption.map(_.cores).getOrElse(0)
}
