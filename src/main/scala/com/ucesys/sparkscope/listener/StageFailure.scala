package com.ucesys.sparkscope.listener

case class StageFailure(stageId: Int, attemptId: Int, jobId: Option[Long], numTasks: Int) {
    override def toString: String =
        s"Failed Stage: id=${stageId} attempt=${attemptId} in job ${jobId.map(_.toString).getOrElse("none")} failed. Stage tasks: ${numTasks}"
}
