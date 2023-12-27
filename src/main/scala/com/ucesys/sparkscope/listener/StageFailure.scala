package com.ucesys.sparkscope.listener

case class StageFailure(stageId: Int, attemptId: Int, jobId: Long, numTasks: Int) {
    override def toString: String =
        s"Failed Stage: id=${stageId} attempt=${attemptId} in job ${jobId} failed. Stage tasks: ${numTasks}"
}
