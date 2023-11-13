package com.ucesys.sparkscope.io

sealed trait InstanceType { def name: String }

case object Driver extends InstanceType { val name = "driver" }

case object Executor extends InstanceType { val name = "executor" }