package com.ucesys.sparkscope.view.warning

case class CPUUtilWarning private(cpuUtil: Double, coreHoursWasted: Double) extends Warning {
    override def toString: String = {
        f"CPU utlization is LOW(${cpuUtil * 100}%1.2f%%)"
    }
}

object CPUUtilWarning {
    def apply(cpuUtil: Double, coreHoursWasted: Double, lowUtilizationThreshold: Float): Option[CPUUtilWarning] = {
        cpuUtil match {
            case util if (util > lowUtilizationThreshold) => None
            case _ => Some(new CPUUtilWarning(cpuUtil, coreHoursWasted))
        }
    }
}