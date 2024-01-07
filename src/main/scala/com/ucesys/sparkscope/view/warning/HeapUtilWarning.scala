package com.ucesys.sparkscope.view.warning

case class HeapUtilWarning private(heapUtil: Double, heapGbHoursWasted: Double) extends Warning {
    override def toString: String = {
        f"Heap utlization is LOW(${heapUtil * 100}%1.2f%%)"
    }
}

object HeapUtilWarning {
    def apply(heapUtil: Double, heapGbHoursWasted: Double, lowUtilizationThreshold: Float): Option[HeapUtilWarning] = {
        heapUtil match {
            case util if (util > lowUtilizationThreshold) => None
            case _ => Some(new HeapUtilWarning(heapUtil, heapGbHoursWasted))
        }
    }
}