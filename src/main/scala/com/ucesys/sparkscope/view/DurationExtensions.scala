package com.ucesys.sparkscope.view

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.DurationInt

object DurationExtensions {
    implicit class FiniteDurationExtensions(duration: FiniteDuration) {
        def durationStr: String = {
            duration match {
                case duration if duration < 1.minutes => s"${duration.toSeconds.toString}s"
                case duration if duration < 1.hours => s"${duration.toMinutes % 60}min ${duration.toSeconds % 60}s"
                case _ => s"${duration.toHours}h ${duration.toMinutes % 60}min ${duration.toSeconds % 60}s"
            }
        }
    }

}
