package com.ucesys.sparkscope.common

import com.ucesys.sparkscope.common.MemorySize.BytesInMB

case class MemorySize(sizeInBytes: Long) {
    def multiply(by: Float): MemorySize = MemorySize((this.sizeInBytes*by).toLong)

    def toMB: Long = sizeInBytes/BytesInMB

    def max(other: MemorySize): MemorySize = {
        if (this.sizeInBytes > other.sizeInBytes) {
            this
        } else {
            other
        }
    }

    def >(other: MemorySize): Boolean = {
        if (this.sizeInBytes > other.sizeInBytes) {
            true
        } else {
            false
        }
    }
}

object MemorySize {
    val BytesInMB: Long = 1024*1024
    val BytesInGB: Long = 1024*1024*1024
    def fromMegaBytes(megaBytes: Long): MemorySize = MemorySize(megaBytes*BytesInMB)
    def fromGigaBytes(gigaBytes: Long): MemorySize = MemorySize(gigaBytes*BytesInGB)

    def fromStr(sizeStr: String)(implicit logger: SparkScopeLogger): MemorySize = {
        try {
            sizeStr match {
                case s if s.contains("m") => MemorySize.fromMegaBytes(s.replace("m", "").toFloat.toLong)
                case s if s.contains("g") => MemorySize.fromGigaBytes(s.replace("g", "").toFloat.toLong)
                case _ => MemorySize.fromMegaBytes(sizeStr.toFloat.toLong)
            }
        } catch {
            case ex: Exception => logger.warn(s"Could not parse ${sizeStr} into memory size. " + ex); MemorySize(0)
        }
    }
}
