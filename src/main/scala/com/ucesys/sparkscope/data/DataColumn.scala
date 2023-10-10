package com.ucesys.sparkscope.data

import java.time.LocalDateTime.ofEpochSecond
import java.time.ZoneOffset.UTC

case class DataColumn(name: String, values: Seq[String]) {
    def size: Int = values.length
    def toFloat: Seq[Float] = values.map(_.toFloat)
    def toLong: Seq[Long] = values.map(_.toFloat.toLong)
    def toDouble: Seq[Double] = values.map(_.toDouble)
    def div(divBy: Long): DataColumn = DataColumn(this.name, values.map(elem => (elem.toDouble / divBy).toString))
    def div(other: DataColumn): DataColumn = {
        val dividedValues =(this.values zip other.values).map{
            case (x, y) if (y.toDouble == 0) => "0"
            case (x, y) => (x.toDouble / y.toDouble).toString
        }
        DataColumn(s"${this.name}DivBy${other.name}", dividedValues)
    }
    def mul(mulBy: Long): DataColumn = DataColumn(this.name, values.map(elem => (elem.toDouble * mulBy).toString))

    def gt(greaterThan: Double): DataColumn = DataColumn(this.name, values.map(elem => Seq(greaterThan,elem.toDouble).max.toString))

    def lt(lowerThan: Double): DataColumn = DataColumn(this.name, values.map(elem => Seq(lowerThan, elem.toDouble).min.toString))


    def mul(other: DataColumn): DataColumn = {
        val multipliedValues = (this.values zip other.values).map { case (x, y) => (x.toDouble * y.toDouble).toString }
        DataColumn(s"${this.name}MulBy${other.name}", multipliedValues)
    }
    def sub(other: DataColumn): DataColumn = {
        val subtractedValues = (this.values zip other.values).map { case (x, y) => (x.toDouble - y.toDouble).toString }
        DataColumn(s"${this.name}Sub${other.name}", subtractedValues)
    }

    def min(other: DataColumn): DataColumn = {
        val subtractedValues = (this.values zip other.values).map { case (x, y) => Seq(x.toDouble, y.toDouble).min.toString}
        DataColumn(s"${this.name}Min${other.name}", subtractedValues)
    }

    def sum: Double = this.toDouble.sum
    def max: Double = this.toDouble.max
    def avg: Double = this.toDouble.sum / values.length
    def tsToDt: DataColumn = DataColumn("dt", values.map(ts => ofEpochSecond(ts.toLong, 0, UTC).toString))
    def lag: DataColumn = DataColumn(s"${name}Lag", Seq(values.head) ++ values.init)

    def rename(newName: String): DataColumn = this.copy(name=newName)

    override def toString: String = (Seq(name) ++ values.map(x => f"${x.toDouble}%.4f")).mkString("\n")
}