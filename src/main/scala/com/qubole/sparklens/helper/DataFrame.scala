package com.qubole.sparklens.helper

case class DataColumn(name: String, values: Seq[String]) {
    def size: Int = values.length
    def toFloat: Seq[Float] = values.map(_.toFloat)
    def toLong: Seq[Long] = values.map(_.toFloat.toLong)
    def toDouble: Seq[Double] = values.map(_.toDouble)
    def sum: Double = this.toDouble.sum
//    def max: Float = this.toFloat.max
}

case class GroupByResult(groupCol: String, aggCol: String, result: Map[String, Seq[String]]) {
    def sum: DataFrame = {
        val aggregated = this.result.map { case (groupCol, aggCol) => (groupCol, aggCol.map(_.toDouble).sum) }
        toDataFrame(s"groupBy${groupCol}.avg(${aggCol})", aggregated)
    }

    def avg: DataFrame = {
        val aggregated = this.result.map { case (groupCol, seq) => (groupCol, seq.map(_.toDouble).sum / seq.length) }
        toDataFrame(s"groupBy${groupCol}.avg(${aggCol})", aggregated)
    }

    def toDataFrame(name: String, aggResult: Map[String, Double]): DataFrame = {
        DataFrame(name, columns = Seq(DataColumn(groupCol, aggResult.keys.toSeq), DataColumn(aggCol, aggResult.values.toSeq.map(x => f"${x}%.4f"))))
    }
}

case class DataFrame(name: String, columns: Seq[DataColumn]) {
    def numCols: Int = columns.length
    def numRows: Int = columns.headOption.map(x => x.values.length).getOrElse(0)

    def columnsNames: Seq[String] = columns.map(_.name)

    // TODO VALIDATE TRANSPOSE
    def toRows: Seq[Seq[String]] = columns.map(_.values).transpose

    def toRowsWithHeader: Seq[Seq[String]] = Seq(columnsNames) ++ this.toRows

    def toCsv(delimeter: String): String = this.toRowsWithHeader.map(_.mkString(delimeter)).mkString("\n")
    override def toString(): String = this.toCsv(",")

    def addColumn(newColumn: DataColumn): DataFrame = {
        assert(this.numRows == newColumn.size, "New Columns needs to have the same amount of rows!")
        new DataFrame(this.name, this.columns ++ Seq(newColumn))
    }

    def addConstColumn(name: String, value: String): DataFrame = {
        addColumn(DataColumn(name, Seq.fill(this.numRows)(value)))
    }

    def union(other: DataFrame): DataFrame = {
        assert(this.columns.map(_.name) == other.columns.map(_.name), "Unioned tables must have the same columns!")
        val colsSeq = (this.toRows ++ other.toRows).transpose
        val columns = (columnsNames zip colsSeq).map{case (name, values) => DataColumn(name, values)}
        DataFrame(this.name, columns)
    }

    def mergeOn(onCol: String, other: DataFrame): DataFrame = {
        assert(this.columns.find(_.name == onCol) == other.columns.find(_.name == onCol) , "MergedOn tables must have the same values for the onColumn!")
        DataFrame(this.name, this.columns ++ other.columns.filterNot(_.name == onCol))
    }

    def groupBy(groupCol: String, aggCol: String): GroupByResult = {
        val groupColumn = this.columns.find(_.name == groupCol).get.values
        val aggColumn = this.columns.find(_.name == aggCol).get.values
        val grouped = groupColumn.zip(aggColumn).groupBy(x => x._1)
          .map { case (groupCol, seq) => (groupCol, seq.map { case (_, aggCol) => aggCol }) }
        GroupByResult(groupCol, aggCol, grouped)
    }
}

object DataFrame {
//    def apply(name: String, columns: Seq[DataColumn]): DataFrame = {
//        new DataFrame(name, columns)
//    }
    def fromCsv(name: String, csvStr: String, delimeter: String): DataFrame = {
        val rows = csvStr.split("\n")
        val header = rows.head.split(delimeter)
        val columnsSeq = rows.tail.map(_.split(delimeter)).transpose
        val columns = (header zip columnsSeq).map{case (name, values) => DataColumn(name, values)}
        DataFrame(name, columns)
    }
}
