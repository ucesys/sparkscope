package com.ucesys.sparkscope

import java.time.LocalDateTime.ofEpochSecond
import java.time.ZoneOffset.UTC

case class DataColumn(name: String, values: Seq[String]) {
    def size: Int = values.length
    def toFloat: Seq[Float] = values.map(_.toFloat)
    def toLong: Seq[Long] = values.map(_.toFloat.toLong)
    def toDouble: Seq[Double] = values.map(_.toDouble)
//    def div(divBy: Long): Seq[Double] = values.map(_.toDouble / divBy)
    def div(divBy: Long): DataColumn = DataColumn(this.name, values.map(elem => (elem.toDouble / divBy).toString))
    def div(other: DataColumn): DataColumn = {
        val dividedValues =(this.values zip other.values).map{case (x, y) => (x.toDouble / y.toDouble).toString}
        DataColumn(s"${this.name}DivBy${other.name}", dividedValues)
    }
    def sum: Double = this.toDouble.sum
    def max: Double = this.toDouble.max
    def avg: Double = this.toDouble.sum / values.length
    def tsToDt: DataColumn = DataColumn("dt", values.map(ts => ofEpochSecond(ts.toLong, 0, UTC).toString))
}

case class GroupByResult(groupCol: String, aggCol: String, result: Map[String, Seq[String]]) {
    def sum: DataFrame = {
        val aggregated = this.result.map { case (groupCol, aggCol) => (groupCol, aggCol.map(_.toDouble).sum) }
        toDataFrame(s"groupBy${groupCol}.avg(${aggCol})", aggregated)
    }

    def max: DataFrame = {
        val aggregated = this.result.map { case (groupCol, aggCol) => (groupCol, aggCol.map(_.toDouble).max) }
        toDataFrame(s"groupBy${groupCol}.avg(${aggCol})", aggregated)
    }

    def min: DataFrame = {
        val aggregated = this.result.map { case (groupCol, aggCol) => (groupCol, aggCol.map(_.toDouble).min) }
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

    def select(column: String): DataColumn = this.columns.find(_.name == column).get
    def select(subColumns: Seq[String]): DataFrame = DataFrame(this.name, this.columns.filter(col => subColumns.contains(col.name)))

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
        val rows = (this.toRows ++ other.toRows)
        DataFrame.fromRows(this.name, this.columnsNames, rows)
    }

    def mergeOn(onCol: String, other: DataFrame): DataFrame = {
        val left = this.columns.find(_.name == onCol)
        val right = other.columns.find(_.name == onCol)
        assert(left ==  right, s"MergedOn failed for tables(${this.name}, ${other.name}) must have the same values for the onColumn! \nLeft: ${left}\nRight: ${right}")
        DataFrame(this.name, this.columns ++ other.columns.filterNot(_.name == onCol))
    }

    def groupBy(groupCol: String, aggCol: String): GroupByResult = {
        val groupColumn = this.columns.find(_.name == groupCol).get.values
        val aggColumn = this.columns.find(_.name == aggCol).get.values
        val grouped = groupColumn.zip(aggColumn).groupBy(x => x._1)
          .map { case (groupCol, seq) => (groupCol, seq.map { case (_, aggCol) => aggCol }) }
        GroupByResult(groupCol, aggCol, grouped)
    }

    def sortBy(col: String): DataFrame = {
        val sortCol: DataColumn = this.select(col)
        val sortedRows = (sortCol.values zip this.toRows).sortBy(_._1).map{case (_, row) => row}
        DataFrame.fromRows(this.name, this.columnsNames, sortedRows)
    }

    def distinct(col: String): DataFrame = {
        val dinstinctCol = this.select(col)
        val rows = (dinstinctCol.values zip this.toRows).groupBy(_._1).map{case (key, value) => value.head._2}.toSeq
        DataFrame.fromRows(this.name, this.columnsNames, rows)
    }
}

object DataFrame {
    def apply(column: DataColumn): DataFrame = {
        DataFrame(column.name, Seq(column))
    }
    def fromCsv(name: String, csvStr: String, delimeter: String): DataFrame = {
        val rows = csvStr.split("\n")
        val header = rows.head.split(delimeter)
        val columnsSeq = rows.tail.map(_.split(delimeter)).transpose
        val columns = (header zip columnsSeq).map{case (name, values) => DataColumn(name, values)}
        DataFrame(name, columns)
    }

    def fromRows(name: String, columnNames: Seq[String], rows: Seq[Seq[String]]): DataFrame = {
        val columns = (columnNames zip rows.transpose).map { case (name, col) => DataColumn(name, col) }
        DataFrame(name, columns)
    }
}
