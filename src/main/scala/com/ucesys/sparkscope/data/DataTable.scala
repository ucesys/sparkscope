package com.ucesys.sparkscope.data

case class DataTable(name: String, columns: Seq[DataColumn]) {
    def numCols: Int = columns.length

    def numRows: Int = columns.headOption.map(x => x.values.length).getOrElse(0)

    def columnsNames: Seq[String] = columns.map(_.name)

    def toRows: Seq[Seq[String]] = columns.map(_.values).transpose

    def toRowsWithHeader: Seq[Seq[String]] = Seq(this.columnsNames) ++ this.toRows

    def select(column: String): DataColumn = this.columns.find(_.name == column).get

    def select(subColumns: Seq[String]): DataTable = DataTable(this.name, this.columns.filter(col => subColumns.contains(col.name)))

    def toCsv(delimeter: String): String = this.toRowsWithHeader.map(_.mkString(delimeter)).mkString("\n")

    override def toString(): String = {
        val paddedCols = this.columns.map { col =>
            val colWithName = Seq(col.name) ++ col.values
            val maxStrLength = colWithName.map(_.length).max
            val borderStr = Seq.fill(maxStrLength)("-").mkString
            val colWithNameWithBorder = Seq(borderStr, col.name, borderStr) ++ col.values ++ Seq(borderStr)
            val paddedValues = colWithNameWithBorder.map { str => str + Seq.fill(maxStrLength - str.length)(" ").mkString }
            DataColumn(col.name, paddedValues)
        }
        DataTable(this.name, paddedCols).toRows.map(_.mkString("|", "|", "|")).mkString("\n")
    }


    def addColumn(newColumn: DataColumn): DataTable = {
        assert(this.numRows == newColumn.size, "New Columns needs to have the same amount of rows!")
        new DataTable(this.name, this.columns ++ Seq(newColumn))
    }

    def addConstColumn(name: String, value: String): DataTable = {
        addColumn(DataColumn(name, Seq.fill(this.numRows)(value)))
    }

    def union(other: DataTable): DataTable = {
        assert(this.columns.map(_.name) == other.columns.map(_.name), "Unioned tables must have the same columns!")
        val rows = (this.toRows ++ other.toRows)
        DataTable.fromRows(this.name, this.columnsNames, rows)
    }

    def mergeOn(onCol: String, other: DataTable): DataTable = {
        val left = this.columns.find(_.name == onCol)
        val right = other.columns.find(_.name == onCol)
        assert(left == right, s"MergedOn failed for tables(${this.name}, ${other.name}) must have the same values for the onColumn! \nLeft: ${left}\nRight: ${right}")
        DataTable(this.name, this.columns ++ other.columns.filterNot(_.name == onCol))
    }

    def groupBy(groupCol: String, aggCol: String): GroupByResult = {
        val groupColumn = this.columns.find(_.name == groupCol).get.values
        val aggColumn = this.columns.find(_.name == aggCol).get.values
        val grouped = groupColumn.zip(aggColumn).groupBy(x => x._1)
          .map { case (groupCol, seq) => (groupCol, seq.map { case (_, aggCol) => aggCol }) }
        GroupByResult(groupCol, aggCol, grouped)
    }

    def sortBy(col: String): DataTable = {
        val sortCol: DataColumn = this.select(col)
        val sortedRows = (sortCol.values zip this.toRows).sortBy(_._1).map { case (_, row) => row }
        DataTable.fromRows(this.name, this.columnsNames, sortedRows)
    }

    def distinct(col: String): DataTable = {
        val dinstinctCol = this.select(col)
        val rows = (dinstinctCol.values zip this.toRows).groupBy(_._1).map { case (key, value) => value.head._2 }.toSeq
        DataTable.fromRows(this.name, this.columnsNames, rows)
    }

    def dropNLastRows(n: Int): DataTable = {
        DataTable.fromRows(this.name, this.columnsNames, this.toRows.dropRight(n))
    }
}

object DataTable {
    def apply(column: DataColumn): DataTable = {
        DataTable(column.name, Seq(column))
    }

    def fromCsv(name: String, csvStr: String, delimeter: String): DataTable = {
        val rows = csvStr.replace("\r\n", "\n").split("\n")
        val header = rows.head.split(delimeter)
        val columnsSeq = rows.tail.map(_.split(delimeter)).transpose
        val columns = (header zip columnsSeq).map { case (name, values) => DataColumn(name, values) }
        DataTable(name, columns)
    }

    def fromCsv(name: String, csvStr: String, delimeter: String, columnNames: Seq[String]): DataTable = {
        val rows = csvStr.replace("\r\n", "\n").split("\n")
        val columnsSeq = rows.tail.map(_.split(delimeter)).transpose
        val columns = (columnNames zip columnsSeq).map { case (name, values) => DataColumn(name, values) }
        DataTable(name, columns)
    }

    def fromRows(name: String, columnNames: Seq[String], rows: Seq[Seq[String]]): DataTable = {
        val columns = (columnNames zip rows.transpose).map { case (name, col) => DataColumn(name, col) }
        DataTable(name, columns)
    }
}
