package com.ucesys.sparkscope.data

case class DataFrame(name: String, columns: Seq[DataColumn]) {
    def numCols: Int = columns.length
    def numRows: Int = columns.headOption.map(x => x.values.length).getOrElse(0)

    def columnsNames: Seq[String] = columns.map(_.name)

    def toRows: Seq[Seq[String]] = columns.map(_.values).transpose

    def toRowsWithHeader: Seq[Seq[String]] = Seq(this.columnsNames) ++ this.toRows

    def select(column: String): DataColumn = this.columns.find(_.name == column).get
    def select(subColumns: Seq[String]): DataFrame = DataFrame(this.name, this.columns.filter(col => subColumns.contains(col.name)))

    def toCsv(delimeter: String): String = this.toRowsWithHeader.map(_.mkString(delimeter)).mkString("\n")
    override def toString(): String = {
        val paddedCols = this.columns.map { col =>
            val colWithName = Seq(col.name) ++ col.values
            val maxStrLength = colWithName.map(_.length).max
            val borderStr = Seq.fill(maxStrLength)("-").mkString
            val colWithNameWithBorder = Seq(borderStr, col.name, borderStr) ++ col.values ++ Seq(borderStr)
            val paddedValues = colWithNameWithBorder.map{ str => str + Seq.fill(maxStrLength-str.length)(" ").mkString}
            DataColumn(col.name, paddedValues)
        }
        DataFrame(this.name, paddedCols).toRows.map(_.mkString("|", "|", "|")).mkString("\n")
    }


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

    def dropNLastRows(n: Int): DataFrame = {
        DataFrame.fromRows(this.name, this.columnsNames, this.toRows.dropRight(n))
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

    def fromCsv(name: String, csvStr: String, delimeter: String, columnNames: Seq[String]): DataFrame = {
        val rows = csvStr.split("\n")
        val columnsSeq = rows.tail.map(_.split(delimeter)).transpose
        val columns = (columnNames zip columnsSeq).map { case (name, values) => DataColumn(name, values) }
        DataFrame(name, columns)
    }

    def fromRows(name: String, columnNames: Seq[String], rows: Seq[Seq[String]]): DataFrame = {
        val columns = (columnNames zip rows.transpose).map { case (name, col) => DataColumn(name, col) }
        DataFrame(name, columns)
    }
}
