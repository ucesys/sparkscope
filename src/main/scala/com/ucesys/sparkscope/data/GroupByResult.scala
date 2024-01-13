package com.ucesys.sparkscope.data

case class GroupByResult(groupCol: String, aggColName: String, result: Map[String, Seq[String]]) {
  def sum: DataFrame = {
    val aggregated = this.result.map { case (groupCol, aggCol) => (groupCol, aggCol.map(_.toDouble).sum) }
    toDataFrame(s"groupBy${groupCol}.avg(${aggColName})", aggregated)
  }

  def max: DataFrame = {
    val aggregated = this.result.map { case (groupCol, aggCol) => (groupCol, aggCol.map(_.toDouble).max) }
    toDataFrame(s"groupBy${groupCol}.avg(${aggColName})", aggregated)
  }

  def min: DataFrame = {
    val aggregated = this.result.map { case (groupCol, aggCol) => (groupCol, aggCol.map(_.toDouble).min) }
    toDataFrame(s"groupBy${groupCol}.avg(${aggColName})", aggregated)
  }

  def avg: DataFrame = {
    val aggregated = this.result.map { case (groupCol, seq) => (groupCol, seq.map(_.toDouble).sum / seq.length) }
    toDataFrame(s"groupBy${groupCol}.avg(${aggColName})", aggregated)
  }

  def count: DataFrame = {
    val aggregated = this.result.map { case (groupCol, seq) => (groupCol, seq.length.toDouble) }
    DataFrame(s"groupBy${groupCol}.cnt", columns = Seq(DataColumn(groupCol, aggregated.keys.toSeq), DataColumn("cnt", aggregated.values.toSeq.map(x => f"${x}%.4f"))))
  }

  def toDataFrame(name: String, aggResult: Map[String, Double]): DataFrame = {
    DataFrame(name, columns = Seq(DataColumn(groupCol, aggResult.keys.toSeq), DataColumn(aggColName, aggResult.values.toSeq.map(x => f"${x}%.4f"))))
  }
}