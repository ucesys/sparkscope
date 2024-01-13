package com.ucesys.sparkscope

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.col

case class Word(word: String)
case class WordWithCount(word: String, count: Long)
case class WordType(word: String, wordType: String)

object WordCount {

    def main(args:Array[String]): Unit = {

        val spark: SparkSession = SparkSession.builder()
          .appName("WordCount")
          .getOrCreate()

        import spark.implicits._

        val wordTypes = Seq(
            WordType("Project", "noun"),
            WordType("Science", "noun"),
            WordType("wayside", "noun"),
            WordType("this", "noun"),
            WordType("National", "adjective"),
            WordType("watch", "verb"),
            WordType("could", "verb"),
            WordType("specifies", "verb"),
            WordType("typically", "adverb"),
        ).toDS()

        val words: Dataset[Word] = spark.read.option("lineSep", " ").text(args(0)).select(col("value").alias("word")).as[Word]
        println("initial partition count:" + words.rdd.getNumPartitions)

        val wordsRepartitioned = words.repartition(args(1).toInt)
        println("re-partition count:" + wordsRepartitioned.rdd.getNumPartitions)

        val dfFiltered = wordsRepartitioned.filter(_.word != "")

        val grouped = dfFiltered.groupBy("word").count().sort(col("count").desc).as[WordWithCount]
        grouped.show()

        val joined = grouped.join(wordTypes, Seq("word"),"left")
        joined.show()
    }
}