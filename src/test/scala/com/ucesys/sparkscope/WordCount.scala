/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

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
            WordType("typically", "adverb")
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