package com.sparkTutorial.pairRdd.sort

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


object SortedWordCountProblem {

  /* Create a Spark program to read the an article from in/word_count.text,
     output the number of occurrence of each word in descending order.

     Sample output:

     apple : 200
     shoes : 193
     bag : 176
     ...
   */

  //https://stackoverflow.com/questions/32954621/sortbykey-function-in-spark-scala-not-working-properly

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)

    //using 3 core
    val conf = new SparkConf().setAppName("wordCounts").setMaster("local")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/word_count.text")
    val wordRdd = lines.flatMap(line => line.split(" "))

    val wordPairRdd = wordRdd.map(word => (word, 1))
    val wordCounts = wordPairRdd.reduceByKey((x, y) => x + y)

    val flippedWordCounts = wordCounts.map(word => (word._2, word._1))

    val sortedWordCounts = flippedWordCounts.sortByKey(ascending = false)

    val secondFlippedWordCounts = sortedWordCounts.map(word => (word._2, word._1))
    for ((word, count) <- secondFlippedWordCounts.collect()) println(word + " , " + count)
  }
}

