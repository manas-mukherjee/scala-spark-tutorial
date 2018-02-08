package com.sparkTutorial.rdd.collect

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}

object CollectExample {
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("collect").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val inputWords = List("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop")
    //Convert string of words to a string rdd
    val wordRdd = sc.parallelize(inputWords)

    //String RDD to list of strings
    val words = wordRdd.collect()

    //Print list of strings
    for (word <- words) println(word)
  }
}
