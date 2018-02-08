package com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object AverageHousePriceSolution {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("avgHousePrice").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/RealEstate.csv")
    val cleanedLines = lines.filter(line => !line.contains("Bedrooms"))

    val housePricePairRdd = cleanedLines.map(line => (line.split(",")(3), AvgCount(1, line.split(",")(2).toDouble)))

    val housePriceTotal = housePricePairRdd.reduceByKey((x, y) => AvgCount(x.count + y.count, x.total + y.total))

    println("housePriceTotal: ")
    for ((bedroom, total) <- housePriceTotal.collect()) println(bedroom + " : " + total)

    val housePriceAvg = housePriceTotal.mapValues(avgCount => avgCount.total / avgCount.count)
    println("housePriceAvg: ")
    for ((bedroom, avg) <- housePriceAvg.collect()) println(bedroom + " : " + avg)
  }
}
