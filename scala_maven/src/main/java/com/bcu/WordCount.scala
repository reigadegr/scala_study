package com.bcu

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val config: SparkConf = new SparkConf().setAppName("wordCount").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(config)
    sc.setLogLevel("WARN")
    val lines: RDD[String] = sc.textFile("scala_maven/data/input/words.txt")

    val words: RDD[String] = lines.flatMap(_.split(" "))
    val wordAndOnes: RDD[(String, Int)] = words.map((_, 1))

    val wordAndCount: RDD[(String, Int)] = wordAndOnes.reduceByKey(_ + _)

    val result: Array[(String, Int)] = wordAndCount.collect()
    result.foreach(println)
  }
}
