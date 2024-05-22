package com.bcu.csvTest

import com.bcu.csvTest.ProductCount.countProductsByFrequency
import org.apache.spark.{SparkConf, SparkContext}

object SparkTest {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkTest")
    val sc = new SparkContext(conf)

    val inputPath = "scala_maven/data/input/data.txt"
    // 读取数据
    val lines = sc.textFile(inputPath)

//    开始函数调用
    println("开始统计商品类-点击总数")
    countProductsByFrequency(lines, 6)
    println("开始统计商品类-下单总数量")
    countProductsByFrequency(lines, 8)
    println("开始统计商品类-支付总数")
    countProductsByFrequency(lines, 10)
    //    需求2，统计session
    println("开始统计session的数量")
    countProductsByFrequency(lines, 2)
    sc.stop()
  }
}
