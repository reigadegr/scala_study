package com.bcu.csvTest

import org.apache.spark.rdd.RDD

object ProductCount {
  def countProductsByFrequency(lines: RDD[String], num: Int): Unit = {
    // 提取商品ID
    val productRDD = lines.map(line => {
      val fields = line.split(",")
      fields(num).trim
    })

    // 统计每个相同str的出现次数
    val productCounts = productRDD.map((_, 1)).reduceByKey(_ + _)

    // 从大到小排序
    val sortedProductCounts = productCounts.sortBy(_._2, ascending = false)
    //    foreach打印
    sortedProductCounts.collect().foreach(println)
  }
}