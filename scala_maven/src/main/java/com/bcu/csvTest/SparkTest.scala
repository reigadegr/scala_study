package com.bcu.csvTest

import org.apache.spark.{SparkConf, SparkContext}

object SparkTest {
  def main(args: Array[String]): Unit = {
    // 设置Spark配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkTest")
    // 创建Spark上下文
    val sc = new SparkContext(conf)

    val inputPath = "scala_maven/data/input/data.txt"
    // 读取数据
    val lines = sc.textFile(inputPath)

    // 解析数据并提取商品ID
    val productRDD = lines.map(line => {
      val fields = line.split(",")
      if (fields.length > 7) {
        fields(7).trim
      }
    })

    // 统计每个商品ID的出现次数
    val productCounts = productRDD.map((_, 1)).reduceByKey(_ + _)

    // 打印结果
    productCounts.collect().foreach(println)

    // 关闭Spark上下文
    sc.stop()
  }
}
