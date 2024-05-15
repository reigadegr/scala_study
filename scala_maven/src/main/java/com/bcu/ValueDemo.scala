package com.bcu

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ValueDemo {
  def main(args: Array[String]): Unit = {
    //先获取sparkcontext对象
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ValueDemo")
    val sc: SparkContext = new SparkContext(conf);
    //通过
    val dataRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val dataRDD2: RDD[Int] = dataRDD.map(num => {
      num * 2
    })
    val dataRDD3: RDD[String] = dataRDD.map(num => {
      num + ""
    })

    val dataRDD4 = dataRDD.mapPartitions(
      datas => {
        datas.filter(_ > 4)
      })

    val dataRDD5 = dataRDD.mapPartitionsWithIndex((index, datas) => {
      datas.map((index, _))
    })

    val dataRDD6 = sc.makeRDD(List(List(1, 2, 3), List(4, 5, 6, 7, 8)), 1)
    val dataRDD7 = dataRDD6.flatMap(
      list => list
    )
    dataRDD2.collect().foreach(println)
    dataRDD3.collect().foreach(println)
    dataRDD4.collect().foreach(println)
    dataRDD5.collect().foreach(println)

    //flatmap 将处理的数据进行扁平化后在进行映射处理（压扁）
    dataRDD7.collect().foreach(println)

    //将一个分区内的数据转换为数组，打印的是内存地址
    val dataRDD8 = dataRDD.glom()
    dataRDD8.collect().foreach(println)

    val dataRDD9 = sc.makeRDD(List(5, 4, 3, 6, 8, 9, 1, 3, 6, 7, 2, 4, 6, 8))
    //进行奇偶数分组
    val dataRDD10 = dataRDD9.groupBy(_ % 2)
    dataRDD10.collect().foreach(println)

    //过滤 保留2的倍数
    val dataRDD11 = dataRDD9.filter(_ % 2 == 0)
    dataRDD11.collect().foreach(println)

    //去重复元素
    val dataRDD12 = dataRDD9.distinct()
    dataRDD12.collect().foreach(println)

  }
}
