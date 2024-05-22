package com.bcu.csvTest

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object HotCategoryRanking {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("HotCategoryRanking")
      .master("local[*]") // 在本地运行，生产环境中请使用 "yarn" 或其他集群管理器
      .getOrCreate()

    import spark.implicits._

    // 读取CSV文件数据
    val uservisitDF = spark.read
      .option("header", true)
      .csv("scala_maven/data/input/data.txt")

    // 处理点击、下单、支付数据
    val clickCountDF = uservisitDF
      .filter($"click_category_id".isNotNull)
      .groupBy("click_category_id")
      .count()
      .withColumnRenamed("count", "clicks")

    val orderCountDF = uservisitDF
      .filter($"order_category_ids".isNotNull && length($"order_category_ids") > 0)
      .selectExpr("explode(split(order_category_ids, ',')) as order_category_id")
      .groupBy("order_category_id")
      .count()
      .withColumnRenamed("count", "orders")

    val payCountDF = uservisitDF
      .filter($"pay_category_ids".isNotNull && length($"pay_category_ids") > 0)
      .selectExpr("explode(split(pay_category_ids, ',')) as pay_category_id")
      .groupBy("pay_category_id")
      .count()
      .withColumnRenamed("count", "pays")

    // 合并三个统计结果
    val combinedCounts = clickCountDF
      .join(orderCountDF, Seq("click_category_id"), "left_outer")
      .join(payCountDF, Seq("click_category_id"), "left_outer")
      .na.fill(0)

    // 计算综合排名分数
    val rankedDF = combinedCounts
      .withColumn("rank_score", col("clicks") * 0.2 + col("orders") * 0.3 + col("pays") * 0.5)
      .orderBy(desc("rank_score"), desc("clicks"), desc("orders"), desc("pays"))

    // 展示结果
    rankedDF.show()

    spark.stop()
  }
}