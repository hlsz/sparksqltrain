package com.scala.service

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SparkSqlToMysql {

  def main(args: Array[String]): Unit = {

    val spark : SparkSession =
      SparkSession
      .builder()
      .appName("SparkSqlToMysql")
      .master("local")
      .getOrCreate()

    val sc : SparkContext =
      spark.sparkContext
    val fileRDD : RDD[String] =
      sc.textFile("people.txt")
    //切分
    val lineRDD : RDD[Array[String]] =
      fileRDD.map(_.split(","))

    //关联 通过StructType 指定schema将rdd转换成DataFrame
    

  }

}
