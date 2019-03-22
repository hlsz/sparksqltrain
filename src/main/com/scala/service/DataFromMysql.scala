package com.scala.service

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object DataFromMysql {

  def main(args: Array[String]): Unit = {
    val spark : SparkSession =
      SparkSession.builder()
        .appName("DFMysql")
        .master("local")
        .getOrCreate()
    val prop : Properties = new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","admin")

    val mysqlDF : DataFrame = spark
      .read.jdbc("jdbc:mysql://192.168.44.31:3306/spark",
    "people",prop)
    mysqlDF.show()

    spark.stop()
  }

}
