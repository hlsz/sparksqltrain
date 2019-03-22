package com.scala.service

import java.util.Properties

import org.apache.spark.sql.SparkSession

object SparkMysql {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("SparkMysql")
      .getOrCreate()

    val jdbcDF = spark.read
      .format("jdbc")
      .option("url","jdbc:mysql://localhost:3306/test")
      .option("dbtable", "wordcount")
      .option("user","root")
      .option("password","admin")
      .option("driver","com.mysql.jdbc.Driver")
      .load()

    //第二种方法
    val connectionProp = new Properties()
    connectionProp.put("user","root")
    connectionProp.put("password","admin")
    connectionProp.put("driver","com.mysql.jdbc.Driver")
    val jdbcDF2 = spark.read.jdbc("jdbc:mysql://localhost:3306/test","wordcount",connectionProp)

    jdbcDF.write
        .format("json")
        .option("url","jdbc:mysql://localhost:3306/test")
        .option("dbtable","wordcount")
        .option("user","root")
        .option("password","admin")
        .save()

    jdbcDF2.write
        .jdbc("jdbc:mysql://localhost:3306/test","wordcount",connectionProp)

    jdbcDF.write
        .option("createTableColumnType","name CHAR(64), comments VARCHAR(1024)")
        .jdbc("jdbc:mysql://localhost:3306/test","wordcount",connectionProp)
    jdbcDF.printSchema()
    jdbcDF.show()

  }

}
