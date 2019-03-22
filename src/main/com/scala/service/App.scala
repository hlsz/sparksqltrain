package com.scala.service

import javax.ws.rs.core.Application
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * Hello world!
 *
 */
object App extends Application {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("SparkJob")
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()

     spark.sql("use default")
     spark.sql("show tables").collect().foreach(println(_))
     spark.sql("select * from default.prediction limit 100").collect().foreach(println(_))

     spark.sql("create table if not exists prediction(id int, name string) row format " +
       " delimited fields terminated by ',' collection items terminated by '-' map keys terminated by  ':'")
    .collect().foreach(println)

     spark.sql("insert into prediction  values(1,'big') ").collect().foreach(println)
  //for implicit  conversions like converting RDDS to Dataframes

      val df = spark.read.json("src/Resources/people.json")

      val userJsonDF = spark.read.format("json").load("people.json")
      userJsonDF.printSchema()

    val userParquetDF = spark.read.parquet("people.parquet")
    userParquetDF.printSchema()

    // spark.read.text返回 DataFrame
    val rdd =spark.read.text("people.txt")





    df.show()


  }

 }
