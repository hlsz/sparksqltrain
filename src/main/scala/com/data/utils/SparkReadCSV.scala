package com.data.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SparkReadCSV {

  val conf = new SparkConf()
    .setAppName("CSv")
    .setMaster("local[2]")

  val spark = SparkSession
    .builder()
    .enableHiveSupport()
    .config(conf)
    .getOrCreate()

  val df = spark.read.format("com.databricks.spark.csv").option("header","true")
    .option("mode","DROPMALFORMED").load("people.csv").cache()

  case class Person(name:String, age:Long)

  import spark.implicits._

  val peopleDF = spark.sparkContext
    .textFile("people.txt")
    .map(_.split(","))
    .map(attr => Person(attr(0), attr(1).trim.toInt))
    .toDS()

  peopleDF.createOrReplaceTempView("people")

  val tDF = spark.sql("select name, age from people where age between 13 and 19")

  tDF.map(t => "name:"+t(0)).show()

  val peopleRDD = spark.sparkContext.textFile("people.txt")

  val schemaString = "name age"

  val fields = schemaString.split(" ")
    .map(fieldName => StructField(fieldName,StringType,true))
  val schema = StructType(fields)

  val rowRDD = peopleRDD
    .map(_.split(","))
    .map(attr => Row(attr(0), attr(1).trim.toInt))

  val peopleDF2 = spark.createDataFrame(rowRDD, schema)

  peopleDF2.createOrReplaceTempView("people2")

  val results = spark.sql("select name from people2 ")

  results.map(attr => "Name:" + attr(0)).show()

  val df2 = spark.read.format("com.databricks.spark.csv")
    .option("header","true")
    .option("mode","DROPMALFORMED")
    .load("people.json")
    .cache()

  val schemaString2 = "name,age"
  df2.createOrReplaceTempView("people3")
  var dataDF = spark.sql("select "+schemaString2 + " from people3")
  //转rdd
  var dfRDD = dataDF.rdd
  val fields3 = schemaString2.split(",")
    .map(fieldName => StructField(fieldName, StringType,true) )
  var schema3 = StructType(fields3)

  val newDF = spark.createDataFrame(dfRDD, schema3)


//  val rowRDD4 = rowRDD.map(attr => {
//    val myAttr:Array[String] = attr
//    val myColNameIndexArr:Array[Int] = c
//
//  })



}
