package com.scala.service

import org.apache.spark.sql.SparkSession

object SparkSessionTest {

  def main(args: Array[String]) :Unit = {
    //val path = args(0)
    val spark = SparkSession.builder().appName("SparkSessionTest").master("local[3]").getOrCreate()
    val filePath = "D:\\code\\java\\sparksqltrain\\src\\Resources\\people.json"
    val people = spark.read.json(filePath)
    people.printSchema()
    people.show()

    spark.stop()

  }
}
