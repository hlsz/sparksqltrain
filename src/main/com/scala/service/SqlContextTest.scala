package com.scala.service

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SqlContextTest {

  def main(args: Array[String]): Unit = {

    val  sparkConf = new SparkConf()
    sparkConf.setAppName("SQLContext").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val filePath = "D:\\code\\java\\sparksqltrain\\src\\Resources\\people.json"
    val people = sqlContext.read.format("json").load(filePath)
    people.printSchema()
    people.show()

    sc.stop()

  }

}
