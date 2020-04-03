package com.data.spark.sql

import org.apache.spark.{SparkConf, SparkContext}

object joinTest extends App {


  val conf = new SparkConf()
    .setMaster("local[2]")
    .setAppName("test")
  val sc = new SparkContext(conf)



}
