package com.data.demo

import org.apache.spark.{SparkConf, SparkContext}

object RDD_Test {
  def main(args: Array[String]): Unit = {
    val conf  = new SparkConf().setMaster("local").setAppName("rdd")
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(Array(("A",0), ("B", 2), ("B",1),("B",2),("B",3)))
    rdd1.foreach(println(_))


    for (elem <- rdd1.countByKey()){
      print(elem)
    }
  }

}
