package com.data.demo

import org.apache.spark.{SparkConf, SparkContext}

object RDDTest {
  def main(args: Array[String]): Unit = {
    val conf  = new SparkConf().setMaster("local").setAppName("rdd")
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(Array(("A",0), ("B", 2), ("B",1),("B",2),("B",3)))
    rdd1.foreach(println(_))


    for (elem <- rdd1.countByKey()){
      println(elem)
    }

    val cnt = sc.accumulator(0)
    val rdd2 = sc.makeRDD(1 to 10, 2)
    rdd2.foreach(println(_))
    rdd2.foreach(x => cnt += x)
    println(cnt)


    val rdd3 = sc.makeRDD(Seq(3, 6,7,1,2,0), 2)
    rdd3.sortBy(x=>x).collect().foreach(println(_))

    rdd3.sortBy(x=>x, false).collect().foreach(println(_))

    //RDD[K, V]类型
    //按照key来排序
    rdd1.sortBy(x=>x).collect().foreach(println(_))

    //按照VALUE的升序排列，false降序
    rdd1.sortBy(x=>x._2, true).collect().foreach(println(_))

  }

}
