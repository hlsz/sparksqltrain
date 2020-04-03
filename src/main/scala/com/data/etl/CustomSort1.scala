package com.data.etl

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


object CustomSort1 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("IpLocation").setMaster("local")
    val sc = new SparkContext(conf)

    // 用spark对数据进行排序
    val users: Array[String] = Array("1,tom,99,34", "2,marry,96,26", "3,mike,98,29", "4,jim,96,30")

    // 并行化成RDD
    val userLines: RDD[String] = sc.makeRDD(users)

    //整理数据
    val userRdd: RDD[User1] = userLines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val fv = fields(2).toInt
      val age = fields(3).toInt
      new User1(id, name, fv, age)
    })

    //排序
    val sorted: RDD[User1] = userRdd.sortBy(u => u)

    //收集数据
    val result: Array[User1] = sorted.collect()

    println(result.toBuffer)

    sc.stop()
  }
}

class User1 (val id:Long,
             val name:String,
             val fv:Int,
             val age:Int) extends Ordered[User1] with Serializable {
  override def compare(that: User1): Int = {
    if (that.fv == this.fv) {
      this.age - that.age
    } else {
      -(this.fv - that.fv)
    }
  }

  override def toString: String = {
    s"User:{$id, $name, $fv, $age}"
  }

  val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
//  val sc = spark.sparkContext
//  val sqlContext = new SQLContext(sc)
//  注册udf函数 注意不要少了最后的一个下划线
  spark.udf.register("len", len _)
  spark.udf.register("longLength", lengthLongerThan _)

  def len(bookTitle: String):Int ={bookTitle.length}

  def lengthLongerThan(bookTitle:String, length:Int):Boolean = {
    bookTitle.length > length
  }

}
