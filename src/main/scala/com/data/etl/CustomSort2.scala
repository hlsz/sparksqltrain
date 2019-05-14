package com.data.etl

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CustomSort2 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark")
      .setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("IpLocat")
      .setMaster("local")
    val sc = new SparkContext(conf)

    val users :Array[String] = Array("1,tom,99,34", "2,marry,96,26", "3,mike,98,29", "4,jim,96,30")

    val userLines: RDD[String] = sc.makeRDD(users)

    val userRdd: RDD[User2] = userLines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val fv = fields(2).toInt
      val age = fields(3).toInt
      User2(id, name, fv, age)
    })

    val sorted: RDD[User2] = userRdd.sortBy(u => u)

    val result: Array[User2] = sorted.collect()

    println(result.toBuffer)
    sc.stop()

  }
}

case class User2 (val id:Long,
                  val name:String,
                  val fv:Int,
                  val age:Int
                 ) extends Ordered[User2] {
  override def compare(that: User2): Int = {
    if (that.fv == this.fv) {
      this.age - that.age
    } else {
      -(this.fv - that.fv)
    }
  }

  override def toString: String = {
    s"User:{$id, $name, $fv, $age}"
  }
}