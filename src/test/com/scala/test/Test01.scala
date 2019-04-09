package com.scala.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Test01 {

  val conf = new SparkConf()
    .setAppName("test01")
    .setMaster("local[2]")

  val spark = SparkSession
    .builder()
    .config(conf)
    .enableHiveSupport()
    .getOrCreate()

  val sqlContext = spark.sparkContext

  val aor_list = spark.sql("select * from tab")
  import spark.implicits._

  val hotel = """hotelsearch""".r // r 代表创建一个Regex对象
  val poi_supp_imp_with_hotel = spark.sql(
    s"""
       select
            `_mt_servername`, recommendstids, uuid
       from
           log.dataapp_recapi_search
       where
            dt='20160303' and length(recommendstids) > 7
     """).map(
    r => (r.getString(0), r.getString(1), r.getString(2))
  ).map{
    case (servername, stids, uuid) => (hotel.findFirstIn(servername), servername, stids, uuid)
  }
  poi_supp_imp_with_hotel.count()


  val poi_supp_table = poi_supp_imp_with_hotel.map(r => NorseAll(r._1,r._2,r._3,r._4)).toDF()
  poi_supp_table.show()
}
case class NorseAll(hotel:Option[String], servername:String, stids:String,uuid:String);
