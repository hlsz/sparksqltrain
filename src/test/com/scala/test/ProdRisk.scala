package com.scala.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable

class ProdRisk {

  private val conf = new SparkConf()
    .setAppName("BizProcessMain")
    //rdd压缩  只有序列化后的RDD才能使用压缩机制
    .set("spark.rdd.compress", "true")

    //设置并行度
    .set("spark.default.parallelism", "100")
    //优化shuffle 读写
    .set("spark.shuffle.file.buffer","128k")
    .set("spark.reducer.maxSizeInFlight","96M")
    //合并map端输出文件
    .set("spark.shuffle.consolidateFiles", "true")
  //设置executor堆外内存
  //    .set("spark.yarn.executor.memoryOverhead","2048M")
      .setMaster("yarn-client")

  val spark = SparkSession
    .builder()
    .config(conf)
    //解决DecimalType存储精度问题， parquet格式 spark和hive不统一
    .config("spark.sql.parquet.writeLegacyFormat", true)
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse/bigdata.db")
    //数据倾斜
    .config("spark.sql.shuffle.partitions", 30)
    .enableHiveSupport()
    .getOrCreate()

  val sc = spark.sparkContext


  val rdd = sc.textFile("hdfs://user/spark/his_ofprice")
  val ofpriceLine = rdd.map(line => line.split(","))
  ofpriceLine.map(r => Row(r(0), r(1), r(2)))

  val schema :StructType = StructType(mutable.ArraySeq(
    StructField("init_date",StringType,false),
    StructField("nav",DoubleType,false),
    StructField("fund_code",StringType,false)
  ))

  var weigths = (new util.Random()).nextDouble()
  weigths = weigths /







}
