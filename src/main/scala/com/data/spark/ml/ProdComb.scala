package com.data.spark.ml

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object ProdComb {



  def main(args: Array[String]): Unit = {
    val startTime = 20180101
    val endTime = 20190101
    val conf = new SparkConf().setAppName("prodComb").setMaster("local[2]")
    val spark = SparkSession.builder().enableHiveSupport.config(conf).getOrCreate()
    val sc = spark.sparkContext

    val conf2 = HBaseConfiguration.create()
    conf2.set(TableInputFormat.INPUT_TABLE, "tableName")
    val rdd = sc.newAPIHadoopRDD(conf2, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])


    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
    Logger.getLogger("org.apache.hadoop").setLevel(Level.OFF)

    var start = System.currentTimeMillis()
    val linesRDD = sc.textFile("d:/tfile.txt")
    linesRDD.cache()
    linesRDD.persist(StorageLevel.MEMORY_ONLY)

    val retRDD = linesRDD.flatMap(_.split(" ")).map((_,1)).reduceByKey(_ + _)
    retRDD.cache()
    retRDD.persist(StorageLevel.MEMORY_ONLY)

    retRDD.count()
    println("first time:"+ (System.currentTimeMillis() - start) + "ms")

    // 执行第二次RDD的计算
    start = System.currentTimeMillis()
    linesRDD.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).count()
    retRDD.count()
    println("second time:" + (System.currentTimeMillis() - start) + "ms")

    //持久化使用结束之后，要想卸载数据
    linesRDD.unpersist()

    sc.stop()



    val ofPriceDF = spark.sql(
      s"""
         |select init_date, fund_code, nav
         |from ods_ofprice
         |where init_date >=${startTime}
         |and init_date < ${endTime}
     """.stripMargin.replace("\r\n"," "))

    ofPriceDF.createOrReplaceTempView("ofPriceTmp")

    val bd = spark.sparkContext.broadcast(ofPriceDF.collect())

    val ofPriceRDD = ofPriceDF.rdd.map( line => line.getValuesMap[Double](List("init_date","fund_code","nav")))


    // combinations(2).toList
  }


}
