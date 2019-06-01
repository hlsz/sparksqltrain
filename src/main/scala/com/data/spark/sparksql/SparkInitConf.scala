package com.data.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class SparkInitConf
{

  def getSparkSession(jobName:String,mode:String): SparkSession ={
    val conf = new SparkConf()
      .setAppName(jobName)
      .setMaster(mode)

    val spark = SparkSession
      .builder()
      .config(conf)
      .config("spark.sql.warehouse.dir","/user/hive/warehouse/bigdata.db")
      //数据倾斜
      .config("spark.sql.shuffle.partitions", 500)
      //解决DecimalType存储精度问题， parquet格式 spark和hive不统一
      .config("spark.sql.parquet.writeLegacyFormat", true)
      .enableHiveSupport()
      .getOrCreate()

    spark
  }

}

object SparkInitConf {


}
