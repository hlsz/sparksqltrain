package com.data.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class SparkInitConf
{

  def getSparkSession(): SparkSession ={
    val conf = new SparkConf()
      .setAppName("SparkJob")
      .setMaster("yarn-client")

    val spark = SparkSession
      .builder()
      .config(conf)
      .config("spark.sql.warehouse.dir","/user/hive/warehouse/bigdata")
      //解决DecimalType存储精度问题， parquet格式 spark和hive不统一
      .config("spark.sql.parquet.writeLegacyFormat", true)
      .enableHiveSupport()
      .getOrCreate()

    spark
  }

}

object SparkInitConf {


}
