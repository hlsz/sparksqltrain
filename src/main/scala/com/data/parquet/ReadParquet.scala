package com.data.parquet
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

object ReadParquet {

    def main(args: Array[String]): Unit ={
      val conf = new SparkConf()
        .setAppName("QJZK")
        .setMaster("local")
      val sc = new SparkContext(conf)
      val sQLContext = new SQLContext(sc)

      //读取一个Parquet文件
      val paquetDF = sQLContext.read.format("parquet").load("D:\\work\\input\\*")
      val dd = paquetDF.rdd //转换成RDD格式

      //读取Parquet文件Schema结构
      val parquetschema = sQLContext.parquetFile("D:\\work\\input\\*.parquet")

      println(paquetDF.count())
      paquetDF.show()
    }
}
