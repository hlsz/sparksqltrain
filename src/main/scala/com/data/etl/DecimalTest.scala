package com.data.etl

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{Decimal, DecimalType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


object DecimalTest {
  def main(args: Array[String]): Unit = {
    //创建SparkSession
    val spark: SparkSession = SparkSession.builder().appName("DecimalTest").master("local[4]").getOrCreate()
    //读取数据创建临时视图
    val detailRDD: RDD[String] = spark.sparkContext.textFile("E:\\data\\dianshang\\input\\stockdetail.txt")
    val detailRow: RDD[Row] = detailRDD.map(line => {
      val splits = line.split(",")
      val ordernumber = splits(0) //订单号
      val rownum = splits(1).toInt //行号
      val itemid = splits(2) //货品
      val qty = splits(3).toInt //数量
      val price = Decimal(splits(4)) //价格
      val amount = Decimal(splits(5)) //总金额
      Row(ordernumber, rownum, itemid, qty, price, amount)
    })
    //创建字段信息
    val detailSch = StructType(List(
      StructField("ordernumber", StringType, true),
      StructField("rownum", IntegerType, true),
      StructField("itemid", StringType, true),
      StructField("qty", IntegerType, true),
      StructField("price", DecimalType(12,2), true),
      StructField("amount",DecimalType(12,2), true)
    ))
    val detailDF: DataFrame = spark.createDataFrame(detailRow, detailSch)
    //创建临时视图
    detailDF.createTempView("v_detail")

    //执行SQL语句
    val result = spark.sql("select ordernumber, sum(amount) total from detail group by ordernumber")

    //收集数据 触发action
    result.show()
    //关闭资源
    spark.stop()
  }
}
