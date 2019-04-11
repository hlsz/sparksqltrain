package com.data.process

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.xml.Null


object BizProcessMain {

  val conf = new SparkConf()
    .setAppName("BizProcessMain")
    .setMaster("yarn-client")

  val spark = SparkSession
    .builder()
    .config(conf)
    //解决DecimalType存储精度问题， parquet格式 spark和hive不统一
    .config("spark.sql.parquet.writeLegacyFormat", true)
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse/bigdata.db")
    //数据倾斜
    .config("spark.sql.shuffle.partitions", 200)
    .enableHiveSupport()
    .getOrCreate()


  def main(args: Array[String]): Unit = {


    spark.sql(
      s"""
         | create table if not exists dm_branch_client_cnt
         | (
         | branch_no string
         | ,client_cnt
         | )
     """.stripMargin)
    val branchClientCntDF = spark.sql(
      s"""
         | select
         | case when branch_no is null or branch_no='' then -1 else branch_no
         | ,client_cnt
         | from (
         |      select   branch_no, count(*) client_cnt
         |      from  hs08_client_for_ai
         |      group by branch_no
         |     )
       """.stripMargin)
    branchClientCntDF.createOrReplaceTempView("branchClientCntTmp")
    spark.sql("insert overwite dm_branch_client_cnt select * from branchClientCntTmp ")

    var calcuDate = args(0).toInt
    var maxIntervalVal = args(1).toInt
    var minintervalVal =   args(2)

    if ( Null.equals(minintervalVal))
      {
        new DmCusttotalassetdmProc().custtotalassetdmProc(calcuDate, maxIntervalVal)

        new  DmDeliverProc().deliverProc(calcuDate, maxIntervalVal)

        new DmBankTransferProc().bankTransferProc(calcuDate, maxIntervalVal)
      } else {
        var minintervalValInt = minintervalVal.toInt
        new DmCusttotalassetdmProc().custtotalassetdmProc(calcuDate, maxIntervalVal, minintervalValInt)

        new  DmDeliverProc().deliverProc(calcuDate, maxIntervalVal, minintervalValInt)

        new DmBankTransferProc().bankTransferProc(calcuDate, maxIntervalVal, minintervalValInt)
      }














  }

}
