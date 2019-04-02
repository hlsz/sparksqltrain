package com.data.spark


import java.util.Date

import com.data.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class CCustBranchTb(cCustNo:String, branchNo:String,organFlag:String)

class GetTargetData{

  val conf = new SparkConf()
    .setAppName("GetTargetData")
    .setMaster("yarn-client")

  val spark = SparkSession
    .builder()
    .config(conf)
    //解决DecimalType存储精度问题， parquet格式 spark和hive不统一
    .config("spark.sql.parquet.writeLegacyFormat", true)
    //数据倾斜
    .config("spark.sql.shuffle.partitions", 500)
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse/bigdata.db")
    .enableHiveSupport()
    .getOrCreate()

  def getTargetData(tableName: String, beginDate: Integer, endDate: Integer): Unit = {

    //    val sparkContext = spark.sparkContext("use spark")
    spark.sql("use bigdata")

    val cCustBranchTbDF = spark.sql("select " +
      "client_id, c_custno, branch_no, organ_flag, open_date, birthday " +
      "from bigdata.hs08_client_for_ai  ")

    val previousDay = DateUtils.addOrMinusDay(new Date(), -1)
    cCustBranchTbDF.createOrReplaceGlobalTempView("c_cust_branch_tb")

    val tradeGetDataDF = spark.sql(
      s"""
         | select c_custno, l_date, f_businessamount,  f_businessprice, f_businessbalance, c_businessflag,
         |    c_moneytype, f_fare0, c_remark, branch_no
         |    from (  select t.c_custno,t.l_date,t.f_businessamount,t.f_businessprice, t.f_businessbalance  t.c_businessflag,
         |        t.c_moneytype, t.f_fare0, t.c_remark, c.branch_no
         |        from (select c_custno, branch_no, organ_flag from global_temp.c_cust_branch_tb  )
         |        left outer join bigdata.hs08_his_deliver  t on t.c_custno = c.custno
         |        where c_businessflag in (4001, 4002) and l_date >= ${beginDate}  and l_date <= ${endDate})
       """.stripMargin)

//    tradeGetDataDF.write.mode("overwrite").saveAsTable(tableName)

    tradeGetDataDF.createOrReplaceTempView("tradeGetDataTmp")

    spark.sql("insert  overwrite "+tableName+" select * from tradeGetDataTmp ")

    spark.stop()
  }
}

object GetTargetData {
//  new  GetTargetDataReplace().getTargetDataReplace("trad_get_date",20180903,20181203)

 }


