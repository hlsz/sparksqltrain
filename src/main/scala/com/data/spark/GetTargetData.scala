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

  def getTargetData(tableName: String, beginDate: Integer, endDate: Integer): String = {

    //    val sparkContext = spark.sparkContext("use spark")
    spark.sql("use bigdata")

    spark.sql(" DROP TABLE IF EXISTS bigdata.c_cust_branch_tb ")
    spark.sql("create table if not exists bigdata.c_cust_branch_tb (client_id int, c_custno string, " +
      " branch_no string, organ_flag string, open_date int , birthday int ) " +
      s" ROW FORMAT DELIMITED FIELDS TERMINATED BY  ${raw"'\t'"}  " +

      s" LINES TERMINATED BY ${raw"'\n'"} " +
      " stored as textfile ")

    val cCustBranchTbDF = spark.sql("insert overwrite table bigdata.c_cust_branch_tb  " +
      " select client_id, c_custno, branch_no, organ_flag, open_date, birthday " +
      "from bigdata.hs08_client_for_ai  ")

    val previousDay = DateUtils.addOrMinusDay(new Date(), -1)
//    cCustBranchTbDF.createOrReplaceGlobalTempView("c_cust_branch_tb")

    spark.sql(" DROP TABLE IF EXISTS "+ tableName)
    spark.sql("create table if not exists  "+tableName +
      "(c_custno string, l_date int, f_businessamount double , f_businessprice double, f_businessbalance double, " +
      " c_businessflag string, c_moneytype string, f_fare0 double, branch_no string )" +
      s" ROW FORMAT DELIMITED FIELDS TERMINATED BY  ${raw"'\t'"}  " +

      s" LINES TERMINATED BY ${raw"'\n'"} " +
      " stored as textfile ")

    val tradeGetDataDF = spark.sql(
      s"""
         | select c_custno, oc_date, f_businessamount,  f_businessprice, f_businessbalance, c_businessflag,
         |    c_moneytype, f_fare0, branch_no
         |    from (  select t.c_custno,t.oc_date,t.business_amount f_businessamount,
         |     t.business_price f_businessprice, t.business_balance f_businessbalance,
         |     t.business_flag c_businessflag, t.money_type c_moneytype, t.fare0 f_fare0, c.branch_no
         |        from (select c_custno, branch_no, organ_flag from bigdata.c_cust_branch_tb  ) c
         |        left outer join bigdata.hs08_his_deliver  t on t.c_custno = c.c_custno
         |        where business_flag in (4001, 4002) and oc_date >= ${beginDate}  and oc_date <= ${endDate})
       """.stripMargin)

//    tradeGetDataDF.write.mode("overwrite").saveAsTable(tableName)

    tradeGetDataDF.createOrReplaceTempView("tradeGetDataTmp")

    spark.sql("insert overwrite  table bigdata."+tableName+" select * from tradeGetDataTmp ")

    spark.stop()

    return "success"
    "fail"
  }
}

class Gtd {

  val conf = new SparkConf()
    .setAppName("Gtd")
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

  val sc = spark.sparkContext

  val clientRdd = sc.textFile("/user/hdfs/hs08_clientinfo_for_ai")
  val clientRdd2 = clientRdd.map(_.split("\t")).filter(_.length == 20)
  val clientCount = clientRdd2.count()





  sc.stop()

}

object GetTargetData {
  def main(args: Array[String]): Unit = {
      new  GetTargetData().getTargetData("trade_get_data",20190101,20190401)
  }

 }


