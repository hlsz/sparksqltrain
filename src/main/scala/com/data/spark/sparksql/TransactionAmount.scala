package com.data.spark.sparksql

import com.data.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

class  TransactionAmount
{
  val conf = new SparkConf()
    .setAppName("TransactionAmount")
    .setMaster("yarn-client")

  val spark = SparkSession
    .builder()
    .config(conf)
    //解决DecimalType存储精度问题， parquet格式 spark和hive不统一
    .config("spark.sql.parquet.writeLegacyFormat", true)
    //数据倾斜
    .config("spark.sql.shuffle.partitions", 500)
    //    .config("spark.sql.warehouse.dir","/user/hive/bigdata.db")
    .enableHiveSupport()
    .getOrCreate()

//  val sqlContext = spark.sqlContext

  case class CCustBranchTb(cCustNo:String, branchNo:String, organFlag:String)

  def transactionAmount(calcuDate:Int, approchMonths:Int, remoteMonths:Int, tableName:String, dataSourTable:String, targetCustTab:String): Unit =
  {
    spark.sql("use bigdata")
    //    val caseClassDS = Seq(CCustBranchTb("c0001","001","01")).toDS()
    //    caseClassDS.show()

    val calcDateVal = DateUtils.intToDate(calcuDate)
    // addOrMinusDayToLong
    val approchMonthsVal = DateUtils.dateToInt(DateUtils.addMonth(calcDateVal, -approchMonths))
    val remoteMonthsVal = DateUtils.dateToInt(DateUtils.addMonth(calcDateVal, -remoteMonths))
    println("approchMonthsVal:"+approchMonthsVal)
    println("remoteMonthsVal:"+remoteMonthsVal)

    val schema = StructType(List(
      StructField("c_custno", StringType, true),
      StructField("appro_months_amount",DoubleType, true),
      StructField("remo_months_amount", DoubleType, true),
      StructField("amount_tendency",DoubleType, true),
      StructField("branch_no", StringType, true)
    ))

    //drop Hive table
    spark.sql("drop table if exists  bigdata."+tableName)
    //create Hive table
    spark.sql("create  table  IF NOT EXISTS   bigdata."+ tableName +
      "( c_custno  string, appro_months_amount double, remo_months_amount double, " +
      " amount_tendency double, branch_no string ) " +
      s" ROW FORMAT DELIMITED FIELDS TERMINATED BY ${raw"'\t'"} " +

      s" LINES TERMINATED BY ${raw"'\n'"} " +
      " stored as textfile ")

    //    spark.sql("LOAD DATA LOCAL INPATH '/home/spark/topNGroup.txt' INTO TABLE tableName")


    val approMonthsAmountDF= spark.sql("select c_custno, " +
      " (case when sum(f_businessbalance) is null then 0 else sum(f_businessbalance) end ) as appro_months_amount " +
      " from  bigdata."+dataSourTable+
      " where c_custno in (select c_custno from "+targetCustTab+" ) " +
      " and l_date <  " + calcuDate +
      " and l_date >= " + approchMonthsVal +
      " group by c_custno  ")
    approMonthsAmountDF.createOrReplaceTempView("approMonthsAmountTmp")

    val remotMonthsAmountDF= spark.sql("select c_custno, " +
      "(case when sum(f_businessbalance) is null  then 0 else sum(f_businessbalance) end ) as remo_months_amount " +
      "from  bigdata."+dataSourTable+
      "where c_custno in (select c_custno from "+targetCustTab+" )" +
      " and l_date <  " + calcuDate +
      " and l_date >= " + remoteMonthsVal +
      " group by c_custno  ")
    remotMonthsAmountDF.createOrReplaceTempView("remotMonthsAmountTmp")

    //remot_months_amount_sql.limit(5).write.mode("overwrite").json("remot_months.json")
    //    remot_months_amount_sql.select("c_custno", "f_businessbalance")
    //      .filter("l_date < calcuDate and l_date >= remoteMonthsVal")


    val amountTransactionDF =  spark.sql("select c_custno, appro_months_amount, remo_months_amount,  " +
      " (case when remo_months_amount = 0 then 0 else appro_months_amount * ( " + remoteMonths +
      "  / " + approchMonths +") / remo_months_amount end ) as amount_tendency, branch_no  " +
      " from ( select a.c_custno, nvl(c.appro_months_amount ,0 ) as appro_months_amount, " +
      " nvl( b.remo_months_amount ,0) as  remo_months_amount , branch_no " +
      " from (select c_custno, branch_no from "+ targetCustTab +" ) a " +
      " left outer join remotMonthsAmountTmp b on a.c_custno =  b.c_custno " +
      " left outer join approMonthsAmountTmp  c on a.c_custno = c.c_custno )  d "
    )
    amountTransactionDF.createOrReplaceTempView("amountTransactionTmp")

    spark.sql("insert overwrite table bigdata."+tableName + " select * from amountTransactionTmp ")

    spark.stop()
}

}

object TransactionAmount {

  def main(args: Array[String]): Unit = {
    new TransactionAmount().transactionAmount(20190401,1,3,"amount_transaction", "trade_get_data","c_cust_branch_tb")
  }

}
