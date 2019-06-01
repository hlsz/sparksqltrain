package com.data.spark.sparksql

import com.data.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class TradingFrequency {

  val conf = new SparkConf()
    .setAppName("TradingFrequency")
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

  def tradingFrequency(calcuDate: Int, approchMonths: Int, remoteMonths: Int, tableName: String, dataSourTab: String, targetCustTab: String): Unit =
  {

    val calcDateVal = DateUtils.intToDate(calcuDate)
    // addOrMinusDayToLong
    val approchMonthsVal = DateUtils.dateToInt(DateUtils.addMonth(calcDateVal, -approchMonths))
    val remoteMonthsVal = DateUtils.dateToInt(DateUtils.addMonth(calcDateVal, -remoteMonths))

    println("approchMonthsVal:"+approchMonthsVal)
    println("remoteMonthsVal:"+remoteMonthsVal)


    spark.sql("use bigdata")

    spark.sql("drop table if exists bigdata."+tableName)
    spark.sql("create table IF NOT EXISTS bigdata." + tableName +
      " ( c_custno string, appro_months_count double, " +
      " remo_months_count double, frequency_tendency double, branch_no string ) " +
      s" ROW FORMAT DELIMITED FIELDS TERMINATED BY ${raw"'\t'"} " +
      s" LINES TERMINATED BY ${raw"'\n'"}" +
      " stored as textfile ")

    val approMonthsAmountDF = spark.sql("select c_custno, count(c_custno) as appro_months_count " +
      " from bigdata." + dataSourTab +
      " where c_custno in (select c_custno from  " + targetCustTab + " ) " +
      " and l_date <  " + calcuDate +
      " and l_date >=  " + approchMonthsVal +
      " group by c_custno "
    )
    approMonthsAmountDF.createOrReplaceTempView("approMonthsAmountTmp")

    val remoMonthsAmountDF = spark.sql("select c_custno, count(c_custno) as remo_months_count " +
      " from bigdata." + dataSourTab +
      " where c_custno in (select c_custno from  " + targetCustTab + " ) " +
      " and l_date <  " + calcuDate +
      " and l_date >=  " + remoteMonthsVal +
      " group by c_custno "
    )
    remoMonthsAmountDF.createOrReplaceTempView("remoMonthsAmountTmp")

    val frequencyTradingDF = spark.sql(" select c_custno, appro_months_count, remo_months_count, " +
      " (case when remo_months_count = 0 then 0 else " +
      " appro_months_count * (" + remoteMonths + " / " + approchMonths + " ) " +
      " / remo_months_count end ) as frequency_tendency, branch_no " +
      " from ( select a.c_custno , " +
      " nvl( appro_months_count,0 ) as appro_months_count, " +
      " nvl( remo_months_count,0) as remo_months_count , " +
      " branch_no from " +
      " ( select c_custno, branch_no from " + targetCustTab + " ) a " +
      " left outer join remoMonthsAmountTmp  b on a.c_custno = b.c_custno " +
      " left outer join approMonthsAmountTmp  c on a.c_custno = c.c_custno ) d ")

    frequencyTradingDF.createOrReplaceTempView("frequencyTradingTmp")

    spark.sql(" insert overwrite  table bigdata."+tableName+" select * from frequencyTradingTmp"   )

//    df.write.mode("overwrite").saveAsTable(tableName)

    spark.stop()
  }
}

object TradingFrequency {

  def main(args: Array[String]): Unit = {

    new TradingFrequency().tradingFrequency(20190401, 1,3,"frequency_trading","trade_get_data","c_cust_branch_tb")

  }

  }



