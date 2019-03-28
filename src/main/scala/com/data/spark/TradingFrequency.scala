package com.data.spark

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
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse/bigdata")
    .enableHiveSupport()
    .getOrCreate()

  def tradingFrequency(calcuDate: Int, approchMonths: Int, remoteMonths: Int, tableName: String, dataSourTab: String, targetCustTab: String): Unit =
  {

    val calcDateVal = DateUtils.intToDate(calcuDate)
    // addOrMinusDayToLong
    val approchMonthsVal = DateUtils.dateToInt(DateUtils.addOrMinusDay(calcDateVal, -approchMonths))
    val remoteMonthsVal = DateUtils.dateToInt(DateUtils.addOrMinusDay(calcDateVal, -remoteMonths))

    spark.sql("use bigdata")

    spark.sql("create table IF NOT EXISTS bigdata." + tableName + " ( c_custno string, appro_months_count double, " +
      " remo_months_count double, frequency_tendency double, branch_no string ) " +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' " +
      " LINES TERMINATED BY ‘\n’ collection items terminated by '-' " +
      " map keys terminated by ':' " +
      " stored as textfile ")

    val appro_months_amount_df = spark.sql("select c_custno, count(c_custno) as appro_months_count " +
      " from bigdata." + dataSourTab +
      " where c_custno in (select c_custno from  global_temp." + targetCustTab + " ) " +
      " and l_date <  " + calcuDate +
      " and l_date >=  " + approchMonthsVal +
      " groupu by c_custno "
    )

    appro_months_amount_df.createOrReplaceTempView("appro_months_amount")


    val remo_months_amount_df = spark.sql("select c_custno, count(c_custno) as appro_months_count " +
      " from bigdata." + dataSourTab +
      " where c_custno in (select c_custno from  global_temp." + targetCustTab + " ) " +
      " and l_date <  " + calcuDate +
      " and l_date >=  " + remoteMonthsVal +
      " groupu by c_custno "
    )
    remo_months_amount_df.createOrReplaceTempView("remo_months_amount")

    val frequencyTradingDF = spark.sql(" select c_custno, appro_months_count, remo_months_count, " +
      " (case when remo_months_count = 0 then 0 else " +
      " appro_months_count * (" + remoteMonths + " / " + approchMonths + " ) " +
      " / remo_months_count end ) as frequency_tendency, branch_no " +
      " from ( select a.c_custno as c_custno, " +
      " (case when appro_months_count is null then 0 else appro_months_count end ) as appro_months_count, " +
      " (case when remo_months_count is null then 0 else remo_months_count end ) as remo_months_count , " +
      " branch_no from " +
      " ( select c_custno, branch_no from global_temp." + targetCustTab + " ) a " +
      " left outer join remo_months_amount  b on a.c_custno = b.c_custno " +
      " left outer join appro_months_amount  c on a.c_custno = c.c_custno )")

    frequencyTradingDF.createOrReplaceTempView("frequencyTradingTmp")

    spark.sql(" insert overwrite into bigdata."+tableName+" select * from frequencyTradingTmp"   )

//    df.write.mode("overwrite").saveAsTable(tableName)

    spark.stop()
  }
}

object TradingFrequency {

  def main(args: Array[String]): Unit = {

//    new TradingFrequency().tradingFrequency("")

  }

  }



