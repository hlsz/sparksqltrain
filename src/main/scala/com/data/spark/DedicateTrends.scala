package com.data.spark

import com.data.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class DedicateTrends {

  val conf = new SparkConf()
    .setAppName("DedicateTrends")
    .setMaster("yarn-client")

  val spark = SparkSession
    .builder()
    .config(conf)
    //解决DecimalType存储精度问题， parquet格式 spark和hive不统一
    .config("spark.sql.parquet.writeLegacyFormat", true)
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse/bigdata.db")
    //数据倾斜
    .config("spark.sql.shuffle.partitions", 500)
    .enableHiveSupport()
    .getOrCreate()

  def dedicateTrends(calcuDate:Int, approchMonths:Int, remoteMonths:Int, tableName:String, dataSourTab:String, targetCustTab:String): Unit =
  {

    val calcDateVal = DateUtils.intToDate(calcuDate)
    // addOrMinusDayToLong
    val approchMonthsVal = DateUtils.dateToInt(DateUtils.addMonth(calcDateVal, -approchMonths))
    val remoteMonthsVal = DateUtils.dateToInt(DateUtils.addMonth(calcDateVal, -remoteMonths))
    println("approchMonthsVal:"+approchMonthsVal)
    println("remoteMonthsVal:"+remoteMonthsVal)

    spark.sql("use bigdata")

    spark.sql("drop table if exists bigdata."+tableName)
    spark.sql("create  table  IF NOT EXISTS  bigdata."+tableName + " (" +
      " c_custno string, f_fare0_approch double, f_fare0_remote double, f_fare0_tendency double, branch_no string ) " +
      s" ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' " +
      s" LINES TERMINATED BY ${raw"'\n'"} " +
      " stored as textfile ")

    val approMonthsAmountDF = spark.sql("select c_custno, sum(nvl(f_fare0,0)) f_fare0  " +
      " from (select c_custno, l_date, " +
      "    (case when c_moneytype = '0' then f_fare0 " +
      "          when c_moneytype = '1' then f_fare0 * 6.875" +
      "          when c_moneytype = '2' then f_fare0 * 0.8858 end ) as f_fare0 " +
      "      from bigdata." + dataSourTab +" " +
      "     where c_custno in (select c_custno from "+targetCustTab+")" +
      " and l_date >= "+approchMonthsVal+" " +
      " and l_date < "+calcuDate +" ) " +
      " group by c_custno " )

    approMonthsAmountDF.createOrReplaceTempView("approMonthsAmountTmp")


    val remoMonthsAmountDF = spark.sql("select c_custno, sum(nvl(f_fare0,0)) f_fare0  " +
      " from (select c_custno, l_date, " +
      "    (case when c_moneytype = '0' then f_fare0 " +
      "          when c_moneytype = '1' then f_fare0 * 6.875" +
      "          when c_moneytype = '2' then f_fare0 * 0.8858 end ) as f_fare0 " +
      "      from bigdata." + dataSourTab +" " +
      "     where c_custno in (select c_custno from "+targetCustTab+")" +
      " and l_date >= "+remoteMonthsVal+" " +
      " and l_date < "+calcuDate +" ) d " +
      " group by c_custno " )

    remoMonthsAmountDF.createOrReplaceTempView("remoMonthsAmountTmp")


    val trendDelicateDF = spark.sql("select c_custno, f_fare0_approch, f_fare0_remote, " +
      " (case when f_fare0_remote = 0 then  0 else f_fare0_approch * (" +remoteMonths+" / "+approchMonths+") / " +
      " f_fare0_remote end ) as f_fare0_tendency , branch_no " +
      " from (select nvl(a.c_custno,0) as c_custno, nvl(c.f_fare0,0) as f_fare0_approch, nvl(b.f_fare0, 0) as f_fare0_remote ," +
      "  a.branch_no " +
      "     from (select c_custno, branch_no from "+targetCustTab+") a " +
      "     left outer join  remoMonthsAmountTmp b on a.c_custno = b.c_custno " +
      "     left outer join approMonthsAmountTmp c on a.c_custno = c.c_custno ) d ")

    trendDelicateDF.createOrReplaceTempView("trendDelicateTmp")

    spark.sql("insert overwrite table  bigdata."+tableName+" select * from trendDelicateTmp ")

    spark.stop()

  }

}

object DedicateTrends {

  def main(args: Array[String]): Unit = {
    new DedicateTrends().dedicateTrends(20190401, 1, 3, "trend_dedicate", "trade_get_data", "c_cust_branch_tb")

  }


}