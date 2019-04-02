package com.data.spark

import com.data.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class PeakValueAsset {

  val conf = new SparkConf()
    .setAppName("PeakValueAsset")
    .setMaster("yarn-client")

  val spark = SparkSession
    .builder()
    .config(conf)
    //解决DecimalType存储精度问题， parquet格式 spark和hive不统一
    .config("spark.sql.parquet.writeLegacyFormat", true)
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse/bigdata")
    //数据倾斜
    .config("spark.sql.shuffle.partitions", 500)
    .enableHiveSupport()
    .getOrCreate()

  def peakValueAsset(resultDataTab:String, assetTb:String, targetDate:Int, dateContains:Int ): Unit =
  {

    val targetDateVal = DateUtils.intToDate(targetDate)
    // addOrMinusDayToLong
    val dateContainssVal = DateUtils.dateToInt(DateUtils.addMonth(targetDateVal, -1))
    val dateContainssVal3 = DateUtils.dateToInt(DateUtils.addMonth(targetDateVal, -3))

    spark.sql("use bigdata")

    spark.sql("create  table  IF NOT EXISTS  bigdata." + resultDataTab +
    " (c_custno string, peak_vasset double ) "+
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' " +
      " LINES TERMINATED BY ‘\n’ collection items terminated by '-' " +
      " map keys terminated by ':' " +
      " stored as textfile "
    )

    val peakAssetDF = spark.sql("select cust_no, nvl(max(total_assbal),0)  peak_vasset " +
      " from  global_temp.c_cust_branch_tb  a " +
      " left outer join " + assetTb+ " b on a.c_custno = b.cust_no " +
      " where b.oc_date >= " + dateContainssVal +
      " and b.oc_date < " + targetDate +
      " group by cust_no")
    peakAssetDF.createOrReplaceTempView("peakAssetTmp")
    spark.sql("insert overwrite table bigdata." +resultDataTab+ "select * from peakAssetTmp")

    spark.sql("select a.c_custno, nvl(mad(b.total_assbal),0) peak_vasset, "+targetDate +
    " from global_temp.c_cust_branch_tb a " +
      " left outer join bigdata.custtotalasset_dm b on a.c_custno = b.cust_no " +
      " where b.oc_date >= "+dateContainssVal3+
      " and b.oc_date < " + targetDate +
      " group by a.c_custno ")

    spark.stop()

  }

}

object PeakValueAsset {

  new PeakValueAsset().peakValueAsset("peak_vasset_tb", "dcraw.custtotalasset_dm", 20181008, 3)
}
