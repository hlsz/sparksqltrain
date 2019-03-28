package com.data.spark

import com.data.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class PeakValueAsset {

  val conf = new SparkConf()
    .setAppName("GetTargetDataReplace")
    .setMaster("yarn-client")

  val spark = SparkSession
    .builder()
    .config(conf)
    //解决DecimalType存储精度问题， parquet格式 spark和hive不统一
    .config("spark.sql.parquet.writeLegacyFormat", true)
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse/bigdata")
    .enableHiveSupport()
    .getOrCreate()

  def peakValueAsset(resultDataTab:String, assetTb:String, targetDate:Int, dateContains:Int ): Unit =
  {

    val targetDateVal = DateUtils.intToDate(targetDate)
    // addOrMinusDayToLong
    val dateContainssVal = DateUtils.dateToInt(DateUtils.addOrMinusDay(targetDateVal, -dateContains))

    spark.sql("use bigdata")

    val peakAssetDF = spark.sql("select cust_no, max(total_assbal) peak_vasset " +
      " from " + assetTb+
      " where oc_date <= " + dateContainssVal +
      " group by cust_no")

  }

}

object PeakValueAsset {

  new PeakValueAsset().peakValueAsset("peak_vasset_tb", "dcraw.custtotalasset_dm", 20181008, 3)
}
