package com.data.spark

import com.data.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class CalOpenDate {

  val conf = new SparkConf()
    .setAppName("CalOpenDate")
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




  def  calOpenDate(calDate:Int, tableName:String): Unit =
  {
    spark.sql("use bigdata")

    spark.sql(" create table IF NOT EXISTS   bigdata."+ tableName +
      " ( branch_no string, c_custno string, open_date int , open_date_dvalue int) " +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' " +
      " LINES TERMINATED BY ‘\n’ collection items terminated by '-' " +
      " map keys terminated by ':' " +
      " stored as textfile ")


    val resultDataOpenDF = spark.sql("select branch_no, c_custno, open_date, " +
      " datediff(" +DateUtils.dateFormat(calDate,"'yyyyMMDD'")+ ", to_date(open_date, 'yyyyMMdd') )" +
      " as open_date_dvalue " +
      " from global_temp.c_cust_branch_tb ")
    resultDataOpenDF.createOrReplaceTempView("resultDataOpenTmp")

    spark.sql("insert overwrite table bigdata."+ tableName +"select * from resultDataOpenTmp ")

    spark.stop()

  }

}

object CalOpenDate
{
//  new CalOpenDate().calOpenDate(20180903,"open_date_tb")
}
