package com.data.spark

import com.data.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


class CalculateData {

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

  def calculateData(branchCustTb:String, frequencyTradeTb:String, amountTransacTb:String, lastTradeTimeTb:String,
                    dedicateTrendsTb:String, openDateTb:String, peakVassetTb:String): Unit =
  {
    val currentDate = DateUtils.dateToInt(DateUtils.getCurrentDate)

    val branchCustDF = spark.sql("select branch_no, count(c_custno) as c_cust_count " +
      " from "+branchCustTb+" group by branch_no ")

    branchCustDF.createOrReplaceTempView("branchCustTmp")

    spark.sql(" create table IF NOT EXISTS  bigdata.cal_date_tb ( " +
      " c_custno string, branch_no string, appro_months_amount double, remo_months_amount double," +
      " amount_tendency double, appro_months_count double, remo_months_count double, " +
      " frequency_tendency double, l_date int, c_businessflag string, c_remark string, " +
      " lastdate_dvalue int, f_fare0_approch double, f_fare0_remote double, f_fare0_tendency double, " +
      " open_date  int, open_date_dvalue int, organ_flag string, peak_vasset double, insert_date int  )" +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' " +
      " LINES TERMINATED BY ‘\n’ collection items terminated by '-' " +
      " map keys terminated by ':' " +
      " stored as textfile ")

    val calDataDF = spark.sql("select k.*, nvl(l.peak_vasset,0) as peak_vasset ,"+currentDate+" as insert_date " +
      " from  ( select i.c_custno, i.branch_no, nvl(appro_months_amount,0) as appro_months_amount , " +
      " nvl(remo_months_amount,0) as remo_months_amount, nvl(amount_tendency,0) as amount_tendency ," +
      " nvl(appro_months_count,0) as appro_months_count, nvl(remo_months_count,0) as remo_months_count, " +
      " nvl(frequency_tendency,0) as frequency_tendency, l_date, c_businessflag, c_remark, lastdate_dvalue, " +
      " nvl(f_fare0_approch,0) as f_fare0_approch, nvl(f_fare0_remote, 0) as f_fare0_remote, " +
      " nvl(f_fare0_tendency,0) as f_fare0_tendency , open_date, open_date_dvalue, organ_flag " +
      " from (select g.*, f_fare0_approch, f_fare0_remote, f_fare0_tendency  " +
      "       from ( select e.*, f.l_date, f.c_businessflag, f.c_remark, f_lastdate_dvalue " +
      "            from (select c.*, appro_months_count, remo_months_count, frequency_tendency " +
      "                  from (select a.c_custno, a.branch_no, a.organ_flag, appro_months_amount, remo_months_amout, amount_tendency " +
      "                      from global_temp."+branchCustTb+" a " +
      "                      left join "+amountTransacTb+" b on a.c_custno = b.c_custno) c " +
      "                  left join "+frequencyTradeTb+" d  on c.c_custno = d.c_custno ) e " +
      "            left join "+lastTradeTimeTb+" f on e.c_custno = f.c_custno) g " +
      "       left join "+dedicateTrendsTb+" h  on g.c_custno = h.c_custno  ) i " +
      "   left join "+openDateTb+" j on i.c_custno = j.c_custno ) k " +
      " left join "+peakVassetTb+"  l on k.c_custno = l.c_custno ")

    val branchAvgMedDF = spark.sql(" select a.branch_no, " +
      "  appro_amount_avg, remo_amount_avg, amount_tend_avg, appro_count_avg, remo_count_avg, " +
      " frequency_tend_avg, last_dv_avg,appro_fare0_avg, remo_fare0_avg, fare0_tend_avg,open_d_dvalue_avg, " +
      " appro_amount_med, remo_amount_med, amount_tend_med, appro_count_med, remo_count_med, " +
      " frequency_tend_med, last_dv_med, appro_fare0_med,remo_fare0_med, fare0_tend_med, open_d_dvalue_med " +
      " from (select branch_no, " +
      " avg(appro_months_amount) as appro_amount_avg , " +
      " avg(remo_months_amount) as remo_amount_avg, " +
      " avg(amount_tendency) amount_tend_avg, " +
      " avg(appro_months_count) appro_count_avg,  " +
      " avg(remo_months_count) remo_count_avg, " +
      " avg(frequency_tendency) frequency_tend_avg, " +
      " avg(f_fare0_approch) appro_fare0_avg, " +
      " avg(f_fare0_remote) remo_fare0_avg, " +
      " avg(f_fare0_tendency) fare0_tend_avg, " +
      " avg(open_date_dvalue) open_d_dvalue_avg, " +
      " median(appro_months_amount) as appro_amount_med , " +
      " median(remo_months_amount) as remo_amount_med, " +
      " median(amount_tendency) amount_tend_med, " +
      " median(appro_months_count) appro_count_med,  " +
      " median(remo_months_count) remo_count_med, " +
      " median(frequency_tendency) frequency_tend_med, " +
      " median(f_fare0_approch) appro_fare0_med, " +
      " median(f_fare0_remote) remo_fare0_med, " +
      " median(f_fare0_tendency) fare0_tend_med, " +
      " median(open_date_dvalue) open_d_dvalue_med " +
      "  from ( select c_custno, branch_no, " +
      "        nvl(appro_months_amount, 0) appro_months_amount, " +
      "         nvl(remo_months_amount,0) remo_months_amount , " +
      "        nvl(amount_tendency,0)  amount_tendency," +
      " nvl(appro_months_count,0)  appro_months_count , ) "+
      " )")

  }


}

object CalculateData {

}