package scala.com.data.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class CalculateResultData {

  val conf = new SparkConf()
    .setAppName("CalculateResultData")
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

  def calculateResultData(custAvgMedTb:String, custRankTb:String, calSourceDataTb:String, lowasset:Int, highasset:Int): Unit ={

    spark.sql("use bigdata")

    spark.sql("create  table  IF NOT EXISTS  bigdata.cust_result_info_tb ( " +
      " c_custno string, branch_no string, cust_classify_flag string,  appro_months_amount double, remo_months_amount double, " +
      " amount_tendency double, appro_months_count double, remo_months_count double, frequency_tendency double, l_date int, " +
      " c_businessflag string, c_remark string, lastdate_dvalue int, f_fare0_approch double, f_fare0_remote double, " +
      " f_fare0_tendency double, open_date int, open_date_dvalue int, peak_vasset double, insert_date int , " +
      " trade_b_amount_rank double, trade_b_frequency_rank double, " +
      " last_b_trade_rank double, fare0_b_tend_rank double, open_date_b_rank double, trade_all_amount_rank double, " +
      " trade_all_frequency_rank double, last_all_trade_time_rank  double, fare0_all_tend_rank double, open_date_all_rank double, " +
      " appro_amount_avg double, remo_amount_avg double, amount_tend_avg double, appro_count_avg double, remo_count_avg double," +
      " frequency_tend_avg double, last_dv_avg double, appro_fare0_avg double, remo_fare0_avg double, fare0_tend_avg double, " +
      " open_d_dvalue_avg double, appro_amount_med double, remo_amount_med double, amount_tend_med double, appro_count_med double, " +
      " remo_count_med double, frequency_tend_med double, last_dv_med double, appro_fare0_med double, remo_fare0_med double, fare0_tend_med double ," +
      " open_d_dvalue_med double  )" +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' " +
      " LINES TERMINATED BY ‘\n’ collection items terminated by '-' " +
      " map keys terminated by ':' " +
      " stored as textfile ")

    val custResultInfoDF  = spark.sql("select c.*, d.appro_amount_avg, d.remo_amount_avg, d.amount_tend_avg, d.appro_count_avg, " +
      " d.remo_count_avg, d.frequency_tend_avg, d.last_dv_avg, d.appro_fare0_avg, d.remo_fare0_avg, d.fare0_tend_avg, " +
      " d.open_d_dvalue_avg, d.appro_amount_med, d.remo_amount_med, d.amount_tend_med, d.appro_count_med, d.remo_count_med, " +
      " d.amount_tend_med, d.frequency_tend_med, d.last_dv_med, d.appro_fare0_med, d.remo_fare0_med, d.fare0_tend_med, d.open_d_dvalue_med " +
      " from (  select a.c_custno, , a.branch_no, " +
            " (case when peak_vasset <= "+ lowasset +" then 1  " +
            " when peak_vasset >= "+ highasset +" then 2 " +
            " when peak_vasset > "+lowasset +" and peak_vasset < "+highasset+" and f_fare0_approch = 0 then 3  end ) " +
            " cust_classify_flag,  a.appro_months_amount, a.remo_months_amount, a.amount_tendency, " +
            " a.appro_months_count, a.remo_months_count, a.frequency_tendency, a.l_date, a.c_businessflag, a.c_remark, a.lastdate_dvalue, " +
            " a.f_fare0_approch, a.f_fare0_remote, a.f_fare0_ tendency, a.open_date, a.open_date_dvalue, a.peak_vasset, " +
            " a.insert_date, b.trade_b_amount_rank, b.trade_b_frequency_rank, b.last_b_trade_time_rank, b.fare0_b_tend_rank, " +
            " b.open_date_b_rank, b.trade_all_amount_rank, b.trade_all_frequency_rank, b.last_all_trade_time_rank, " +
            " b.fare0_all_tend_rank, b.open_date_all_rank " +
            " from "+calSourceDataTb+" a " +
            " left join "+custRankTb+" b on a.c_custno = b.c_custno  ) c " +
      " left join "+custAvgMedTb + " d on c_branch_no = d.branch_no ")
    custResultInfoDF.createOrReplaceTempView("custResultInfoTmp")

    spark.sql("insert overwrite table cust_result_info_tb select * from custResultInfoTmp where peak_vasset " +
      "<= "+lowasset +" or peak_vasset >= " +highasset+" or ( peak_vasset > "+lowasset+" and peak_vasset < "+highasset+" " +
      " and f_fare0_approch = 0 ) ")

    spark.sql("create  table  IF NOT EXISTS  k_means_source_tb  " +
      "( c_custno string, branch_no string, amount_tendency double, frequency_tendency double, " +
      "  lastdate_dvalue int, f_fare0_tendency double, open_date_dvalue int, peak_vasset double, f_fare0_approch double )" +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' " +
      " LINES TERMINATED BY ‘\n’ collection items terminated by '-' " +
      " map keys terminated by ':' " +
      " stored as textfile " )

    spark.sql("insert overwrite table k_means_source_tb " +
      " select c_custno, branch_no, amount_tendency, frequency_tendency, lastdate_dvalue, f_fare0_tendency, open_date_dvalue," +
      " peak_vasset, f_fare0_approch " +
      " from  custResultInfoTmp where peak_vasset > "+lowasset+" and peak_vasset < "+highasset+" and f_fare0_approch != 0 ")

    spark.stop()

  }


}
