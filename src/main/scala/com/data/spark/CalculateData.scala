package com.data.spark

import com.data.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


class CalculateData {

  val conf = new SparkConf()
    .setAppName("CalculateData")
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

  def calculateData(branchCustTb:String, frequencyTradeTb:String, amountTransacTb:String, lastTradeTimeTb:String,
                    dedicateTrendsTb:String, openDateTb:String, peakVassetTb:String, inputDate:Int): Unit =
  {
    val currentDate = DateUtils.dateToInt(DateUtils.getCurrentDate)

    spark.sql("use bigdata")

    val branchCustCountDF = spark.sql("select branch_no, count(c_custno) as c_cust_count " +
      " from "+branchCustTb+" group by branch_no ")
    branchCustCountDF.createOrReplaceTempView("branchCustCountTmp")

    spark.sql(" create table IF NOT EXISTS  bigdata.cal_data_tb ( " +
      " c_custno string, branch_no string, appro_months_amount double, remo_months_amount double," +
      " amount_tendency double, appro_months_count double, remo_months_count double, " +
      " frequency_tendency double, l_date int, c_businessflag string,  " +
      " lastdate_dvalue int, f_fare0_approch double, f_fare0_remote double, f_fare0_tendency double, " +
      " open_date  int, open_date_dvalue int, organ_flag string, peak_vasset double, insert_date int, input_date int   )" +
      s" ROW FORMAT DELIMITED FIELDS TERMINATED BY ${raw"'\t'"} " +

      s" LINES TERMINATED BY ${raw"'\n'"} " +
      " stored as textfile ")

    val calDataDF = spark.sql("select k.*, nvl(l.peak_vasset,0) as peak_vasset ,"+currentDate+" as insert_date, "+inputDate +" inputdate " +
      " from  ( select i.c_custno, i.branch_no, nvl(appro_months_amount,0) as appro_months_amount , " +
      " nvl(remo_months_amount,0) as remo_months_amount, nvl(amount_tendency,0) as amount_tendency ," +
      " nvl(appro_months_count,0) as appro_months_count, nvl(remo_months_count,0) as remo_months_count, " +
      " nvl(frequency_tendency,0) as frequency_tendency, l_date, c_businessflag,  lastdate_dvalue, " +
      " nvl(f_fare0_approch,0) as f_fare0_approch, nvl(f_fare0_remote, 0) as f_fare0_remote, " +
      " nvl(f_fare0_tendency,0) as f_fare0_tendency , open_date, open_date_dvalue, organ_flag " +
      " from (select g.*, f_fare0_approch, f_fare0_remote, f_fare0_tendency  " +
      "       from ( select e.*, f.l_date, f.c_businessflag,  f.lastdate_dvalue " +
      "            from (select c.*, appro_months_count, remo_months_count, frequency_tendency " +
      "                  from (select a.c_custno, a.branch_no, a.organ_flag, appro_months_amount, remo_months_amount, amount_tendency " +
      "                      from "+branchCustTb+" a " +
      "                      left join "+amountTransacTb+" b on a.c_custno = b.c_custno) c " +
      "                  left join "+frequencyTradeTb+" d  on c.c_custno = d.c_custno ) e " +
      "            left join "+lastTradeTimeTb+" f on e.c_custno = f.c_custno) g " +
      "       left join "+dedicateTrendsTb+" h  on g.c_custno = h.c_custno  ) i " +
      "   left join "+openDateTb+" j on i.c_custno = j.c_custno ) k " +
      " left join "+peakVassetTb+"  l on k.c_custno = l.c_custno ")

    calDataDF.createOrReplaceTempView("calDataTmp")

    val custnoCountRDD = spark.sql("select c_custno from calDataTmp where input_date = " + inputDate)
    val custnoCount = custnoCountRDD.count().toInt

    spark.sql("delete from bigdata.cal_data_tb  where input_date = "+ inputDate )
    spark.sql("insert into bigdata.cal_data_tb select * from calDataTmp ")

    //--计算各指标
    //--计算各营业部各指标的均值、中值(营业部所有客户的均值/中值)
    val branchAvgMedDF = spark.sql(" select a.branch_no, " +
      "  appro_amount_avg, remo_amount_avg, amount_tend_avg, appro_count_avg, remo_count_avg, " +
      " frequency_tend_avg, last_dv_avg,appro_fare0_avg, remo_fare0_avg, fare0_tend_avg,open_d_dvalue_avg, " +
      " peak_vasset_avg, appro_amount_med, remo_amount_med, amount_tend_med, appro_count_med, remo_count_med, " +
      " frequency_tend_med, last_dv_med, appro_fare0_med,remo_fare0_med, fare0_tend_med, open_d_dvalue_med, peak_vasset_med " +
      " from (select 'ALL' branch_no, " +
      " avg(appro_months_amount) as appro_amount_avg , " +
      " avg(remo_months_amount) as remo_amount_avg, " +
      " avg(amount_tendency) amount_tend_avg, " +
      " avg(appro_months_count) appro_count_avg,  " +
      " avg(remo_months_count) remo_count_avg, " +
      " avg(frequency_tendency) frequency_tend_avg, " +
      " avg(f_fare0_approch) appro_fare0_avg, " +
      " avg(f_fare0_remote) remo_fare0_avg, " +
      " avg(f_fare0_tendency) fare0_tend_avg, " +
      " avg(open_date_dvalue) open_d_dvalue_avg," +
      " avg(peak_vasset) peak_vasset_avg,  " +
      " median(appro_months_amount) as appro_amount_med , " +
      " median(remo_months_amount) as remo_amount_med, " +
      " median(amount_tendency) amount_tend_med, " +
      " median(appro_months_count) appro_count_med,  " +
      " median(remo_months_count) remo_count_med, " +
      " median(frequency_tendency) frequency_tend_med, " +
      " median(f_fare0_approch) appro_fare0_med, " +
      " median(f_fare0_remote) remo_fare0_med, " +
      " median(f_fare0_tendency) fare0_tend_med, " +
      " median(open_date_dvalue) open_d_dvalue_med ," +
      " median(peak_vasset) peak_vasset_med " +
      "  from ( select c_custno, branch_no, " +
              " nvl(appro_months_amount, 0) appro_months_amount, " +
              " nvl(remo_months_amount,0) remo_months_amount , " +
              " nvl(amount_tendency,0)  amount_tendency," +
              " nvl(appro_months_count,0)  appro_months_count , " +
              " nvl(remo_months_count, 0) remo_months_count, " +
              " nvl(frequency_tendency,0) frequency_tendency," +
              " nvl(f_fare0_approch,0) f_fare0_approch ," +
              " nvl(f_fare0_remote,0) f_fare0_remote, " +
              " nvl(f_fare0_tendency,0) f_fare0_tendency, " +
              " nvl(open_date_dvalue,0) open_date_dvalue " +
              " from bigdata.cal_data_tb t where input_date = " +inputDate+" ) " +
      " group by branch_no ) a " +
      " left outer join (select  branch_no, avg(lastdate_dvalue) last_dv_avg, median(lastdate_dvalue) last_dv_med " +
      "  from cal_data_tb where lastdate_dvalue != -1 and input_date =  "+ inputDate +
      "  group by branch_no ) b  on a.branch_no = b.branch_no ")
    branchAvgMedDF.createOrReplaceTempView("branchAvgMedTmp")

    // 计算所有客户的均值，中值
    val allAvgMedDF = spark.sql(" select a.branch_no, " +
      " appro_amount_al_avg, remo_amount_al_avg, amount_tend_al_avg, appro_count_al_avg, remo_count_al_avg, " +
      " frequency_tend_al_avg, last_dv_al_avg,appro_fare0_al_avg, remo_fare0_al_avg, fare0_tend_al_avg, " +
      " open_d_dvalue_al_avg, peak_vasset_avg, appro_amount_al_med, remo_amount_al_med, amount_tend_al_med, appro_count_al_med, " +
      " remo_count_al_med, frequency_tend_al_med, last_dv_al_med, appro_fare0_al_med,remo_fare0_al_med, " +
      " fare0_tend_al_med, open_d_dvalue_al_med , peak_vasset_med" +
      " from (select 'ALL' branch_no, " +
      " avg(appro_months_amount) as appro_amount_al_avg , " +
      " avg(remo_months_amount) as remo_amount_al_avg, " +
      " avg(amount_tendency) amount_tend_al_avg, " +
      " avg(appro_months_count) appro_count_al_avg,  " +
      " avg(remo_months_count) remo_count_al_avg, " +
      " avg(frequency_tendency) frequency_tend_al_avg, " +
      " avg(f_fare0_approch) appro_fare0_al_avg, " +
      " avg(f_fare0_remote) remo_fare0_al_avg, " +
      " avg(f_fare0_tendency) fare0_tend_al_avg, " +
      " avg(open_date_dvalue) open_d_dvalue_al_avg," +
      " avg(peak_vasset) peak_vasset_avg ,  " +
      " median(appro_months_amount) as appro_amount_al_med , " +
      " median(remo_months_amount) as remo_amount_al_med, " +
      " median(amount_tendency) amount_tend_al_med, " +
      " median(appro_months_count) appro_count_al_med,  " +
      " median(remo_months_count) remo_count_al_med, " +
      " median(frequency_tendency) frequency_tend_al_med, " +
      " median(f_fare0_approch) appro_fare0_al_med, " +
      " median(f_fare0_remote) remo_fare0_al_med, " +
      " median(f_fare0_tendency) fare0_tend_al_med, " +
      " median(open_date_dvalue) open_d_dvalue_al_med ," +
      " median(peak_vasset) peak_vasset_med " +
      " from ( select c_custno, branch_no, " +
                   " nvl(appro_months_amount, 0) appro_months_amount, " +
                    " nvl(remo_months_amount,0) remo_months_amount , " +
                    " nvl(amount_tendency,0)  amount_tendency," +
                    " nvl(appro_months_count,0)  appro_months_count , " +
                    " nvl(remo_months_count, 0) remo_months_count, " +
                    " nvl(frequency_tendency,0) frequency_tendency," +
                    " nvl(f_fare0_approch,0) f_fare0_approch ," +
                    " nvl(f_fare0_remote,0) f_fare0_remote, " +
                    " nvl(f_fare0_tendency,0) f_fare0_tendency, " +
                    " nvl(open_date_dvalue,0) open_date_dvalue, " +
                    " nvl(peak_vasset,0) peak_vasset " +
                    " from cal_data_tb t  where input_date = "+inputDate+")) a " +
      " left outer join (select 'ALL' branch_no, " +
      " avg(lastdate_dvalue) last_dv_al_avg, " +
      " median(lastdate_dvalue) last_dv_al_med " +
      "  from cal_data_tb where lastdate_dvalue != -1 and input_date = "+inputDate+ " ) b " +
      " on a.branch_no = b.branch_no ")

    allAvgMedDF.createOrReplaceGlobalTempView("allAvgMedTmp")
    //计算客户交易金额在营业部的排名
    val bTradeAmRankDF = spark.sql("select t.branch_no, c_custno, " +
      " (case when amount_tendency = 0 " +
      " then -1 else round((c_cust_count - rk ) * 100 / c_cust_count, 4) end ) rak " +
      " from (select c_custno, branch_no, amount_tendency, dense_rank() over(partition by branch_no order by amount_tendency desc ) rk " +
      "        from (select c_custno, branch_no, appro_months_amount amount_tendency from cal_data_tb where input_date = "+inputDate+")) a" +
      " left outer join branchCustCountTmp t on a.branch_no = t.branch_no ")
    bTradeAmRankDF.createOrReplaceTempView("bTradeAmRankTmp")
    //计算客户交易频率在营业部的排名
    val bTradeFrRankDF = spark.sql("select t.branch_no, c_custno, " +
      " (case when frequency_tendency = 0 " +
      " then -1 else round((c_cust_count - rk ) * 100 / c_cust_count, 4) end ) rak " +
      " from (select c_custno, branch_no, frequency_tendency, dense_rank() over(partition by branch_no order by frequency_tendency desc ) rk " +
      "        from (select c_custno, branch_no, appro_months_count frequency_tendency from cal_data_tb where input_date = "+inputDate+")) a" +
      " left outer join branchCustCountTmp t on a.branch_no = t.branch_no ")
    bTradeFrRankDF.createOrReplaceTempView("bTradeFrRankTmp")

    val bPeakVassetRankDF = spark.sql("select t.branch_no, c_custnno, (case when peak_vasset = 0 then  -1 " +
      " else round((c_cust_count - rk ) * 100 / c_cust_count, 4) end ) rak " +
      " from (select c_custno, branch_no, peak_vasset, dense_rank() over (partition by branch_no order by peak_vasset desc ) rk " +
      "     from (select c_custno, branch_no, peak_vasset from cal_data_tb where input_date = '"+inputDate+"'))")
    bPeakVassetRankDF.createOrReplaceTempView("bPeakVassetRankTmp")

    val allPeakVassetRankDF = spark.sql("select branch_no, c_custno, (case when peak_vasset = 0  then -1 else " +
      " round(("+custnoCount+" - rk) * 100 / "+custnoCount+" , 4 ) end ) rak " +
      " from (select c_custno, branch_no, peak_vasset, dense_rank() over (order by peak_vasset desc ) rk " +
      "  from (select c_custno, branch_no, peak_vasset from cal_data_tb and input_date = "+inputDate+" )) ")
    allPeakVassetRankDF.createOrReplaceTempView("allPeakVassetRankTmp")

    //计算客户最近一次交易时间在营业部的排名
    val bLstTradeDtRankDF = spark.sql("select t.branch_no,c_custno, l_date, c_bussinessflag,   " +
      " lastdate_dvalue, (case when l_date = 0 and lastdate_dvalue = 100 the  -1 " +
      " when l_date = 0 and lastdate_dvalue  = 200 the  -1   " +
      " when l_date = 0 and lastdate_dvalue  = 400 the  -1" +
      " else round((c_cust_count - rk)  * 100 / c_cust_count ,4) end ) rak " +
      " from (select c_custno, branch_no, l_date, c_businessflag,   lastdate_dvalue, " +
      "     dense_rank() over (partition by branch_no order by lastdate_dvalue asc ) rk " +
      "   from cal_data_tb " +
      "   where input_date = "+inputDate+") a " +
      " left join branchCustCountTmp t on a.branch_no = t.branch_no ")
    bLstTradeDtRankDF.createOrReplaceTempView("bLstTradeDtRankTmp")

    //计算客户贡献趋势在营业部的排名
    val bFare0TendRankDF = spark.sql("select t.branch_no, c_custno, (case when f_fare0_tendency = 0 " +
      " then -1 else round((c_cust_count -rk ) * 100 / c_cust_count, 4) end ) rak  " +
      " from ( select c_custno, branch_no, f_fare0_tendency,dense_rank() over (partition by  branch_no order by f_fare0_tendency desc) rk " +
      "     from (select c_custno, branch_no, f_fare0_tendency from cal_data_tb where input_date = "+inputDate+")) a " +
      "left outer join branchCustCountTmp t on a.branch_no = t.branch_no ")
    bFare0TendRankDF.createOrReplaceTempView("bFare0TendRankTmp")

    val bFare0RankDF =spark.sql("select t.branch_no, c_custno, (case when f_fare0_approch = 0 then -1 " +
      " else round((c_cust_count - rk) * 100 / c_cust_count, 4) end ) rak " +
      " from (select c_custno, branch_no, f_fare0_approch, dense_rank() over (partition by branch_no order by f_fare0_approch " +
      " desc ) rk from (select c_custno, branch_no, f_fare0_approch, from cal_data_tb where input_date = "+inputDate+")) a " +
      " left join branchCustCountTmp t on a.branch_no = t.branch_no ")
    bFare0RankDF.createOrReplaceTempView("bFare0RankTmp")
    //计算客户开户时长在营业部的排名
    val openDateRankDF = spark.sql("select t.branch_no, c_custno, open_date, open_date_dvalue ," +
      " round((c_cust_count - rk ) * 100 / c_cust_count , 4) rak " +
      " from (select c_custno, branch_no, open_date, open_date_dvalue, " +
      " dense_rank() over (partition by  branch_no order by open_date_dvalue desc ) rk " +
      " from cal_data_tb where input_date = "+inputDate+")) a" +
      " left outer join branchCustCountTmp t on a.branch_no = t.branch_no ")
    openDateRankDF.createOrReplaceTempView("openDateRankTmp")
    //计算客户交易金额在全部客户中的排名
    val allTradeAmRankDF = spark.sql("select branch_no,c_custno, (case when amount_tendency = 0 then -1 " +
      " else round(("+custnoCount+" - rk ) * 100 / "+custnoCount+" , 4 ) end ) rak " +
      " from (select c_custno, branch_no, amount_tendency, dense_rank() over(order by amount_tendency desc) rk " +
      "     from (select c_custno, branch_no,appro_months_amount amount_tendency from cal_data_tb where input_date = "+inputDate+"))")
    allTradeAmRankDF.createOrReplaceTempView("allTradeAmRankTmp")
    //计算客户交易频率在全部客户中的排名
    val allTradeFrRankDF = spark.sql("select branch_no,c_custno, (case when frequency_tendency = 0 then -1 " +
      " else round(("+custnoCount+" - rk ) * 100 / "+custnoCount+" , 4 ) end ) rak " +
      " from (select c_custno, branch_no, frequency_tendency, dense_rank() over(order by frequency_tendency desc) rk " +
      "     from (select c_custno, branch_no, appro_months_count frequency_tendency from cal_data_tb where input_date= "+inputDate+"))")
    allTradeFrRankDF.createOrReplaceTempView("allTradeFrRankTmp")
    //计算客户最近一次交易时间在全部客户中的排名
    val allLstTradeDtRankDF = spark.sql("select t.branch_no,c_custno, l_date, c_bussinessflag,  " +
      " lastdate_dvalue, (case when l_date = 0 and lastdate_dvalue = 100 the  -1 " +
      " when l_date = 0 and lastdate_dvalue  = 200 the  -1   " +
      " when l_date = 0 and lastdate_dvalue  = 400 the  -1" +
      " else round((c_cust_count - rk)  * 100 / c_cust_count ,4) end rak " +
      " from (select c_custno, branch_no, l_date, c_businessflag,  lastdate_dvalue, " +
      "     dense_rank() over (order by lastdate_davalue asc ) rk " +
      "   from cal_data_tb where input_date = "+inputDate+") a " +
      " left join cal_data_tb t on a.branch_no = t.branch_no ")
    allLstTradeDtRankDF.createOrReplaceTempView("allLstTradeDtRankTmp")

    //计算客户贡献趋势在全部客户中的排名
    val allFare0TendRankDF = spark.sql("select t.branch_no, c_custno, (case when f_fare0_tendency = 0 " +
      " then -1 else round(("+custnoCount+" -rk ) * 100 / "+custnoCount+", 4) end ) rak  " +
      " from ( select c_custno, branch_no, f_fare0_tendency,dense_rank() over (order by f_fare0_tendency desc) rk " +
      "     from (select c_custno, branch_no, f_fare0_tendency fromm cal_data_tb  where input_date = "+inputDate+")  ) ")
    allFare0TendRankDF.createOrReplaceTempView("allFare0TendRankTmp")

    val allFare0RankDF = spark.sql("select branch_no, c_custno, (case when f_fare0_approch = 0 then " +
      " -1 else round(("+custnoCount+" - rk ) * 100 / "+custnoCount+", 4) end ) rak " +
      " from ( select c_custno, branch_no, f_fare0_approch, dense_rank() over(order by f_fare0_approch desc ) rk " +
      "    from (select c_custno, branch_no, f_fare0_approch from cal_data_tb where input_date = "+inputDate+"))")
    allFare0RankDF.createOrReplaceTempView("allFare0RankTmp")

    //计算客户开户时长在全部客户中的排名
    val allOpenDateRankDF = spark.sql("select t.branch_no, c_custno, open_date, open_date_dvalue ," +
      " round(("+custnoCount+" - rk ) * 100 / "+custnoCount+" , 4) rak " +
      " from (select c_custno, branch_no, open_date, open_date_dvalue, " +
      "        dense_rank() over (order by open_date_dvalue desc ) rk " +
      " from cal_data_tb where input_date = "+inputDate+") ")
    allOpenDateRankDF.createOrReplaceTempView("allOpenDateRankTmp")

    //创建各指标均指/中值表(包括营业部及全部客户)
    spark.sql("create  table  IF NOT EXISTS  bigdata.b_all_avg_med_tb (" +
      " branch_no string, appro_amount_avg double, " +
      " remo_amount_avg double, amount_tend_avg double, " +
      " appro_count_avg double, remo_count_avg double, " +
      " frequency_tend_avg double, last_dv_avg double, " +
      " appro_fare0_avg double, remo_fare0_avg double, " +
      " fare0_tend_avg double, open_d_dvalue_avg double, " +
      " peak_vasset_avg double, " +
      " appro_amount_med double, remo_amount_med double, " +
      " amount_tend_med double, appro_count_med double, " +
      " remo_count_med double, frequency_tend_med double, " +
      " last_dv_med double, appro_fare0_med double," +
      " remo_fare0_med double, fare0_tend_med double, " +
      " open_d_dvalue_med double, peak_vasset_med double, " +
      " insert_date int , input_date int ) " +
      s" ROW FORMAT DELIMITED FIELDS TERMINATED BY ${raw"'\t'"} " +

      s" LINES TERMINATED BY ${raw"'\n'"} " +
      " stored as textfile " )

    spark.sql("insert overwrite table  bigdata.b_all_avg_med_tb ( ,branch_no appro_amount_avg, remo_amount_avg," +
      "     amount_tend_avg, appro_count_avg, remo_count_avg, frequency_tend_avg, last_dv_avg, appro_fare0_avg, remo_fare0_avg, " +
      "     fare0_tend_avg, open_d_dvalue_avg, peak_vasset_avg, appro_amount_med, remo_amount_med,amount_tend_med, appro_count_med, remo_count_med, " +
      "     frequency_tend_med, last_dv_med, appro_fare0_med, remo_fare0_med, fare0_tend_med, open_d_dvalue_med, peak_vasset_med, insert_date)" +
      " select branch_no, appro_amount_avg, remo_amount_avg, amount_tend_avg, appro_count_avg, remo_count_avg, " +
      " frequency_tend_avg, last_dv_avg, appro_fare0_avg, remo_fare0_avg, fare0_tend_avg, open_d_dvalue_avg, " +
      " appro_amount_med, remo_amount_med,amount_tend_med, appro_count_med, remo_count_med, frequency_tend_med, " +
      " last_dv_med, appro_fare0_med, remo_fare0_med, fare0_tend_med, open_d_dvalue_med, "+currentDate+" insert_date, " +
      inputDate +" input_date  " +
      " from branchAvgMedTmp ")

    spark.sql("insert overwrite  table  bigdata.b_all_avg_med_tb ( branch_no, appro_amount_avg, remo_amount_avg," +
      "     amount_tend_avg, appro_count_avg, remo_count_avg, frequency_tend_avg, last_dv_avg, appro_fare0_avg, remo_fare0_avg, " +
      "     fare0_tend_avg, open_d_dvalue_avg,peak_vasset_avg, appro_amount_med, remo_amount_med,amount_tend_med, appro_count_med, remo_count_med, " +
      "     frequency_tend_med, last_dv_med, appro_fare0_med, remo_fare0_med, fare0_tend_med, open_d_dvalue_med,peak_vasset_med, insert_date)" +
      " select branch_no,   appro_amount_al_avg, remo_amount_al_avg, amount_tend_al_avg, appro_count_al_avg, remo_count_al_avg, " +
      " frequency_tend_al_avg, last_dv_al_avg,appro_fare0_al_avg, remo_fare0_al_avg, fare0_tend_al_avg, " +
      " open_d_dvalue_al_avg, appro_amount_al_med, remo_amount_al_med, amount_tend_al_med, appro_count_al_med, " +
      " remo_count_al_med, frequency_tend_al_med, last_dv_al_med, appro_fare0_al_med,remo_fare0_al_med, " +
      " fare0_tend_al_med, open_d_dvalue_al_med ,"+currentDate+" insert_date , " +inputDate+" input_date" +
      " from allAvgMedTmp" )

    spark.sql("create  table  IF NOT EXISTS  bigdata.client_rank_tb ( " +
      " c_custno string, branch_no string, trade_b_amount_rank double, trade_b_frequency_rank double, " +
      " last_b_trade_time_rank double, fare0_b_tend_rank double, open_date_b_rank double, trade_all_amount_rank double, " +
      " trade_all_frequency_rank double, last_all_trade_time_rank double, fare0_all_tend_rank double, open_date_all_rank double, " +
      " fare0_b_rank double, fare0_all_rank double, peakasset_b_rank double, peakasset_all_rank double " +
      " insert_date int , input_date int )  " +
      s" ROW FORMAT DELIMITED FIELDS TERMINATED BY ${raw"'\t'"} " +

      s" LINES TERMINATED BY ${raw"'\n'"} " +
      " stored as textfile ")

    spark.sql("insert into  bigdata.client_rank_tb ( " +
      " c_custno, branch_no, trade_b_amount_rank, trade_b_frequency_rank, last_b_trade_time_rank, fare0_b_tend_rank," +
      " open_data_b_rank, trade_all_amount_rank, trade_all_frequency_rank, last_all_trade_time_rank, fare0_all_tend_rank," +
      " open_date_all_rank, fare0_b_rank, fare0_all_rank, peakasset_b_rank, peakasset_all_rank, " +
      " insert_date , input_date ) " +
      " select b.c_custno,b.branch_no, b.rak, c.rak, d.rak, e.rak, j.rak,f.rak,g.rak, h.rak, i.rak, k.rak , m.rak,o.rak,l.rak,n.rak, " +
      currentDate+"  insert_date , " +inputDate + " input_date " +
      " from bTradeAmRankTmp b  " +
      " left outer join bTradeFrRankTmp c on b.c_custno = c.c_custno " +
      " left outer join bLstTradeDtRankTmp d on b.c_custno = d.c_custno " +
      " left outer join bFare0TendRankTmp e on b.c_custno = e.c_custno " +
      " left outer join openDateRankTmp j on b.c_custno = j.c_custno " +
      " left outer join allTradeAmRankTmp f on b.c_custno = f.c_custno " +
      " left outer join allTradeFrRankTmp g on b.c_custno = g.c_custno " +
      " left outer join allLstTradeDtRankTmp h on b.c_custno = h.c_custno " +
      " left outer join allFare0TendRankTmp i on b.c_custno = i.c_custno " +
      " left outer join allOpenDateRankTmp k on b.c_custno = k.c_custno " +
      " left outer join bPeakVassetRankTmp l on b.c_custno = l.c_custno " +
      " left outer join bFare0RankTmp m on b.c_custno = m.c_custno " +
      " left outer join allPeakVassetRankTmp n on b.c_custno = n.c_custno " +
      " left outer join allFare0RankTmp o on b.c_custno = o.c_custno " )

    spark.stop()

  }

}

object CalculateData {
  def main(args: Array[String]): Unit = {

    new CalculateData().calculateData("c_cust_branch_tb", "frequency_trading", "amount_transaction",
    "trade_time_last","trend_dedicate","open_date_tb","peak_vasset_tb",20190401)


  }

}