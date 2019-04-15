package com.data.process

import com.data.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object DmDeliverProc {
  def main(args: Array[String]): Unit = {
    new DmDeliverProc().deliverProc(20190401,3)

  }

}

class DmDeliverProc {

  val conf = new SparkConf()
    .setAppName("DmDeliverProc")
//    .setMaster("yarn-client")

  val spark = SparkSession
    .builder()
    .config(conf)
    //解决DecimalType存储精度问题， parquet格式 spark和hive不统一
    .config("spark.sql.parquet.writeLegacyFormat", true)
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse/bigdata.db")
    //数据倾斜
//    .config("spark.sql.shuffle.partitions", 30)
    .enableHiveSupport()
    .getOrCreate()


  // 设置参数
  // hive > set  hive.exec.dynamic.partition.mode = nonstrict;
  // hive > set  hive.exec.dynamic.partition = true;
  spark.sql("set  hive.exec.dynamic.partition.mode = nonstrict")
  spark.sql("set  hive.exec.dynamic.partition = true")

  /**
    *
    * @param endDate
    * @param maxIntervalVal
    */
  def deliverProc( endDate:Int, maxIntervalVal:Int ): Unit ={

    val minIntervalValInt = 1
    deliverProc(endDate, maxIntervalVal, minIntervalValInt)

  }

  /**
    *
    * @param endDate
    * @param maxIntervalVal
    * @param minIntervalVal
    */
  def deliverProc( endDate:Int, maxIntervalVal:Int, minIntervalVal:Int ): Unit =
  {

    var endDateFormatStr  =  DateUtils.intToDateStr(endDate, "yyyy-MM-dd")
    var startDateVal = DateUtils.addMonth( DateUtils.intToDate(endDate), -maxIntervalVal )
    var startDate:Int = DateUtils.dateToInt(startDateVal)
    var startDateFormatStr  =  DateUtils.intToDateStr(startDate, "yyyy-MM-dd")

    val intervalDays  = DateUtils.intervalDays(startDateFormatStr,endDateFormatStr)

    val minIntervalValResult =  minIntervalVal

    val intervalMonths =  Math.abs(maxIntervalVal / minIntervalValResult)

    val hcfa = "hs08_client_for_ai"
    val dds = "dm_deliver_stat"



    spark.sql("use bigdata")

    val DmDeliverDF = spark.sql(
      s"""
         | select t.c_custno,t.client_id,t.branch_no,t.open_date,t.organ_flag,t.birthday,
         |	   t.l_date,t.f_businessamount,t.f_businessprice,t.f_businessbalance,t.c_businessflag,
         |	   t.c_moneytype,t.f_fare0
         | from (
         |		select c.c_custno,
         |			   c.client_id,
         |			   c.branch_no,
         |			   c.open_date,
         |			   c.organ_flag,
         |			   c.birthday,
         |			   he.oc_date l_date ,
         |			   he.business_amount f_businessamount,
         |			   he.business_price f_businessprice,
         |			   he.business_balance f_businessbalance,
         |			   he.business_flag c_businessflag,
         |			   he.money_type c_moneytype,
         |			   he.fare0 ,
         |			   (case when he.money_type = '0' then fare0
         |					 when he.money_type = '1' then fare0 * 6.875
         |					 when he.money_type = '2' then fare0 * 0.8858 end ) f_fare0
         |		from hs08_client_for_ai c
         |		left join hs08_his_deliver  he on c.c_custno = he.c_custno
         |		where he.oc_date >= ${startDate} and   he.oc_date < ${endDate}
         | ) t
       """.stripMargin.replace("\r\n"," "))

    DmDeliverDF.createOrReplaceTempView("DmDeliverTmp")

    val  clientCountDF = spark.sql("select c_custno from hs08_client_for_ai")
    val clientCount = clientCountDF.count().toInt

    spark.sql(
      s"""
         | create table if not exists dm_deliver_stat
         | (
         | c_custno string
         | ,branch_no string
         | ,open_date_dvalue double
         | ,age int
         | ,appro_months_amount double
         | ,remo_months_amount double
         | ,amount_tendency double
         | ,appro_months_count double
         | ,remo_months_count double
         | ,frequency_tendency double
         | ,f_fare0_approch double
         | ,f_fare0_remote double
         | ,f_fare0_tendency double
         | ,lastdate_dvalue double
         | ,input_date int
         | )  PARTITIONED BY (input_date int)
         | ROW FORMAT DELIMITED FIELDS TERMINATED BY  ${raw"'\t'"}
         | LINES TERMINATED BY ${raw"'\n'"}
         | stored as textfile
       """.stripMargin.replace("\r\n"," "))


    val dmDeliverStatDF = spark.sql(
      s"""
         | select
         |    c_custno
         |		,branch_no
         |		,open_date_dvalue
         |		,age
         |		,appro_months_amount
         |		,remo_months_amount
         |		,(case when remo_months_amount = 0 then 0 else appro_months_amount * (${intervalMonths} ) / remo_months_amount   end ) as amount_tendency
         |		,appro_months_count
         |		,remo_months_count
         |		,(case when remo_months_count = 0 then 0 else  appro_months_count * ( ${intervalMonths} ) /  remo_months_count end ) as frequency_tendency
         |		,f_fare0_approch
         |		,f_fare0_remote
         |		,(case when f_fare0_remote = 0 then  0 else f_fare0_approch * ( ${intervalMonths} ) / f_fare0_remote end ) as f_fare0_tendency
         |		,(case when he.l_date = 0 and ${maxIntervalVal} =3 then 100
         |			  when he.l_date = 0 and ${maxIntervalVal} =6 then 200
         |			  when he.l_date = 0 and ${maxIntervalVal} =9 then 400
         |			  else datediff('${endDateFormatStr}' , concat(substr(he.l_date,0,4),'-',substr(he.l_date,5,2),'-',substr(he.l_date,7,2))) end )   lastdate_dvalue
         |    ,${endDate} input_date
         | from (
         |		select c_custno
         |				,max(branch_no)  branch_no
         |        ,max(open_date_dvalue) open_date_dvalue
         |        ,max(age) age
         |				,max(l_date) l_date
         |				,sum(case when l_date_interval_months = ${minIntervalValResult} then f_businessbalance else 0  end ) appro_months_amount
         |				,sum(case when l_date_interval_months = ${maxIntervalVal} then  f_businessbalance else 0 end ) remo_months_amount
         |				,count(case when l_date_interval_months = ${minIntervalValResult} then c_custno else null end  ) appro_months_count
         |				,count(case when l_date_interval_months = ${maxIntervalVal} then  c_custno else null  end ) remo_months_count
         |				,sum(case when l_date_interval_months = ${minIntervalValResult} then f_fare0 else 0   end )  f_fare0_approch
         |				,sum(case when l_date_interval_months = ${maxIntervalVal} then f_fare0 else 0  end ) f_fare0_remote
         |		from (
         |				select  client_id
         |            ,c_custno
         |            ,branch_no
         |            ,organ_flag
         |						,l_date
         |						,c_businessflag
         |						,f_fare0
         |            ,f_businessbalance
         |						,ceil(months_between('${startDateFormatStr}','${endDateFormatStr}'))  l_date_interval_months
         |						,datediff('${endDateFormatStr}' , concat(substr(open_date,0,4),'-',substr(open_date,5,2),'-',substr(open_date,7,2)) )
         |            as open_date_dvalue
         |						,case when birthday = 0 then -1 else year('${endDateFormatStr}') - year(birthday)  end age
         |				from DmDeliverTmp
         |			)
         |  group by c_custno
         |) he
       """.stripMargin.replace("\r\n"," "))
    dmDeliverStatDF.createOrReplaceTempView("dmDeliverStatTmp")
    spark.sql("insert overwrite table  dm_deliver_stat select * from dmDeliverStatTmp")

    spark.sql(
      s"""
         | create table if not exists dm_deliver_cacl
         | (
         | branch_no string,
         |    appro_amount_avg double, remo_amount_avg double,
         |    amount_tend_avg double, appro_count_avg double,
         |    remo_count_avg double, frequency_tend_avg double,
         |    appro_fare0_avg double, remo_fare0_avg double,
         |    fare0_tend_avg double, open_d_dvalue_avg double,
         |    appro_amount_med double, remo_amount_med double,
         |    amount_tend_med double, appro_count_med double,
         |    remo_count_med double,frequency_tend_med double,
         |    appro_fare0_med double, remo_fare0_med double,
         |    fare0_tend_med double, open_d_dvalue_med double,
         |	  approavg_idlerate  double,appromed_idlerate  double,
         |    remoteavg_idlerate  double,remotemed_idlerate  double,
         |    avg_age double,med_age double,
         |	  last_dv_avg double,last_dv_med double
         |    ,input_date int
         | )  PARTITIONED BY (input_date int)
         | ROW FORMAT DELIMITED FIELDS TERMINATED BY  ${raw"'\t'"}
         | LINES TERMINATED BY ${raw"'\n'"}
         | stored as textfile
       """.stripMargin.replace("\r\n"," "))

    val dmDeliverCaclDF = spark.sql(
      s"""
         | select case when branch_no is null then  -1 else branch_no end branch_no
         |	  ,appro_amount_avg, remo_amount_avg
         |    ,amount_tend_avg, appro_count_avg
         |    ,remo_count_avg, frequency_tend_avg
         |    ,appro_fare0_avg, remo_fare0_avg
         |    ,fare0_tend_avg, open_d_dvalue_avg
         |    ,appro_amount_med, remo_amount_med
         |    ,amount_tend_med, appro_count_med
         |    ,remo_count_med,frequency_tend_med
         |    ,appro_fare0_med, remo_fare0_med
         |    ,fare0_tend_med, open_d_dvalue_med
         |	  ,approavg_idlerate ,appromed_idlerate
         |    ,remoteavg_idlerate ,remotemed_idlerate
         |    ,avg_age,med_age
         |	  ,last_dv_avg,last_dv_med
         |    ,${endDate} input_date
         | from (
         |	 select
         |		  a.branch_no
         |		 ,round(avg(a.appro_months_amount) ,2)as appro_amount_avg
         |		 ,round(avg(a.remo_months_amount),2) as remo_amount_avg
         |		 ,round(avg(a.amount_tendency),2) amount_tend_avg
         |		 ,round(avg(a.appro_months_count),2) appro_count_avg
         |		 ,round(avg(a.remo_months_count),2) remo_count_avg
         |		 ,round(avg(a.frequency_tendency),2) frequency_tend_avg
         |		 ,round(avg(a.f_fare0_approch),2) appro_fare0_avg
         |		 ,round(avg(a.f_fare0_remote),2) remo_fare0_avg
         |		 ,round(avg(a.f_fare0_tendency),2) fare0_tend_avg
         |		 ,round(avg(a.open_date_dvalue),2)open_d_dvalue_avg
         |		 ,round(percentile_approx(a.appro_months_amount,0.5),2) as appro_amount_med
         |		 ,round(percentile_approx(a.remo_months_amount,0.5),2) as remo_amount_med
         |		 ,round(percentile_approx(a.amount_tendency,0.5),2)amount_tend_med
         |		 ,round(percentile_approx(a.appro_months_count,0.5),2) appro_count_med
         |		 ,round(percentile_approx(a.remo_months_count,0.5),2) remo_count_med
         |		 ,round(percentile_approx(a.frequency_tendency,0.5),2) frequency_tend_med
         |		 ,round(percentile_approx(a.f_fare0_approch,0.5),2) appro_fare0_med
         |		 ,round(percentile_approx(a.f_fare0_remote,0.5),2) remo_fare0_med
         |		 ,round(percentile_approx(a.f_fare0_tendency,0.5),2) fare0_tend_med
         |		 ,round(percentile_approx(a.open_date_dvalue,0.5),2) open_d_dvalue_med
         |		 ,round(avg(b.approch_idle_rate),2) approavg_idlerate
         |		 ,round(percentile_approx(b.approch_idle_rate,0.5),2) appromed_idlerate
         |		 ,round(avg(b.remote_idle_rate),2) remoteavg_idlerate
         |		 ,round(percentile_approx(b.remote_idle_rate,0.5),2) remotemed_idlerate
         |		 ,round(avg(a.age),2) avg_age
         |		 ,round(percentile_approx(a.age,0.5),2) med_age
         |		 ,round(avg(a.lastdate_dvalue),2) last_dv_avg
         |		 ,round(percentile_approx(a.lastdate_dvalue,0.5),2) last_dv_med
         |	from  dm_deliver_stat a
         |  left join dm_custtotalasset_dm_stat b on a.c_custno = b.c_custno
         |  group by a.branch_no,1 grouping sets(a.branch_no,1)
         |	)
       """.stripMargin.replace("\r\n"," "))
    dmDeliverCaclDF.createOrReplaceTempView("dmDeliverCaclTmp")

    spark.sql("insert  overwrite table   dm_deliver_cacl  select * from dmDeliverCaclTmp ")

    spark.sql(
      s"""
         | create table if not exists bigdata.dm_deliver_rank
         | (
         |    c_custno string,
         |		branch_no string,
         |		trade_b_amount_rank double,
         |		trade_b_frequency_rank double,
         |		last_b_trade_time_rank double,
         |		fare0_b_tend_rank double,
         |		fare0_b_rank double,
         |		open_data_b_rank double,
         |		peakasset_b_rank double,
         |		trade_all_amount_rank double,
         |		trade_all_frequency_rank double,
         |		last_all_trade_time_rank double,
         |		fare0_all_tend_rank double,
         |		fare0_all_rank double,
         |		open_date_all_rank double,
         |		peakasset_all_rank  double,
         |    input_date int
         | )  PARTITIONED BY (input_date int)
         | ROW FORMAT DELIMITED FIELDS TERMINATED BY  ${raw"'\t'"}
         | LINES TERMINATED BY ${raw"'\n'"}
         | stored as textfile
       """.stripMargin.replace("\r\n"," "))

    val dmDeliverRankDF = spark.sql(
      s"""
         | select
         |		c_custno,
         |		branch_no,
         |		trade_b_amount_rank,
         |		trade_b_frequency_rank,
         |		last_b_trade_time_rank,
         |		fare0_b_tend_rank,
         |		fare0_b_rank,
         |		open_data_b_rank,
         |		peakasset_b_rank,
         |		trade_all_amount_rank,
         |		trade_all_frequency_rank,
         |		last_all_trade_time_rank,
         |		fare0_all_tend_rank,
         |		fare0_all_rank,
         |		open_date_all_rank,
         |		peakasset_all_rank,
         |    ${endDate} input_date
         | from (
         |		select
         |			c_custno
         |			,branch_no
         |			,(case when amount_tendency = 0  then -1 else round((v_branch_count - trade_b_amount_rk ) * 100 / v_branch_count, 2) end ) trade_b_amount_rank
         |			,(case when frequency_tendency = 0  then -1 else round((v_branch_count - trade_b_frequency_rk ) * 100 / v_branch_count, 2) end ) trade_b_frequency_rank
         |			,(case when l_date = 0 and lastdate_dvalue = 100 then  -1
         |             when l_date = 0 and lastdate_dvalue  = 200 then  -1
         |             when l_date = 0 and lastdate_dvalue  = 400 then  -1
         |             else round((v_branch_count - last_b_trade_time_rk)  * 100 / v_branch_count ,2) end ) last_b_trade_time_rank
         |			,(case when f_fare0_tendency = 0  then -1 else round((v_branch_count - fare0_b_tend_rk ) * 100 / v_branch_count, 2) end ) fare0_b_tend_rank
         |			,(case when f_fare0_approch = 0 then -1   else round((v_branch_count - fare0_b_rk) * 100 / v_branch_count, 2) end ) fare0_b_rank
         |			,round((v_branch_count - open_data_b_rk ) * 100 / v_branch_count , 2) open_data_b_rank
         |			,(case when peak_vasset = 0 then  -1 else round((v_branch_count - peakasset_b_rk ) * 100 / v_branch_count, 2) end ) peakasset_b_rank
         |			,(case when amount_tendency = 0 then -1   else round(( ${clientCount} - trade_all_amount_rk ) * 100 / ${clientCount} , 2 ) end ) trade_all_amount_rank
         |			,(case when frequency_tendency = 0 then -1  else round(( ${clientCount}  - trade_all_frequency_rk ) * 100 / ${clientCount}  , 2 ) end )  trade_all_frequency_rank
         |			,(case when l_date = 0 and lastdate_dvalue = 100 then  -1
         |			       when l_date = 0 and lastdate_dvalue  = 200 then  -1
         |			       when l_date = 0 and lastdate_dvalue  = 400 then  -1
         |			      else round((${clientCount} - last_all_trade_time_rk)  * 100 / ${clientCount} ,2) end ) last_all_trade_time_rank
         |			,(case when f_fare0_tendency = 0  then -1 else round(( ${clientCount} -fare0_all_tend_rk ) * 100 / ${clientCount} , 2) end ) fare0_all_tend_rank
         |			,(case when f_fare0_approch = 0 then   -1 else round((${clientCount} - fare0_all_rk ) * 100 / ${clientCount}, 2) end ) fare0_all_rank
         |			,round((${clientCount} - open_date_all_rk ) * 100 / ${clientCount} , 2)   open_date_all_rank
         |			,(case when peak_vasset = 0  then -1 else round((${clientCount} - peakasset_all_rk) * 100 / ${clientCount} , 2) end ) peakasset_all_rank
         |		from (
         |			select
         |          a.c_custno
         |					,a.branch_no
         |          ,a.l_date
         |          ,a.lastdate_dvalue
         |          ,a.f_fare0_tendency
         |          ,a.f_fare0_approch
         |          ,b.peak_vasset
         |          ,a.amount_tendency
         |          ,a.frequency_tendency
         |
         |					,dense_rank() over (partition by a.branch_no order by a.amount_tendency desc ) trade_b_amount_rk
         |					,dense_rank() over (partition by a.branch_no order by a.frequency_tendency desc ) trade_b_frequency_rk
         |					,dense_rank() over (partition by a.branch_no order by a.lastdate_dvalue asc ) last_b_trade_time_rk
         |					,dense_rank() over (partition by a.branch_no order by a.f_fare0_tendency desc) fare0_b_tend_rk
         |					,dense_rank() over (partition by a.branch_no order by a.f_fare0_approch  desc ) fare0_b_rk
         |					,dense_rank() over (partition by a.branch_no order by a.open_date_dvalue desc ) open_data_b_rk
         |					,dense_rank() over (partition by a.branch_no order by b.peak_vasset desc ) peakasset_b_rk
         |					,dense_rank() over (order by a.amount_tendency desc)    trade_all_amount_rk
         |					,dense_rank() over (order by a.frequency_tendency desc) trade_all_frequency_rk
         |					,dense_rank() over (order by a.lastdate_dvalue asc ) last_all_trade_time_rk
         |					,dense_rank() over (order by a.f_fare0_tendency desc) fare0_all_tend_rk
         |					,dense_rank() over (order by a.f_fare0_approch desc ) fare0_all_rk
         |					,dense_rank() over (order by a.open_date_dvalue desc ) open_date_all_rk
         |					,dense_rank() over (order by b.peak_vasset desc )  peakasset_all_rk
         |          ,c.client_cnt as v_branch_count
         |		 from dmDeliverStatTmp a
         |     left join dm_custtotalasset_dm_stat b on a.c_custno = b.c_custno
         |     left join dm_branch_client_cnt c on a.branch_no = c.branch_no
         |			)
         |	)
       """.stripMargin.replace("\r\n"," "))
    dmDeliverRankDF.createOrReplaceTempView("dmDeliverRankTmp")
    spark.sql(s"insert  overwrite table   dm_deliver_rank  PARTITION( input_date=${endDate} ) select * from dmDeliverRankTmp ")

    spark.stop()

  }

}
