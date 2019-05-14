package com.data.process

import com.data.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DmBankTransferProc {
  def main(args: Array[String]): Unit = {
    new DmBankTransferProc().bankTransferProc(20190401,3)

  }

}

class DmBankTransferProc {

  private val conf = new SparkConf()
    .setAppName("DmBankTransferProc")
//    .setMaster("yarn-client")

  private val spark = SparkSession
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
  def bankTransferProc(endDate:Int, maxIntervalVal:Int): Unit =
  {
    val minIntervalValInt = 1
    bankTransferProc( endDate, maxIntervalVal, minIntervalValInt)
  }

  /**
    *
    * @param endDate
    * @param maxIntervalVal
    * @param minIntervalVal
    */
  def bankTransferProc( endDate:Int, maxIntervalVal:Int, minIntervalVal:Int): Unit =
  {
    var endDateFormatStr  =  DateUtils.intToDateStr(endDate, "yyyy-MM-dd")
    var startDateVal = DateUtils.addMonth( DateUtils.intToDate(endDate), -maxIntervalVal )
    var startDate:Int = DateUtils.dateToInt(startDateVal)
    var startDateFormatStr  =  DateUtils.intToDateStr(startDate, "yyyy-MM-dd")

    val intervalDays  = DateUtils.intervalDays(startDateFormatStr,endDateFormatStr)

    val minIntervalValResult =  minIntervalVal

    val intervalMonths =  Math.abs(maxIntervalVal / minIntervalValResult)

    spark.sql("use bigdata")

    val dmBanktransferDF = spark.sql(
      s"""
         | select
         | t.c_custno
         | ,t.client_id
         | ,t.branch_no
         | ,t.open_date
         | ,t.organ_flag
         | ,t.birthday
         | ,t.occur_balance
         | ,trans_type
         | ,curr_date
         | from ( select
         |         c.c_custno,
         |			   c.client_id,
         |			   c.branch_no,
         |			   c.open_date,
         |			   c.organ_flag,
         |			   c.birthday,
         |			   b.occur_balance,
         |			   b.trans_type,
         |			   b.curr_date
         |		from hs08_client_for_ai c
         |		join (select client_id,concat('c',client_id) c_custno, curr_date, bktrans_status, trans_type, nvl(occur_balance, '0') occur_balance
         |						  from bigdata.hs08_his_banktransfer
         |						  where bktrans_status = '2' and trans_type in ('01', '02')
         |					) b on  a.c_custno = b.c_custno
         |		where b.curr_date >= ${startDate} and curr_date <  ${endDate}
         |	) t
       """.stripMargin.replace("\r\n"," "))
    dmBanktransferDF.createOrReplaceTempView("dmBanktransferTmp")

    val  clientCountDF = spark.sql("select c_custno from dmBanktransferTmp group by c_custno")
    val clientCount = clientCountDF.count().toInt

    val branchClientCntDF = spark.sql(
      s"""
         | select
         | case when branch_no is null or branch_no='' then -1 else branch_no end branch_no
         | ,branch_client_cnt
         | from (
         |      select   branch_no, count(*) branch_client_cnt
         |      from  dmBanktransferTmp
         |      group by branch_no
         |     )
       """.stripMargin.replace("\r\n"," "))
    branchClientCntDF.createOrReplaceTempView("branchClientCntTmp")

    // alter table partition_test add partition (dt=20190401,province='henan');
    spark.sql("drop table if exists bigdata.dm_banktransfer_stat")

    spark.sql(
      s"""
         | create table if not exists bigdata.dm_banktransfer_stat
         | (
         | c_custno string,
         | branch_no string,
         | client_id int,
         | appro_in_frequency double,
         | appro_out_frequency double,
         | appro_frequency_dvalue double,
         | appro_in_sum double,
         | appro_out_sum double,
         | appro_sum_dvalue double,
         | remote_in_frequency double,
         | remot_out_frequency double,
         | remot_frequency_dvalue double,
         | remot_in_sum double,
         | remot_out_sum double,
         | remot_sum_dvalue double,
         | in_frequency_tendency double,
         | out_frequency_tendency double,
         | in_sum_tendency double,
         | out_sum_tendency double,
         | frequency_dvalue_tendency double,
         | sum_dvalue_tendency double,
         | input_date int
         | )
         | PARTITIONED BY (dt int)
         | ROW FORMAT DELIMITED FIELDS TERMINATED BY  ${raw"'\t'"}
         | LINES TERMINATED BY ${raw"'\n'"}
         | stored as textfile
       """.stripMargin.replace("\r\n"," "))

    val dmBanktransferStatDF = spark.sql(
      s"""
         | select c_custno,branch_no,client_id,
         |		appro_in_frequency,
         |		appro_out_frequency,
         |		appro_frequency_dvalue,
         |		appro_in_sum,
         |		appro_out_sum,
         |		appro_sum_dvalue,
         |		remote_in_frequency,
         |		remot_out_frequency,
         |		remot_frequency_dvalue,
         |		remot_in_sum,
         |		remot_out_sum,
         |		remot_sum_dvalue,
         |		case when remote_in_frequency =0 then 0 else round( appro_in_frequency * ( ${intervalMonths} ) / remote_in_frequency ,2) end 	in_frequency_tendency,
         |		case when remot_out_frequency =0 then 0 else round( appro_out_frequency * ( ${intervalMonths} ) / remot_out_frequency ,2)  end out_frequency_tendency,
         |		case when remot_in_sum =0 then 0 else round( appro_in_sum * (${intervalMonths} ) / remot_in_sum , 2)  end in_sum_tendency,
         |		case when remot_out_sum =0 then 0 else round( appro_out_sum * (${intervalMonths} ) / remot_out_sum , 2) end out_sum_tendency,
         |		case when remot_frequency_dvalue =0 then 0 else round( appro_frequency_dvalue * (${intervalMonths}) / remot_frequency_dvalue ,2 )  end frequency_dvalue_tendency,
         |		case when remot_sum_dvalue =0 then 0 else round( appro_sum_dvalue * (${intervalMonths} ) / remot_sum_dvalue , 2) end sum_dvalue_tendency,
         |    ${endDate} input_date
         | from (
         |	select c_custno,branch_no,client_id,
         |			appro_in_frequency,
         |			appro_out_frequency,
         |			appro_in_sum,
         |			appro_out_sum,
         |			remote_in_frequency,
         |			remot_out_frequency,
         |			remot_in_sum,
         |			remot_out_sum,
         |			(appro_in_frequency - appro_out_frequency) appro_frequency_dvalue,
         |			(remote_in_frequency - remot_out_frequency) remot_frequency_dvalue,
         |			(remot_in_sum - remot_out_sum) remot_sum_dvalue,
         |			(appro_in_sum - appro_out_sum) appro_sum_dvalue
         |	from (
         |			select c_custno,branch_no,client_id,
         |					count(case when data_months <= ${minIntervalValResult} and trans_type = '01' then client_id else null end ) appro_in_frequency,
         |					count(case when data_months <= ${minIntervalValResult}  and trans_type = '02' then client_id else null end ) appro_out_frequency,
         |					sum(case when data_months <= ${minIntervalValResult}  and trans_type = '01' then occur_balance else 0 end )  appro_in_sum,
         |					sum(case when data_months <= ${minIntervalValResult}  and trans_type = '02' then occur_balance else 0 end ) appro_out_sum,
         |					count(case when data_months <= ${maxIntervalVal} and trans_type = '01' then client_id else null end ) remote_in_frequency,
         |					count(case when data_months <= ${maxIntervalVal} and trans_type = '02' then client_id else null end ) remot_out_frequency,
         |					sum(case when data_months <= ${maxIntervalVal} and trans_type = '01' then occur_balance else 0 end ) remot_in_sum,
         |					sum(case when data_months <= ${maxIntervalVal} and trans_type = '02' then occur_balance else 0 end ) remot_out_sum
         |			from (
         |				   select  c_custno,
         |						       client_id,
         |						       branch_no,
         |						       open_date,
         |						       organ_flag,
         |						       birthday,
         |						       occur_balance,
         |						       trans_type,
         |						       ceil(months_between('${endDateFormatStr}', concat(substr(curr_date,0,4),'-',substr(curr_date,5,2),'-',substr(curr_date,7,2)) )) data_months
         |				     from dmBanktransferTmp
         |			    ) t
         |      group by  c_custno,branch_no,client_id
         |		)
         |	)
       """.stripMargin.replace("\r\n"," "))

    dmBanktransferStatDF.createOrReplaceTempView("dmBanktransferStatTmp")
    spark.sql(
      s"""
         | insert  overwrite table    dm_banktransfer_stat partition(dt=${endDate})
         | select c_custno,branch_no,client_id,
         |		appro_in_frequency,
         |		appro_out_frequency,
         |		appro_frequency_dvalue,
         |		appro_in_sum,
         |		appro_out_sum,
         |		appro_sum_dvalue,
         |		remote_in_frequency,
         |		remot_out_frequency,
         |		remot_frequency_dvalue,
         |		remot_in_sum,
         |		remot_out_sum,
         |		remot_sum_dvalue,
         |    in_frequency_tendency ,
         |    out_frequency_tendency ,
         |    in_sum_tendency ,
         |    out_sum_tendency ,
         |    frequency_dvalue_tendency ,
         |    sum_dvalue_tendency ,
         |    input_date
         |  from dmBanktransferStatTmp
       """.stripMargin.replace("\r\n"," "))

    spark.sql("drop table if exists bigdata.dm_banktransfer_cacl")
    spark.sql(
      s"""
         | create table if not exists dm_banktransfer_cacl
         | (   branch_no string
         |    ,appro_avginsum double,appro_avgoutsum double,appro_avginfrequ double,appro_avgoutfrequ double
         |		,appro_avgsumdvalue double,appro_avgfrequdvalue double,avginsumtrendency double,avgoutsumtrendency double
         |		,avginfrequtendency double,avgoutfrequtendency double,avgsumdvaluetendency double,avgfrequdvaluetendency double
         |		,appro_medinsum double,appro_medoutsum double,appro_medinfrequ double,appro_medoutfrequ double,appro_medsumdvalue double
         |		,appro_medfrequdvalue double,medinsumtrendency double,medoutsumtrendency double,medinfrequtendency double,medoutfrequtendency double
         |		,medsumdvaluetendency double,medfrequdvaluetendency double,remo_avginsum double,remo_avgoutsum double,remo_avginfrequ double,remo_avgoutfrequ double
         |		,remo_avgsumdvalue double,remo_avgfrequdvalue double,remo_medinsum double,remo_medoutsum double,remo_medinfrequ double,remo_medoutfrequ double,remo_medsumdvalue double
         |		,remo_medfrequdvalue double, input_date int
         | )
         |  PARTITIONED BY (dt int)
         |  ROW FORMAT DELIMITED FIELDS TERMINATED BY  ${raw"'\t'"}
         | LINES TERMINATED BY ${raw"'\n'"}
         | stored as textfile
       """.stripMargin.replace("\r\n"," "))

    val dmBanktransferCaclDF = spark.sql(
      s"""
         | select  case when branch_no is null then  -1 else branch_no end branch_no
         |		,appro_avginsum,appro_avgoutsum,appro_avginfrequ,appro_avgoutfrequ
         |		,appro_avgsumdvalue,appro_avgfrequdvalue,avginsumtrendency,avgoutsumtrendency
         |		,avginfrequtendency,avgoutfrequtendency,avgsumdvaluetendency,avgfrequdvaluetendency
         |		,appro_medinsum,appro_medoutsum,appro_medinfrequ,appro_medoutfrequ,appro_medsumdvalue
         |		,appro_medfrequdvalue,medinsumtrendency,medoutsumtrendency,medinfrequtendency,medoutfrequtendency
         |		,medsumdvaluetendency,medfrequdvaluetendency,remo_avginsum,remo_avgoutsum,remo_avginfrequ,remo_avgoutfrequ
         |		,remo_avgsumdvalue,remo_avgfrequdvalue,remo_medinsum,remo_medoutsum,remo_medinfrequ,remo_medoutfrequ,remo_medsumdvalue
         |		,remo_medfrequdvalue, ${endDate} input_date
         | from (
         |	select
         |		branch_no
         |		,round(avg(appro_in_sum),2)		APPRO_AVGINSUM
         |		,round(avg(appro_out_sum),2)		APPRO_AVGOUTSUM
         |		,round(avg(appro_in_frequency),2)		APPRO_AVGINFREQU
         |		,round(avg(appro_out_frequency),2)		APPRO_AVGOUTFREQU
         |		,round(avg(appro_sum_dvalue),2)		APPRO_AVGSUMDVALUE
         |		,round(avg(appro_frequency_dvalue),2)		APPRO_AVGFREQUDVALUE
         |		,round(avg(in_sum_tendency),2)		AVGINSUMTRENDENCY
         |		,round(avg(out_sum_tendency),2)		AVGOUTSUMTRENDENCY
         |		,round(avg(in_frequency_tendency),2)		AVGINFREQUTENDENCY
         |		,round(avg(out_frequency_tendency),2)		AVGOUTFREQUTENDENCY
         |		,round(avg(sum_dvalue_tendency),2)		 AVGSUMDVALUETENDENCY
         |		,round(avg(frequency_dvalue_tendency),2)		AVGFREQUDVALUETENDENCY
         |		,round(percentile_approx(appro_in_sum,0.5),2)		APPRO_MEDINSUM
         |		,round(percentile_approx(appro_out_sum,0.5),2)		APPRO_MEDOUTSUM
         |		,round(percentile_approx(appro_in_frequency,0.5),2)		APPRO_MEDINFREQU
         |		,round(percentile_approx(appro_out_frequency,0.5),2)		APPRO_MEDOUTFREQU
         |		,round(percentile_approx(appro_sum_dvalue,0.5),2)		APPRO_MEDSUMDVALUE
         |		,round(percentile_approx(appro_frequency_dvalue,0.5),2)		APPRO_MEDFREQUDVALUE
         |		,round(percentile_approx(in_sum_tendency,0.5),2)		MEDINSUMTRENDENCY
         |		,round(percentile_approx(out_sum_tendency,0.5),2)		MEDOUTSUMTRENDENCY
         |		,round(percentile_approx(in_frequency_tendency,0.5),2)		MEDINFREQUTENDENCY
         |		,round(percentile_approx(out_frequency_tendency,0.5),2)		MEDOUTFREQUTENDENCY
         |		,round(percentile_approx(sum_dvalue_tendency,0.5),2)		MEDSUMDVALUETENDENCY
         |		,round(percentile_approx(frequency_dvalue_tendency,0.5),2)		MEDFREQUDVALUETENDENCY
         |		,round(avg(remot_in_sum),2)		REMO_AVGINSUM
         |		,round(avg(remot_out_sum),2)		REMO_AVGOUTSUM
         |		,round(avg(remote_in_frequency),2)		REMO_AVGINFREQU
         |		,round(avg(remot_out_frequency),2)	REMO_AVGOUTFREQU
         |		,round(avg(remot_sum_dvalue),2)		REMO_AVGSUMDVALUE
         |		,round(avg(remot_frequency_dvalue),2)		REMO_AVGFREQUDVALUE
         |		,round(percentile_approx(remot_in_sum,0.5),2)		REMO_MEDINSUM
         |		,round(percentile_approx(remot_out_sum,0.5),2)	REMO_MEDOUTSUM
         |		,round(percentile_approx(remote_in_frequency,0.5),2)		REMO_MEDINFREQU
         |		,round(percentile_approx(remot_out_frequency,0.5),2)		REMO_MEDOUTFREQU
         |		,round(percentile_approx(remot_sum_dvalue,0.5),2)		REMO_MEDSUMDVALUE
         |		,round(percentile_approx(remot_frequency_dvalue,0.5),2)		REMO_MEDFREQUDVALUE
         |	from dmBanktransferStatTmp
         |	group  by branch_no,1 grouping sets(branch_no,1)
         |   )
       """.stripMargin.replace("\r\n"," "))

    dmBanktransferCaclDF.createOrReplaceTempView("dmBanktransferCaclTmp")
    spark.sql(
      s"""
         | insert  overwrite table   dm_banktransfer_cacl  partition(dt=${endDate})
         | select  branch_no
         |		,appro_avginsum,appro_avgoutsum,appro_avginfrequ,appro_avgoutfrequ
         |		,appro_avgsumdvalue,appro_avgfrequdvalue,avginsumtrendency,avgoutsumtrendency
         |		,avginfrequtendency,avgoutfrequtendency,avgsumdvaluetendency,avgfrequdvaluetendency
         |		,appro_medinsum,appro_medoutsum,appro_medinfrequ,appro_medoutfrequ,appro_medsumdvalue
         |		,appro_medfrequdvalue,medinsumtrendency,medoutsumtrendency,medinfrequtendency,medoutfrequtendency
         |		,medsumdvaluetendency,medfrequdvaluetendency,remo_avginsum,remo_avgoutsum,remo_avginfrequ,remo_avgoutfrequ
         |		,remo_avgsumdvalue,remo_avgfrequdvalue,remo_medinsum,remo_medoutsum,remo_medinfrequ,remo_medoutfrequ,remo_medsumdvalue
         |		,remo_medfrequdvalue, input_date
         |  from dmBanktransferCaclTmp
       """.stripMargin.replace("\r\n"," "))


    spark.sql("drop table if exists bigdata.dm_banktransfer_rank")
    spark.sql(
      s"""
         | create table if not exists bigdata.dm_banktransfer_rank
         | (
         |    c_custno string
         |    ,branch_no string
         |    ,supercom_apprinsum double
         |    ,supercom_approutsum double
         |    ,supercom_apprinfre double
         |    ,supercom_approutfre double
         |    ,supercom_apprsumdva double
         |    ,supercom_apprfredva double
         |    ,supercom_insumten double
         |    ,supercom_outsumten double
         |    ,supercom_infreten double
         |    ,supercom_outfreten double
         |    ,supercom_sumdvaten double
         |    ,supercom_fredvaten double
         |    ,superbra_apprinsum double
         |    ,superbra_approutsum double
         |    ,superbra_apprinfre double
         |    ,superbra_approutfre double
         |    ,superbra_apprsumdva double
         |    ,superbra_apprfredva double
         |    ,superbra_insumten double
         |    ,superbra_outsumten double
         |    ,supercbra_infreten double
         |    ,superbra_outfreten double
         |    ,superbra_sumdvaten double
         |    ,superbra_fredvaten double
         |    ,input_date int
         | )
         | PARTITIONED BY (dt int)
         | ROW FORMAT DELIMITED FIELDS TERMINATED BY  ${raw"'\t'"}
         | LINES TERMINATED BY ${raw"'\n'"}
         | stored as textfile
       """.stripMargin.replace("\r\n"," "))

    val dmDeliverRankDF = spark.sql(
      s"""
         | select
         |	  c_custno
         |    ,branch_no
         |    ,supercom_apprinsum
         |    ,supercom_approutsum
         |    ,supercom_apprinfre
         |    ,supercom_approutfre
         |    ,supercom_apprsumdva
         |    ,supercom_apprfredva
         |    ,supercom_insumten
         |    ,supercom_outsumten
         |    ,supercom_infreten
         |    ,supercom_outfreten
         |    ,supercom_sumdvaten
         |    ,supercom_fredvaten
         |    ,superbra_apprinsum
         |    ,superbra_approutsum
         |    ,superbra_apprinfre
         |    ,superbra_approutfre
         |    ,superbra_apprsumdva
         |    ,superbra_apprfredva
         |    ,superbra_insumten
         |    ,superbra_outsumten
         |    ,supercbra_infreten
         |    ,superbra_outfreten
         |    ,superbra_sumdvaten
         |    ,superbra_fredvaten
         |    ,${endDate} input_date
         | from (
         |	select
         |		 c_custno
         |		 ,a.branch_no
         |		 ,case when appro_in_sum=0 then 0 else round((${clientCount} -nvl(a.supercom_apprinsum,${clientCount}) ) * 100 /${clientCount} ,2) end		supercom_apprinsum
         |		 ,case when appro_out_sum=0 then 0 else round((${clientCount} -nvl(a.supercom_approutsum,${clientCount}) ) * 100 /${clientCount} ,2) end		supercom_approutsum
         |		 ,case when appro_in_frequency=0 then 0 else round((${clientCount} -nvl(a.supercom_apprinfre,${clientCount}) ) * 100 /${clientCount} ,2) end		supercom_apprinfre
         |		 ,case when appro_out_frequency=0 then 0 else round((${clientCount} -nvl(a.supercom_approutfre,${clientCount}) ) * 100 /${clientCount} ,2) end		supercom_approutfre
         |		 ,round((${clientCount} -nvl(a.supercom_apprsumdva,${clientCount}) ) * 100 /${clientCount} ,2)		supercom_apprsumdva
         |		 ,case when appro_frequency_dvalue<=0 then 0 else round((${clientCount} -nvl(a.supercom_apprfredva,${clientCount}) ) * 100 /${clientCount} ,2) end		supercom_apprfredva
         |		 ,round((${clientCount} -nvl(a.supercom_insumten,${clientCount}) ) * 100 /${clientCount} ,2)		supercom_insumten
         |		 ,round((${clientCount} -nvl(a.supercom_outsumten,${clientCount}) ) * 100 /${clientCount} ,2)		supercom_outsumten
         |		 ,round((${clientCount} -nvl(a.supercom_infreten,${clientCount}) ) * 100 /${clientCount} ,2)		supercom_infreten
         |		 ,round((${clientCount} -nvl(a.supercom_outfreten,${clientCount}) ) * 100 /${clientCount} ,2)		supercom_outfreten
         |		 ,round((${clientCount} -nvl(a.supercom_sumdvaten,${clientCount}) ) * 100 /${clientCount} ,2)		supercom_sumdvaten
         |		 ,round((${clientCount} -nvl(a.supercom_fredvaten,${clientCount}) ) * 100 /${clientCount} ,2)		supercom_fredvaten
         |		 ,case when appro_in_sum=0 then 0 else  round((v_branch_count -nvl(a.superbra_apprinsum,v_branch_count) ) * 100 /v_branch_count ,2) end		superbra_apprinsum
         |		 ,case when appro_out_sum=0 then 0 else round((v_branch_count -nvl(a.superbra_approutsum,v_branch_count) ) * 100 /v_branch_count ,2) end		superbra_approutsum
         |		 ,case when appro_in_frequency=0 then 0 else round((v_branch_count -nvl(a.superbra_apprinfre,v_branch_count) ) * 100 /v_branch_count ,2) end		superbra_apprinfre
         |		 ,case when appro_out_frequency=0 then 0 else round((v_branch_count -nvl(a.superbra_approutfre,v_branch_count) ) * 100 /v_branch_count ,2) end		superbra_approutfre
         |		 ,round((v_branch_count -nvl(a.superbra_apprsumdva,v_branch_count) ) * 100 /v_branch_count ,2)		superbra_apprsumdva
         |		 ,case when appro_frequency_dvalue<=0 then 0 else round((v_branch_count -nvl(a.superbra_apprfredva,v_branch_count) ) * 100 /v_branch_count ,2) end		superbra_apprfredva
         |		 ,round((v_branch_count -nvl(a.superbra_insumten,v_branch_count) ) * 100 /v_branch_count ,2)		superbra_insumten
         |		 ,round((v_branch_count -nvl(a.superbra_outsumten,v_branch_count) ) * 100 /v_branch_count ,2)		superbra_outsumten
         |		 ,round((v_branch_count -nvl(a.supercbra_infreten,v_branch_count) ) * 100 /v_branch_count ,2)		supercbra_infreten
         |		 ,round((v_branch_count -nvl(a.superbra_outfreten,v_branch_count) ) * 100 /v_branch_count ,2)		superbra_outfreten
         |		 ,round((v_branch_count -nvl(a.superbra_sumdvaten,v_branch_count) ) * 100 /v_branch_count ,2)		superbra_sumdvaten
         |		 ,round((v_branch_count -nvl(a.superbra_fredvaten,v_branch_count) ) * 100 /v_branch_count ,2)		superbra_fredvaten
         |	 from (
         |		  select
         |			  bs.c_custno
         |			 ,bs.branch_no
         |			 ,bs.appro_in_sum
         |			 ,bs.appro_out_sum
         |			 ,bs.appro_in_frequency
         |			 ,bs.appro_out_frequency
         |			 ,bs.appro_sum_dvalue
         |			 ,bs.appro_frequency_dvalue
         |			 ,dense_rank() over(order by bs.appro_in_sum  desc)              supercom_apprinsum
         |			 ,dense_rank() over(order by bs.appro_out_sum  desc)             supercom_approutsum
         |			 ,dense_rank() over(order by bs.appro_in_frequency  desc)        supercom_apprinfre
         |			 ,dense_rank() over(order by bs.appro_out_frequency  desc)       supercom_approutfre
         |			 ,dense_rank() over(order by bs.appro_sum_dvalue  desc)          supercom_apprsumdva
         |			 ,dense_rank() over(order by bs.appro_frequency_dvalue  desc)    supercom_apprfredva
         |			 ,dense_rank() over(order by bs.in_sum_tendency  desc)           supercom_insumten
         |			 ,dense_rank() over(order by bs.out_sum_tendency  desc)          supercom_outsumten
         |			 ,dense_rank() over(order by bs.in_frequency_tendency  desc)     supercom_infreten
         |			 ,dense_rank() over(order by bs.out_frequency_tendency  desc)    supercom_outfreten
         |			 ,dense_rank() over(order by bs.sum_dvalue_tendency  desc)       supercom_sumdvaten
         |			 ,dense_rank() over(order by bs.frequency_dvalue_tendency  desc) supercom_fredvaten
         |			 ,dense_rank() over(partition by bs.branch_no order by bs.appro_in_sum  desc)              superbra_apprinsum
         |			 ,dense_rank() over(partition by bs.branch_no  order by bs.appro_out_sum  desc)             superbra_approutsum
         |			 ,dense_rank() over(partition by bs.branch_no  order by bs.appro_in_frequency  desc)        superbra_apprinfre
         |			 ,dense_rank() over(partition by bs.branch_no  order by bs.appro_out_frequency  desc)       superbra_approutfre
         |			 ,dense_rank() over(partition by bs.branch_no  order by bs.appro_sum_dvalue  desc)          superbra_apprsumdva
         |			 ,dense_rank() over(partition by bs.branch_no  order by bs.appro_frequency_dvalue  desc)    superbra_apprfredva
         |			 ,dense_rank() over(partition by bs.branch_no  order by bs.in_sum_tendency  desc)           superbra_insumten
         |			 ,dense_rank() over(partition by bs.branch_no  order by bs.out_sum_tendency  desc)          superbra_outsumten
         |			 ,dense_rank() over(partition by bs.branch_no  order by bs.in_frequency_tendency  desc)     supercbra_infreten
         |			 ,dense_rank() over(partition by bs.branch_no  order by bs.out_frequency_tendency  desc)    superbra_outfreten
         |			 ,dense_rank() over(partition by bs.branch_no  order by bs.sum_dvalue_tendency  desc)       superbra_sumdvaten
         |			 ,dense_rank() over(partition by bs.branch_no  order by bs.frequency_dvalue_tendency  desc) superbra_fredvaten
         |       ,b.branch_client_cnt as v_branch_count
         |		 from dmBanktransferStatTmp bs
         |     left join branchClientCntTmp b on bs.branch_no = b.branch_no
         |    ) a
         |	)
       """.stripMargin.replace("\r\n"," "))

    dmDeliverRankDF.createOrReplaceTempView("dmDeliverRankTmp")
    spark.sql(
      s"""
         | insert  overwrite table   dm_banktransfer_rank  partition(dt=${endDate})
         | select  c_custno
         |    ,branch_no
         |    ,supercom_apprinsum
         |    ,supercom_approutsum
         |    ,supercom_apprinfre
         |    ,supercom_approutfre
         |    ,supercom_apprsumdva
         |    ,supercom_apprfredva
         |    ,supercom_insumten
         |    ,supercom_outsumten
         |    ,supercom_infreten
         |    ,supercom_outfreten
         |    ,supercom_sumdvaten
         |    ,supercom_fredvaten
         |    ,superbra_apprinsum
         |    ,superbra_approutsum
         |    ,superbra_apprinfre
         |    ,superbra_approutfre
         |    ,superbra_apprsumdva
         |    ,superbra_apprfredva
         |    ,superbra_insumten
         |    ,superbra_outsumten
         |    ,supercbra_infreten
         |    ,superbra_outfreten
         |    ,superbra_sumdvaten
         |    ,superbra_fredvaten
         |    ,input_date
         |   from dmDeliverRankTmp
       """.stripMargin.replace("\r\n"," "))

    spark.stop()


  }

}
