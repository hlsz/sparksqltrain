package com.data.spark

import com.data.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class BankBondCalculte {
  val conf = new SparkConf()
    .setAppName("GetTargetDataReplace")
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

  def bankBondCalculte(calcuDate:Int, approchMonths:Int, remoteMonths:Int): Unit =
  {

    spark.sql("use bigdata")
    val calcDateVal = DateUtils.intToDate(calcuDate)
    // addOrMinusDayToLong
    val approchMonthsVal:Int = DateUtils.dateToInt(DateUtils.addMonth(calcDateVal, -approchMonths))
    val remoteMonthsVal:Int = DateUtils.dateToInt(DateUtils.addMonth(calcDateVal, -remoteMonths))

    spark.sql("create  table  IF NOT EXISTS  banktransfer_tb ( " +
      " c_custno string, client_id string, curr_date int, bktrans_status string, trans_type string, occur_balance double  ) " +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' " +
      " LINES TERMINATED BY ‘\n’ collection items terminated by '-' " +
      " map keys terminated by ':' " +
      " stored as textfile ")

    spark.sql("into overwrite banktransfer_tb (c_custno, client_id, curr_date, bktrans_status, trans_type, occur_balance) " +
      " select c_custno, a.client_id, nvl(curr_date,0) curr_date, nvl(bktrans_status,'0') bktrans_status , " +
      " nvl(trans_type, '0') trans_type , nvl(occur_balance, '0') occur_balance " +
      " from global_temp.c_cust_branch_tb a " +
      " left outer join (select client_id, curr_date, bktrans_status, trans_type, occur_balance " +
      "    from bigdata.hs08_his_banktransfer where bktrans_status = '2' and trans_type in ('01', '02') " +
      "  and curr_date < "+calcuDate+" " +
      "  and curr_date >= "+remoteMonthsVal+") b on  a.client_id = b.client_id " )
    //计算较短时间内的转入金额/转入笔数、
    val inApproBalanceDF = spark.sql("select client_id, count(client_id) appro_in_frequency, sum(occur_balance) appro_in_sum " +
      " from banktransfer_tb where trans_type = '01' and curr_date >= "+approchMonthsVal+" group by client_id")
    inApproBalanceDF.createOrReplaceTempView("inApproBalanceTmp")

    //计算较长时间内的转入金额/转入笔数
    val inRemoteBalanceDF = spark.sql("select client_id, count(client_id) remote_in_frequency, " +
      " sum(occur_balance) remot_in_sum " +
      " from banktransfer_tb where trans_type = '01'   group by client_id")
    inRemoteBalanceDF.createOrReplaceTempView("inRemoteBalanceTmp")

    //计算较短时间内的转出金额/转出笔数
    val outApproBalanceDF = spark.sql("select client_id, count(client_id) appro_out_frequency, sum(occur_balance) appro_out_sum " +
      " from banktransfer_tb where trans_type = '02' and curr_date >= "+approchMonthsVal+" group by client_id ")
    outApproBalanceDF.createOrReplaceTempView("outApproBalanceTmp")
    //计算较长时间内的转出金额/转出笔数
    val outRemotBalanceDF = spark.sql("select client_id, count(client_id) remot_out_frequency, sum(occur_balance) remot_out_sum " +
      " from banktransfer_tb where trans_type = '02'  group by client_id ")
    outRemotBalanceDF.createOrReplaceTempView("outRemotBalanceTmp")

    val monthDvalue = remoteMonths / approchMonths

    spark.sql("create  table  IF NOT EXISTS  bigdata.banktransfer_result_tb (" +
      " c_custno string, branch_no string, client_id, appro_in_frequency int, appro_out_frequency int, appro_frequency_dvalue int, " +
      " appro_in_sum double, appro_out_sum double, appro_sum_dvalue double, remote_in_frequency int, remot_out_frequency int, " +
      " remot_frequency_dvalue int, remot_in_sum double, remot_out_sum double , remot_sum_dvalue  double , in_frequency_tendency double, " +
      " out_frequency_tendency double, in_sum_tendency double, out_sum_tendency double, frequency_dvalue_tendency double, " +
      " sum_dvalue_tendency double, input_date int  )" +
      " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' " +
      " LINES TERMINATED BY ‘\n’ collection items terminated by '-' " +
      " map keys terminated by ':' " +
      " stored as textfile ")

    spark.sql("delete from bigdata.banktransfer_result_tb where input_date = "+calcuDate+"")

    spark.sql(
      s"""
        |insert into bigdata.banktransfer_result_tb
        |      (c_custno,branch_no,client_id,appro_in_frequency,appro_out_frequency,appro_frequency_dvalue,
        |       appro_in_sum,appro_out_sum,appro_sum_dvalue,remote_in_frequency,remot_out_frequency,
        |				remot_frequency_dvalue,remot_in_sum,remot_out_sum,
        |       remot_sum_dvalue,in_frequency_tendency,out_frequency_tendency,in_sum_tendency,out_sum_tendency,
        |				frequency_dvalue_tendency,sum_dvalue_tendency,input_date )
        |  select  c_custno,
        |  branch_no,
        |  client_id,
        |  appro_in_frequency,
        |  appro_out_frequency,
        |  appro_frequency_dvalue,
        |  appro_in_sum,
        |  appro_out_sum,
        |  appro_sum_dvalue,
        |  remote_in_frequency,
        |  remot_out_frequency,
        |  remot_frequency_dvalue,
        |  remot_in_sum,
        |  remot_out_sum,
        |  remot_sum_dvalue,
        |  round(in_frequency_tendency,4) in_frequency_tendency,
        |  round(out_frequency_tendency,4) out_frequency_tendency,
        |  round(in_sum_tendency,4) in_sum_tendency,
        |  round(out_sum_tendency,4) out_sum_tendency,
        |  round(frequency_dvalue_tendency,4) frequency_dvalue_tendency,
        |  round(sum_dvalue_tendency,4) sum_dvalue_tendency,
        |  ${calcuDate} input_date
        |  from
        |    (select c_custno,
        |     branch_no,
        |     client_id,
        |     appro_in_frequency,
        |     appro_out_frequency,
        |     appro_frequency_dvalue,
        |     appro_in_sum,
        |     appro_out_sum,
        |     appro_sum_dvalue,
        |     remote_in_frequency,
        |     remot_out_frequency,
        |     remot_frequency_dvalue,
        |     remot_in_sum,
        |     remot_out_sum,
        |     remot_sum_dvalue,
        |     case when remote_in_frequency =0 then 0 else appro_in_frequency * ( ${monthDvalue} ) / remote_in_frequency end in_frequency_tendency,
        |     case when remot_out_frequency =0 then 0 else appro_out_frequency * ( ${monthDvalue} ) / remot_out_frequency end out_frequency_tendency,
        |     case when remot_in_sum =0 then 0 else appro_in_sum * (${monthDvalue} ) / remot_in_sum end in_sum_tendency,
        |     case when remot_out_sum =0 then 0 else appro_out_sum * (${monthDvalue} ) / remot_out_sum end out_sum_tendency,
        |     case when remot_frequency_dvalue =0 then 0 else appro_frequency_dvalue * (${monthDvalue}) / remot_frequency_dvalue end frequency_dvalue_tendency,
        |     case when remot_sum_dvalue =0 then 0 else appro_sum_dvalue * (${monthDvalue}) / remot_sum_dvalue end sum_dvalue_tendency
        |        from
        |            (select  c_custno,
        |             branch_no,
        |    g.client_id,
        |    nvl(appro_in_frequency,0) appro_in_frequency,
        |    nvl(appro_out_frequency,0) appro_out_frequency,
        |    (nvl(appro_in_frequency,0) - nvl(appro_out_frequency,0)) appro_frequency_dvalue,
        |    nvl(appro_in_sum,0) appro_in_sum,
        |    nvl(appro_out_sum,0) appro_out_sum,
        |    (nvl(appro_in_sum,0) - nvl(appro_out_sum,0)) appro_sum_dvalue,
        |    nvl(remote_in_frequency,0) remote_in_frequency,
        |    nvl(remot_out_frequency,0) remot_out_frequency,
        |    (nvl(remote_in_frequency,0) - nvl(remot_out_frequency,0)) remot_frequency_dvalue,
        |    nvl(remot_in_sum,0) remot_in_sum,
        |    nvl(remot_out_sum,0) remot_out_sum,
        |    (nvl(remot_in_sum,0) - nvl(remot_out_sum,0)) remot_sum_dvalue
        |            from
        | (select  c_custno,
        |   branch_no,
        |        e.client_id,
        |        remote_in_frequency,
        |        remot_in_sum,
        |        remot_out_frequency,
        |        remot_out_sum,
        |        appro_in_frequency,
        |        appro_in_sum
        | from
        |     (select  c_custno,
        |             branch_no,
        |            c.client_id,
        |            remote_in_frequency,
        |            remot_in_sum,
        |            remot_out_frequency,
        |            remot_out_sum
        |     from
        |           (select  c_custno,
        |    a.branch_no,
        |   a.client_id,
        |   remote_in_frequency,
        |   remot_in_sum
        |           from c_cust_no_tb a
        |      left outer join ( inRemotBalanceTmp ) b on a.client_id = b.client_id) c
        |     left outer join ( outRemotBalanceTmp ) d on c.client_id = d.client_id) e
        |    left outer join ( inAqpproBalanceTmp) f on e.client_id = f.client_id) g
        |    left outer join ( outAqpproBalanceTmp) h  on g.client_id = h.client_id))
      """.stripMargin)

    spark.sql(s"""create  table  IF NOT EXISTS  bigdata.result_branchbanktransfer ( " +
                 |   branch_no int
                 | ,appro_avginsum double
                 | ,appro_avgoutsum double
                 | ,appro_avginfrequ double
                 | ,appro_avgoutfrequ  double
                 | ,appro_avgsumdvalue double
                 | ,appro_avgfrequdvalue double
                 | ,avginsumtrendency double
                 | ,avgoutsumtrendency double
                 | ,avginfrequtendency double
                 | ,avgoutfrequtendency double
                 | ,avgsumdvaluetendency double
                 | ,avgfrequdvaluetendency double
                 | ,appro_medinsum double
                 | ,appro_medoutsum double
                 | ,appro_medinfrequ double
                 | ,appro_medoutfrequ double
                 | ,appro_medsumdvalue double
                 | ,appro_medfrequdvalue double
                 | ,medinsumtrendency double
                 | ,medoutsumtrendency double
                 | ,medinfrequtendency double
                 | ,medoutfrequtendency double
                 | ,medsumdvaluetendency double
                 | ,medfrequdvaluetendency double
                 | ,remo_avginsum double
                 | ,remo_avgoutsum double
                 | ,remo_avginfrequ double
                 | ,remo_avgoutfrequ double
                 | ,remo_avgsumdvalue double
                 | ,remo_avgfrequdvalue double
                 | ,remo_medinsum double
                 | ,remo_medoutsum double
                 | ,remo_medinfrequ double
                 | ,remo_medoutfrequ double
                 | ,remo_medsumdvalue double
                 | ,remo_medfrequdvalue double
                 | ,input_date int ) """.stripMargin)

    spark.sql("delete  from result_branchbanktransfer  where input_date = "+calcuDate)
    spark.sql(
      s"""
        | insert  into result_branchbanktransfer(
        |   branch_no
        | ,appro_avginsum
        | ,appro_avgoutsum
        | ,appro_avginfrequ
        | ,appro_avgoutfrequ
        | ,appro_avgsumdvalue
        | ,appro_avgfrequdvalue
        | ,avginsumtrendency
        | ,avgoutsumtrendency
        | ,avginfrequtendency
        | ,avgoutfrequtendency
        | ,avgsumdvaluetendency
        | ,avgfrequdvaluetendency
        | ,appro_medinsum
        | ,appro_medoutsum
        | ,appro_medinfrequ
        | ,appro_medoutfrequ
        | ,appro_medsumdvalue
        | ,appro_medfrequdvalue
        | ,medinsumtrendency
        | ,medoutsumtrendency
        | ,medinfrequtendency
        | ,medoutfrequtendency
        | ,medsumdvaluetendency
        | ,medfrequdvaluetendency
        | ,remo_avginsum
        | ,remo_avgoutsum
        | ,remo_avginfrequ
        | ,remo_avgoutfrequ
        | ,remo_avgsumdvalue
        | ,remo_avgfrequdvalue
        | ,remo_medinsum
        | ,remo_medoutsum
        | ,remo_medinfrequ
        | ,remo_medoutfrequ
        | ,remo_medsumdvalue
        | ,remo_medfrequdvalue
        | ,input_date
        | )
        | select
        |        -1
        |    ,round(avg(appro_in_sum),4)
        |    , round(avg(appro_out_sum), 4)
        |    , round(avg(appro_in_frequency) ,4)
        |    , round(avg(appro_out_frequency) ,4)
        |    , round(avg(appro_sum_dvalue),4)
        |    , round(avg(appro_frequency_dvalue),4)
        |    , round(avg(in_sum_tendency),4)
        |    , round(avg(out_sum_tendency),4)
        |    , round(avg(in_frequency_tendency),4)
        |    , round(avg(out_frequency_tendency),4)
        |    , round(avg(sum_dvalue_tendency),4)
        |    , round(avg(frequency_dvalue_tendency),4)
        |    , round(median(appro_in_sum),4)
        |    , round(median(appro_out_sum),4)
        |    , round(median(appro_in_frequency),4)
        |    , round(median(appro_out_frequency),4)
        |    , round(median(appro_sum_dvalue) ,4)
        |    , round(median(appro_frequency_dvalue)  ,4)
        |    , round(median(in_sum_tendency) ,4)
        |    , round(median(out_sum_tendency),4)
        |    , round(median(in_frequency_tendency),4)
        |    , round(median(out_frequency_tendency) ,4)
        |    , round(median(sum_dvalue_tendency) ,4)
        |    , round(median(frequency_dvalue_tendency) ,4)
        |    , round(avg(remot_in_sum) ,4)
        |    , round(avg(remot_out_sum),4)
        |    , round(avg(remote_in_frequency)  ,4)
        |    , round(avg(remot_out_frequency)  ,4)
        |    , round(avg(remot_sum_dvalue) ,4)
        |    , round(avg(remot_frequency_dvalue),4)
        |    , round(median(remot_in_sum) ,4)
        |    , round(median(remot_out_sum)  ,4)
        |    , round(median(remote_in_frequency),4)
        |    , round(median(remot_out_frequency)  ,4)
        |    , round(median(remot_sum_dvalue)  ,4)
        |    , round(median(remot_frequency_dvalue),4)
        |     ,calcu_date
        |    from banktransfer_result_tb  where input_date= ${calcuDate}
        | union
        | select
        |     branch_no
        |    , avg(appro_in_sum)
        |     ,avg(appro_out_sum)
        |     ,avg(appro_in_frequency)
        |     ,avg(appro_out_frequency)
        |     ,avg(appro_sum_dvalue)
        |     ,avg(appro_frequency_dvalue)
        |     ,avg(in_sum_tendency)
        |     ,avg(out_sum_tendency)
        |     ,avg(in_frequency_tendency)
        |     ,avg(out_frequency_tendency)
        |     ,avg(sum_dvalue_tendency)
        |     ,avg(frequency_dvalue_tendency)
        |     ,median(appro_in_sum)
        |     ,median(appro_out_sum)
        |     ,median(appro_in_frequency)
        |     ,median(appro_out_frequency)
        |     ,median(appro_sum_dvalue)
        |     ,median(appro_frequency_dvalue)
        |     ,median(in_sum_tendency)
        |     ,median(out_sum_tendency)
        |     ,median(in_frequency_tendency)
        |     ,median(out_frequency_tendency)
        |     ,median(sum_dvalue_tendency)
        |     ,median(frequency_dvalue_tendency)
        |     ,avg(remot_in_sum)
        |     ,avg(remot_out_sum)
        |     ,avg(remote_in_frequency)
        |     ,avg(remot_out_frequency)
        |     ,avg(remot_sum_dvalue)
        |     ,avg(remot_frequency_dvalue)
        |     ,median(remot_in_sum)
        |     ,median(remot_out_sum)
        |     ,median(remote_in_frequency)
        |     ,median(remot_out_frequency)
        |     ,median(remot_sum_dvalue)
        |     ,median(remot_frequency_dvalue)
        |     ,calcu_date
        |	 from banktransfer_result_tb where input_date= ${calcuDate}  group by branch_no
      """.stripMargin)

    spark.sql("delete   from  result_banktranrk  where input_date = "+calcuDate)

    spark.sql(
      s"""
         | insert  into result_banktranrk (
         |     c_custno,
         |    branch_no,
         |    supercom_apprinsum
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
         |    ,input_date )
         | select
         |  c_custno
         | ,a.branch_no
         | ,case when appro_in_sum=0 then 0 else
         | round((countAll_client -nvl(a.supercom_apprinsum,countAll_client) ) * 100 /countAll_client ,4) end
         | ,case when appro_out_sum=0 then 0 else
         | round((countAll_client -nvl(a.supercom_approutsum,countAll_client) ) * 100 /countAll_client ,4) end
         | ,case when appro_in_frequency=0 then 0 else
         | round((countAll_client -nvl(a.supercom_apprinfre,countAll_client) ) * 100 /countAll_client ,4) end
         | ,case when appro_out_frequency=0 then 0 else
         | round((countAll_client -nvl(a.supercom_approutfre,countAll_client) ) * 100 /countAll_client ,4) end
         | ,round((countAll_client -nvl(a.supercom_apprsumdva,countAll_client) ) * 100 /countAll_client ,4)
         | ,case when appro_frequency_dvalue<=0 then 0 else
         | round((countAll_client -nvl(a.supercom_apprfredva,countAll_client) ) * 100 /countAll_client ,4) end
         | ,round((countAll_client -nvl(a.supercom_insumten,countAll_client) ) * 100 /countAll_client ,4)
         | ,round((countAll_client -nvl(a.supercom_outsumten,countAll_client) ) * 100 /countAll_client ,4)
         | ,round((countAll_client -nvl(a.supercom_infreten,countAll_client) ) * 100 /countAll_client ,4)
         | ,round((countAll_client -nvl(a.supercom_outfreten,countAll_client) ) * 100 /countAll_client ,4)
         | ,round((countAll_client -nvl(a.supercom_sumdvaten,countAll_client) ) * 100 /countAll_client ,4)
         | ,round((countAll_client -nvl(a.supercom_fredvaten,countAll_client) ) * 100 /countAll_client ,4)
         | ,case when appro_in_sum=0 then 0 else
         | round((b.branchallcount -nvl(a.superbra_apprinsum,b.branchallcount) ) * 100 /b.branchallcount ,4) end
         | ,case when appro_out_sum=0 then 0 else
         | round((b.branchallcount -nvl(a.superbra_approutsum,b.branchallcount) ) * 100 /b.branchallcount ,4) end
         | ,case when appro_in_frequency=0 then 0 else
         | round((b.branchallcount -nvl(a.superbra_apprinfre,b.branchallcount) ) * 100 /b.branchallcount ,4) end
         | ,case when appro_out_frequency=0 then 0 else
         | round((b.branchallcount -nvl(a.superbra_approutfre,b.branchallcount) ) * 100 /b.branchallcount ,4) end
         | ,round((b.branchallcount -nvl(a.superbra_apprsumdva,b.branchallcount) ) * 100 /b.branchallcount ,4)
         | ,case when appro_frequency_dvalue<=0 then 0 else
         | round((b.branchallcount -nvl(a.superbra_apprfredva,b.branchallcount) ) * 100 /b.branchallcount ,4) end
         | ,round((b.branchallcount -nvl(a.superbra_insumten,b.branchallcount) ) * 100 /b.branchallcount ,4)
         | ,round((b.branchallcount -nvl(a.superbra_outsumten,b.branchallcount) ) * 100 /b.branchallcount ,4)
         | ,round((b.branchallcount -nvl(a.supercbra_infreten,b.branchallcount) ) * 100 /b.branchallcount ,4)
         | ,round((b.branchallcount -nvl(a.superbra_outfreten,b.branchallcount) ) * 100 /b.branchallcount ,4)
         | ,round((b.branchallcount -nvl(a.superbra_sumdvaten,b.branchallcount) ) * 100 /b.branchallcount ,4)
         | ,round((b.branchallcount -nvl(a.superbra_fredvaten,b.branchallcount) ) * 100 /b.branchallcount ,4)
         | ,${calcuDate} input_date
         | from (
         |  select
         |  c_custno
         |  ,branch_no
         |  ,appro_in_sum
         |  ,appro_out_sum
         |  ,appro_in_frequency
         |  ,appro_out_frequency
         |  ,appro_sum_dvalue
         |  ,appro_frequency_dvalue
         | ,dense_rank() over(order by appro_in_sum  desc)              supercom_apprinsum
         | ,dense_rank() over(order by appro_out_sum  desc)             supercom_approutsum
         | ,dense_rank() over(order by appro_in_frequency  desc)        supercom_apprinfre
         | ,dense_rank() over(order by appro_out_frequency  desc)       supercom_approutfre
         | ,dense_rank() over(order by appro_sum_dvalue  desc)          supercom_apprsumdva
         | ,dense_rank() over(order by appro_frequency_dvalue  desc)    supercom_apprfredva
         | ,dense_rank() over(order by in_sum_tendency  desc)           supercom_insumten
         | ,dense_rank() over(order by out_sum_tendency  desc)          supercom_outsumten
         | ,dense_rank() over(order by in_frequency_tendency  desc)     supercom_infreten
         | ,dense_rank() over(order by out_frequency_tendency  desc)    supercom_outfreten
         | ,dense_rank() over(order by sum_dvalue_tendency  desc)       supercom_sumdvaten
         | ,dense_rank() over(order by frequency_dvalue_tendency  desc) supercom_fredvaten
         | ,dense_rank()  over(partition by branch_no order by appro_in_sum  desc)              superbra_apprinsum
         | ,dense_rank() over(partition by branch_no  order by appro_out_sum  desc)             superbra_approutsum
         | ,dense_rank() over(partition by branch_no  order by appro_in_frequency  desc)        superbra_apprinfre
         | ,dense_rank() over(partition by branch_no  order by appro_out_frequency  desc)       superbra_approutfre
         | ,dense_rank() over(partition by branch_no  order by appro_sum_dvalue  desc)          superbra_apprsumdva
         | ,dense_rank() over(partition by branch_no  order by appro_frequency_dvalue  desc)    superbra_apprfredva
         | ,dense_rank() over(partition by branch_no  order by in_sum_tendency  desc)           superbra_insumten
         | ,dense_rank() over(partition by branch_no  order by out_sum_tendency  desc)          superbra_outsumten
         | ,dense_rank() over(partition by branch_no  order by in_frequency_tendency  desc)     supercbra_infreten
         | ,dense_rank() over(partition by branch_no  order by out_frequency_tendency  desc)    superbra_outfreten
         | ,dense_rank() over(partition by branch_no  order by sum_dvalue_tendency  desc)       superbra_sumdvaten
         | ,dense_rank() over(partition by branch_no  order by frequency_dvalue_tendency  desc) superbra_fredvaten
         | from banktransfer_result_tb where input_date= ${calcuDate} )a
         | left join (select   count(*) branchallcount ,branch_no from global_temp.c_cust_branch_tb
         | group by branch_no  ) b on a.branch_no = b.branch_no
       """.stripMargin)
    spark.stop()

  }

}
