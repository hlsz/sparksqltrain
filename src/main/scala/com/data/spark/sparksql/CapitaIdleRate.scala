package com.data.spark.sparksql

import com.data.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class CapitaIdleRate {

  val conf = new SparkConf()
    .setAppName("CapitaIdleRate")
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

  def capitaIdleRate(calcuDate:Int, approchMonths:Int, remoteMonths:Int): Unit =
  {
    spark.sql("use bigdata")
    val calcDateVal = DateUtils.intToDate(calcuDate)
    // addOrMinusDayToLong
    val approchMonthsVal:Int = DateUtils.dateToInt(DateUtils.addMonth(calcDateVal, -approchMonths))
    val remoteMonthsVal:Int = DateUtils.dateToInt(DateUtils.addMonth(calcDateVal, -remoteMonths))

    val capitalCalDataDF = spark.sql(
      s"""
         | select
         |	cust_no c_custno,
         |	oc_date,
         |	balance,
         |	total_assbal
         | from bigdata.custtotalasset_dm
         | where oc_date <  ${calcuDate}
         | and oc_date >=   ${remoteMonthsVal}
       """.stripMargin)
    capitalCalDataDF.createOrReplaceTempView("capitalCalDataTmp")

    //计算较短时间范围内的资产闲置率
    val calApprochCapitalIdleDF = spark.sql(
      s"""
         | select c_custno,
         |	   case when total_assbal_sum = 0 then 0
         |		 else round(balance_sum/total_assbal_sum,4) end approch_idle_rate
         | from
         |   (select c_custno,
         |		sum(balance) balance_sum,
         |		sum(total_assbal) total_assbal_sum
         |   from capitalCalDataTmp
         |   where oc_date >=${approchMonthsVal}
         |   group by c_custno) a
       """.stripMargin)
    calApprochCapitalIdleDF.createOrReplaceTempView("calApprochCapitalIdleTmp")

    //  --计算较长时间范围内的资产闲置率
    val calRemoteCapitalIdleDF = spark.sql(
      s"""
         | select c_custno,
         |   case when total_assbal_sum = 0 then 0
         |		else round(balance_sum/total_assbal_sum,4) end remote_idle_rate
         | from
         |   (select c_custno,
         |		sum(balance) balance_sum,
         |		sum(total_assbal) total_assbal_sum
         |   from capitalCalDataTmp
         |   group by c_custno) a
       """.stripMargin)
    calRemoteCapitalIdleDF.createOrReplaceTempView("calRemoteCapitalIdleTmp")

    spark.sql(" create table if not exists capital_idal_rate_tb " +
      " ( c_custno string, branch_no string, client_id int, approch_idle_rate double, remote_idle_rate double, " +
      " idle_rate_tendency double, input_date int ) " +
      s" ROW FORMAT DELIMITED FIELDS TERMINATED BY ${raw"'\t'"} " +

      s" LINES TERMINATED BY ${raw"'\n'"} " +
      " stored as textfile " )

    //  --插入计算结果
    val capitalIdalRateDF = spark.sql(
      s"""
         | select
         |	 c_custno,
         |	 branch_no,
         |	 client_id,
         |	 approch_idle_rate,
         |	 remote_idle_rate,
         |	 nvl(idle_rate_tendency,0) idle_rate_tendency,
         |	${calcuDate} input_date
         | from
         |	 (select c.c_custno,
         |			branch_no,
         |			 client_id,
         |			 nvl(approch_idle_rate,0) approch_idle_rate,
         |			 remote_idle_rate,
         |			 case when (remote_idle_rate = 0 or approch_idle_rate = 0 )  then 0
         |				 else round(nvl(approch_idle_rate,0) / remote_idle_rate,4) end idle_rate_tendency
         |	 from
         |		 (select a.c_custno,
         |				a.branch_no,
         |				client_id,
         |				nvl(remote_idle_rate,0) remote_idle_rate
         |		 from bigdata.c_cust_branch_tb a
         |		 left join  calRemoteCapitalIdleTmp  b on a.c_custno = b.c_custno) c
         |	  left join  calApprochCapitalIdleTmp   d on c.c_custno = d.c_custno) d
       """.stripMargin)

    capitalIdalRateDF.createOrReplaceTempView("capitalIdalRateTmp")
    spark.sql("insert overwrite table  capital_idal_rate_tb select * from capitalIdalRateTmp")


    val countAllClientDF = spark.sql("select client_id from c_cust_branch_tb ")
    val countAllClient = countAllClientDF.count().toInt

    spark.sql("create table if not exists result_idle_rate " +
      "( client_id int, branch_no string, c_custno string, approch_idle_rate double, remote_idle_rate double, " +
      " approidle_all_rank double, approidle_b_rank double, remoteidle_all_rank double, remoteidle_b_rank double, " +
      " input_date int, idle_rate_tendency double  ) " +
      s" ROW FORMAT DELIMITED FIELDS TERMINATED BY ${raw"'\t'"} " +

      s" LINES TERMINATED BY ${raw"'\n'"} " +
      " stored as textfile ")
    spark.sql("delete  from result_idle_rate where input_date = "+calcuDate)


    spark.sql(
      s"""
         | insert  into
         |  result_idle_rate (
         |   client_id
         |   ,branch_no
         |   ,c_custno
         |   ,approch_idle_rate
         |    ,remote_idle_rate
         |    ,idle_rate_tendency
         |    ,approidle_all_rank
         |    ,approidle_b_rank
         |    ,remoteidle_all_rank
         |    ,remoteidle_b_rank
         |    ,input_date
         |  )
         |   select
         |   a.client_id
         |   ,a.branch_no
         |   ,a.c_custno
         |   ,a.approch_idle_rate
         |   ,a.remote_idle_rate
         |   ,a.idle_rate_tendency
         |   ,round((${countAllClient} -nvl(a.acompay_rank,${countAllClient}) ) * 100 /${countAllClient} ,4) approidle_all_rank
         |   ,round((b.branchallcount -  nvl(a.abranch_rank,b.branchallcount)) * 100 /b.branchallcount ,4) approidle_b_rank
         |  , round((${countAllClient} -nvl(a.rcompay_rank,${countAllClient}) ) * 100 /${countAllClient} ,4) remoteidle_all_rank
         |  , round((b.branchallcount -  nvl(a.rbranch_rank,b.branchallcount)) * 100 /b.branchallcount ,4) remoteidle_b_rank
         |  ,${calcuDate} input_date
         |  from (
         |  select
         |   client_id
         |   ,branch_no
         |   ,c_custno
         |   ,approch_idle_rate
         |   ,remote_idle_rate
         |   ,idle_rate_tendency
         | ,dense_rank() over(order by approch_idle_rate  desc)  acompay_rank
         | ,dense_rank() over(partition by branch_no order by approch_idle_rate desc ) abranch_rank
         | ,dense_rank() over(order by remote_idle_rate  desc)  rcompay_rank
         | ,dense_rank() over(partition by branch_no order by remote_idle_rate desc ) rbranch_rank
         | from capital_idal_rate_tb  where input_date = ${calcuDate} ) a
         | left join (select  count(*) branchallcount ,branch_no from bigdata.c_cust_branch_tb   group by branch_no
         |  ) b  on a.branch_no =b.branch_no
       """.stripMargin)

    spark.sql(
      s"""
         | create  table  IF NOT EXISTS   bigdata.result_branchidle (
         |  branch_no string ,
         | approavg_idlerate double ,
         | appromed_idlerate double,
         | remoteavg_idlerate double,
         | remotemed_idlerate double,
         | input_date int )
         | ROW FORMAT DELIMITED FIELDS TERMINATED BY ${raw"'\t'"}
         | LINES TERMINATED BY ${raw"'\n’"}
         |
         | stored as textfile
       """.stripMargin)


    spark.sql(
      s"""
         | insert  into result_branchidle(
         |  approavg_idlerate
         | ,appromed_idlerate
         | ,branch_no
         | ,remoteavg_idlerate
         | ,remotemed_idlerate
         | ,input_date
         | )
         | select   round(avg(approch_idle_rate),4)  approavg_idlerate , round(median(approch_idle_rate),4) appromed_idlerate,
         |         |         -1 branch_no , round(avg(remote_idle_rate),4)  remoteavg_idlerate,
         |      round(median(remote_idle_rate),4) remotemed_idlerate, ${calcuDate} input_date
         |  from  capitalIdalRateTmp
         |  where input_date = ${calcuDate}
         | union
         | select  round(avg(approch_idle_rate),4) approavg_idlerate
         |    ,round(median(approch_idle_rate),4) appromed_idlerate
         |    ,branch_no, round(avg(remote_idle_rate),4)  remoteavg_idlerate,
         |  round(median(remote_idle_rate),4) remotemed_idlerate , ${calcuDate} input_date
         | from capitalIdalRateTmp
         | where  input_date = ${calcuDate}  group by branch_no
       """.stripMargin)

    spark.stop()

  }

}

object CapitaIdleRate
{
  def main(args: Array[String]): Unit = {
    new CapitaIdleRate().capitaIdleRate(20190401,1,3)
  }
}
