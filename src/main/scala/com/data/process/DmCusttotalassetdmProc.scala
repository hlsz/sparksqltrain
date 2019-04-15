package com.data.process

import com.data.utils.DateUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DmCusttotalassetdmProc {
  def main(args: Array[String]): Unit = {
    new DmCusttotalassetdmProc().custtotalassetdmProc(20190401,3)

  }

}

class DmCusttotalassetdmProc {

  val conf = new SparkConf()
    .setAppName("DmCusttotalassetdmProc")
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
  def custtotalassetdmProc(endDate:Int, maxIntervalVal:Int): Unit =
  {
    val minIntervalValInt =  1
    custtotalassetdmProc(endDate, maxIntervalVal, minIntervalValInt)
  }


  /**
    *
    * @param endDate
    * @param maxIntervalVal
    * @param minIntervalVal
    */
  def custtotalassetdmProc(endDate:Int, maxIntervalVal:Int, minIntervalVal:Int): Unit ={

    var endDateFormatStr  =  DateUtils.intToDateStr(endDate, "yyyy-MM-dd")
    var startDateVal = DateUtils.addMonth( DateUtils.intToDate(endDate), -maxIntervalVal )
    var startDate:Int = DateUtils.dateToInt(startDateVal)
    var startDateFormatStr  =  DateUtils.intToDateStr(startDate, "yyyy-MM-dd")

    val intervalDays  = DateUtils.intervalDays(startDateFormatStr,endDateFormatStr)
    val intervalMonths =  DateUtils.intervalMonths(startDateFormatStr,endDateFormatStr)

    val minIntervalValResult =  minIntervalVal

    spark.sql("use bigdata")

    val dmCusttotalassetDmDF = spark.sql(
      s"""
         | select
         |	  t.c_custno,t.client_id,t.branch_no,t.open_date,t.organ_flag,t.birthday,
         |	  t.total_assbal,t.balance
         | from (
         |	select c.c_custno,
         |		   c.client_id,
         |		   c.branch_no,
         |		   c.open_date,
         |		   c.organ_flag,
         |		   c.birthday,
         |		   nvl(cd.total_assbal,0) total_assbal,
         |		   nvl(cd.balance,0) balance,
         |		   cd.oc_date
         |	from hs08_client_for_ai c
         |	left join bigdata.custtotalasset_dm cd on c.c_custno = cd.cust_no
         |	where cd.oc_date >= ${startDate} and cd.oc_date < ${endDate}
         |	) t
       """.stripMargin.replace("\r\n"," "))



    dmCusttotalassetDmDF.createOrReplaceTempView("dmCusttotalassetDmTmp")

    spark.sql(
      s"""
         | create table if not exists bigdata.dm_custtotalasset_dm_stat
         | (
         |    client_id int,branch_no string,
         |    c_custno string,balance_sum double,
         |    total_assbal_sum double, peak_vasset double,
         |		approch_idle_rate double,
         |		remote_idle_rate double,
         |		idle_rate_tendency double,
         |    input_date int
         | )  PARTITIONED BY (input_date int)
         |  ROW FORMAT DELIMITED FIELDS TERMINATED BY  ${raw"'\t'"}
         | LINES TERMINATED BY ${raw"'\n'"}
         | stored as textfile
       """.stripMargin.replace("\r\n"," "))

    val dmCusttotalassetdmStatDF = spark.sql(
      s"""
         | select client_id,
         | branch_no,
         | c_custno,
         | balance_sum,
         | total_assbal_sum,
         | peak_vasset,
         | approch_idle_rate,
         | remote_idle_rate,
         | idle_rate_tendency,
         | ${endDate} input_date
         | from (
         |		select 	client_id,branch_no,c_custno,balance_sum,total_assbal_sum,peak_vasset,
         |				approch_idle_rate,
         |				remote_idle_rate,
         |				case when (remote_idle_rate = 0 or approch_idle_rate = 0 )  then 0
         |					   else round(nvl(approch_idle_rate,0) / remote_idle_rate,2) * 100 end idle_rate_tendency
         |		from (
         |				select  client_id,branch_no,c_custno,balance_sum,total_assbal_sum,peak_vasset,
         |						case when total_assbal_sum = 0 then 0
         |							   else round(balance_sum/total_assbal_sum,2) * 100 end approch_idle_rate ,
         |						case when total_assbal_sum = 0 then 0
         |							   else round(balance_sum/total_assbal_sum,2) * 100 end remote_idle_rate
         |				from (
         |						select  client_id,c_custno,branch_no,
         |								sum(balance) balance_sum,
         |								sum(total_assbal) total_assbal_sum,
         |								max(total_assbal)  peak_vasset
         |						from (
         |								 select
         |									   client_id,c_custno,branch_no,
         |									   balance,
         |									   total_assbal
         |								 from dmCusttotalassetDmTmp
         |							)
         |						group by client_id,c_custno,branch_no
         |					)
         |			 )
         |		)
       """.stripMargin.replace("\r\n"," "))
    dmCusttotalassetdmStatDF.createOrReplaceTempView("dmCusttotalassetdmStatTmp")

    spark.sql("insert  overwrite table   dm_custtotalasset_dm_stat select * from  dmCusttotalassetdmStatTmp ")


    spark.sql(
      s"""
         | create table if not exists bigdata.dm_custtotalasset_dm_cacl
         | (
         | branch_no string
         | ,peak_vasset_avg double
         | ,peak_vasset_med double
         | ,input_date int
         | )  PARTITIONED BY (input_date int)
         | ROW FORMAT DELIMITED FIELDS TERMINATED BY  ${raw"'\t'"}
         | LINES TERMINATED BY ${raw"'\n'"}
         | stored as textfile
       """.stripMargin.replace("\r\n"," "))

    val dmCusttotalassetDmCaclDF = spark.sql(
      s"""
         | select
         |	case when branch_no is null then  -1 else branch_no end branch_no
         |	,peak_vasset_avg
         |	,peak_vasset_med
         |  ,${endDate} input_date
         | from (
         |	select branch_no
         |		,round( avg(peak_vasset), 2) peak_vasset_avg
         |		,round( percentile_approx(peak_vasset,0.5), 2) peak_vasset_med
         |	 from dmCusttotalassetdmStatTmp
         |	group by branch_no,1 grouping sets(branch_no,1)
         |	)
       """.stripMargin.replace("\r\n"," "))
    dmCusttotalassetDmCaclDF.createOrReplaceTempView("dmCusttotalassetDmCaclTmp")

    spark.sql("insert  overwrite table   dm_custtotalasset_dm_cacl select * from dmCusttotalassetDmCaclTmp ")

    spark.stop()

  }

}
