package com.data.process

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class CalcuteCustInfo{

  val conf = new SparkConf()
    .setAppName("CalcuteCustInfo")
    .setMaster("yarn-client")

  val spark = SparkSession
    .builder()
    .config(conf)
    //解决DecimalType存储精度问题， parquet格式 spark和hive不统一
    .config("spark.sql.parquet.writeLegacyFormat", true)
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse/bigdata.db")
    //数据倾斜
//    .config("spark.sql.shuffle.partitions", 500)
    .enableHiveSupport()
    .getOrCreate()

  // 设置参数
  // hive > set  hive.exec.dynamic.partition.mode = nonstrict;
  // hive > set  hive.exec.dynamic.partition = true;
  spark.sql("set  hive.exec.dynamic.partition.mode = nonstrict")
  spark.sql("set  hive.exec.dynamic.partition = true")


  def calcuteCustInfo(endDate:Int): Unit =
  {

    spark.sql("use bigdata")

    val custInfoDF = spark.sql(
      s"""
         | select
         |	  c_custno,
         |	  client_id,
         |	  branch_no,
         |    open_date,
         |    birthday,
         |	  case when birthday = 0 then -1
         |		   else
         |	 substr( '${endDate}',0,4) - substr(birthday,0,4)   end age
         |	 ,${endDate} input_date
         | from bigdata.hs08_client_for_ai
       """.stripMargin.replace("\r\n"," "))
    custInfoDF.createOrReplaceTempView("custInfoTmp")

    spark.sql("drop table if exists bigdata.dm_cust_stat")
    spark.sql(
      s""" create table if not exists bigdata.dm_cust_stat
         | ( c_custno string
         | ,client_id int
         | ,branch_no string
         | ,age int
         | ,super_comage double
         | ,super_branchage double
         | ,all_avg_age double
         | ,all_med_age double
         | ,branch_avg_age double
         | ,branch_med_age double
         | ,input_date int)   PARTITIONED BY (dt int)
         | ROW FORMAT DELIMITED FIELDS TERMINATED BY ${raw"'\t'"}
         | LINES TERMINATED BY ${raw"'\n'"}
         | stored as textfile """.stripMargin.replace("\r\n"," "))

    val dmCustStatDF = spark.sql(
      s"""
         | select
         |   a.c_custno
         |   ,a.client_id
         |   ,a.branch_no
         |   ,a.age
         |   ,round((all_client_cnt - nvl(a.all_age_rank, all_client_cnt) ) * 100 /all_client_cnt , 2) super_comage
         |   ,round((a.branch_client_cnt - nvl(a.branch_age_rank, a.branch_client_cnt)) * 100 /a.branch_client_cnt , 2) super_branchage
         |   ,round(all_avg_age,2) all_avg_age
         |   ,round(all_med_age,2) all_med_age
         |   ,round(branch_avg_age,2) branch_avg_age
         |   ,round(branch_med_age,2) branch_med_age
         |   ,${endDate} input_date
         |  from (
         |    select
         |    c_custno
         |    ,client_id
         |   ,branch_no
         |   ,age
         |   ,dense_rank() over(order by age  desc)  all_age_rank
         |   ,dense_rank() over(partition by branch_no order by age desc ) branch_age_rank
         |   ,count(*) over () all_client_cnt
         |   ,count(*) over (partition by branch_no) branch_client_cnt
         |   ,avg(age) over()  all_avg_age
         |   ,percentile_approx(age, 0.5) over() all_med_age
         |   ,avg(age) over (partition by branch_no)  branch_avg_age
         |   ,percentile_approx(age, 0.5) over (partition by branch_no ) branch_med_age
         | from custInfoTmp where input_date = ${endDate} ) a
       """.stripMargin.stripMargin.replace("\r\n"," "))
    dmCustStatDF.createOrReplaceTempView("dmCustStatTmp")

    // dmCustStatDF.queryExecution
    // Parsed Logical Plan
    // Analyzed Logical Plan
    // Optimized Logical Plan
    // dmCustStatDF.queryExecution.sparkPlan
    // Physical Plan



    //删除临时表
//    spark.catalog.dropTempView("dmCustStatTmp")

//    dmCustStatDF.toDF().write.mode(SaveMode.Overwrite).format("hive").saveAsTable("dm_cust_stat_tmp")

    spark.sql(
      s"""
         | insert overwrite table  bigdata.dm_cust_stat partition(dt=${endDate})
         |   select
         |    c_custno
         |   ,client_id
         |   ,branch_no
         |   ,age
         |   ,super_comage
         |   ,super_branchage
         |   ,all_avg_age
         |   ,all_med_age
         |   ,branch_avg_age
         |   ,branch_med_age
         |   ,input_date
         |   from  dmCustStatTmp
       """.stripMargin.replace("\r\n"," "))

    spark.sql("drop table if exists bigdata.dm_branch_stat")
    spark.sql(
      s""" create table if not exists bigdata.dm_branch_stat
         | ( branch_no string
         | ,avg_age double
         | ,med_age double
         | ,input_date int)   PARTITIONED BY (dt int)
         | ROW FORMAT DELIMITED FIELDS TERMINATED BY ${raw"'\t'"}
         | LINES TERMINATED BY ${raw"'\n'"}
         | stored as textfile """.stripMargin.replace("\r\n"," "))

    spark.sql(
      s"""
         |  insert overwrite  table  bigdata.dm_branch_stat partition(dt=${endDate})
         |   select
         |   case when branch_no is null then -1 else branch_no end branch_no
         |   ,avg_age
         |   ,med_age
         |   ,${endDate}  input_date
         |   from (
         |   select
         |     branch_no
         |     ,max(branch_avg_age)  avg_age
         |     ,max(branch_med_age)  med_age
         |   from  dmCustStatTmp
         |   group by branch_no,1 grouping sets(branch_no,1)
         |   )
       """.stripMargin.stripMargin.replace("\r\n"," "))


    spark.stop()

  }

}

object CalcuteCustInfo
{
  def main(args: Array[String]): Unit = {

    new CalcuteCustInfo().calcuteCustInfo(20190401)

  }
}
