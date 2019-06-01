package com.data.spark.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class CalcuteCustAge {

  val conf = new SparkConf()
    .setAppName("CalcuteCustAge")
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

  def calcuteCustAge(calYear:Int): Unit =
  {

    spark.sql("use bigdata")

    spark.sql(" create table if not exists bigdata.cust_age_tb " +
      " ( c_custno string, client_id int, age int, branch_no string, input_date int) " +
      s" ROW FORMAT DELIMITED FIELDS TERMINATED BY ${raw"'\t'"} " +

      s" LINES TERMINATED BY ${raw"'\n'"} " +
      " stored as textfile ")

    val custAgeDF = spark.sql(
      s"""
         | select
         |	  c_custno,
         |	  client_id,
         |	  branch_no,
         |	  case when birthday = 0 then -1
         |		   else
         |	 substr( s'${calYear}',0,4) - substr(birthday,0,4)   end age
         |	 ,${calYear} input_date
         | from bigdata.c_cust_branch_tb
       """.stripMargin)
    custAgeDF.createOrReplaceTempView("custAgeTmp")

    val countAllClientRDD  = spark.sql("select client_id from  bigdata.c_cust_branch_tb  ")
    val countAllClient = countAllClientRDD.count().toInt

    spark.sql("create table if not exists bigdata.result_custage " +
      " ( c_custno string, branch_no string, age int, super_comage double, super_branchage double, input_date int) " +
      s" ROW FORMAT DELIMITED FIELDS TERMINATED BY ${raw"'\t'"} " +

      s" LINES TERMINATED BY ${raw"'\n'"} " +
      " stored as textfile ")

    spark.sql(
      s"""
         | insert into bigdata.result_custage (
         |    c_custno
         |   ,branch_no
         |   ,age
         |   ,super_comage
         |   ,super_branchage
         |   ,input_date
         |  )
         |   select
         |   a.c_custno
         |   ,a.branch_no
         |   ,a.age
         |  , round((${countAllClient} -nvl(a.acomage_rank,${countAllClient}) ) * 100 /${countAllClient} ,4) super_comage
         |  , round((b.branchallcount -  nvl(a.abranchage_rank,b.branchallcount)) * 100 /b.branchallcount ,4) super_branchage
         |  ,${calYear} input_date
         |  from (
         |  select
         |   c_custno
         |   ,branch_no
         |   ,age
         |   ,dense_rank() over(order by age  desc)  acomage_rank
         |   ,dense_rank() over(partition by branch_no order by age desc ) abranchage_rank
         | from custAgeTmp where input_date = ${calYear} ) a
         | left join (select   count(*) branchallcount ,branch_no from  c_cust_branch_tb
         |   group by branch_no ) b  on a.branch_no =b.branch_no
       """.stripMargin)

    spark.sql("create table if not exists bigdata.result_branchage " +
      " (  branch_no string, avg_age int, med_age double,  input_date int) " +
      s" ROW FORMAT DELIMITED FIELDS avg_ageTERMINATED BY ${raw"'\t'"} " +

      s" LINES TERMINATED BY ${raw"'\n'"} " +
      " stored as textfile ")

    spark.sql(
      s"""
         | insert   into bigdata.result_branchage (
         | branch_no
         | ,avg_age
         | ,med_age
         | ,input_date
         | )
         | select  -1,round(avg(age),2) , round(median(age),2),${calYear}
         |  from  custAgeTmp
         | where input_date = ${calYear}
         | union
         | select branch_no,round(avg(age),2) , round(median(age),2),${calYear}
         |  from  custAgeTmp  where input_date = ${calYear} group by branch_no
       """.stripMargin)

    spark.stop()


  }

}

object CalcuteCustAge
{
  def main(args: Array[String]): Unit = {

    new CalcuteCustAge().calcuteCustAge(20190401)

  }
}
