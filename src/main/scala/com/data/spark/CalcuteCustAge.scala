package scala.com.data.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class CalcuteCustAge {

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

  def calcuteCustAge(calYear:Int): Unit =
  {

    spark.sql("use bigdata")

    val custAgeDF = spark.sql(
      s"""
         | select
         |	  concat('c',client_id) c_custno,
         |	  client_id,
         |	  branch_no,
         |	  case when birthday = 0 then -1
         |		   else
         |	 substr( s'${calYear}',1,4)  - substr(birthday,1,4)  end age
         |	 ,${calYear} input_date
         | from global_temp.c_cust_branch_tb
       """.stripMargin)
    custAgeDF.createOrReplaceTempView("custAgeTmp")

    val countAllClientDF  = spark.sql("select count(1) cnt from  global_temp.c_cust_branch_tb  ").collect()
    val countAllClient = countAllClientDF.map(x => x(0))

    spark.sql(
      s"""
         | insert into
         |  result_custage(
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
         |  , round((${countAllClient} -nvl(a.acomage_rank,${countAllClient}) ) * 100 /${countAllClient} ,4)
         |  , round((b.branchallcount -  nvl(a.abranchage_rank,b.branchallcount)) * 100 /b.branchallcount ,4)
         |  ,${calYear} input_date
         |  from (
         |  select
         |   c_custno
         |   ,branch_no
         |   ,age
         |   ,dense_rank() over(order by age  desc)  acomage_rank
         |   ,dense_rank() over(partition by age order by age desc ) abranchage_rank
         | from custAgeTmp where input_date = ${calYear} ) a
         | left join(select   count(*) branchallcount ,branch_no from  global_temp.c_cust_branch_tb
         |  where cancel_date=0 group by branch_no )b  on a.branch_no =b.branch_no
       """.stripMargin)

    spark.sql(
      s"""
         | insert   into result_branchage(
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
