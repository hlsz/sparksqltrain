package com.data.utils

import org.apache.spark.sql.SparkSession

object Spark_Sql_Load_Data_To_Hive{
  val spark = SparkSession.builder().appName("project")
    .enableHiveSupport()
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {

    try{
      if (args.length != 1) {
        data_load(args(0).toInt)
      }else if (args.length != 2){
        for (i <- args(0).toInt to args(1).toInt){
          data_load(i)
        }
      } else{
        System.err.println("Usage:<start_time> or <start_time> <end_time>")
        System.exit(1)
      }
    }catch {
      case ex:Exception => println("Exceptioin")
    }finally {
      spark.stop()
    }
  }

  def data_load(i:Int): Unit ={
    println(s"********data_${i}************")

    val data = spark.read.option("inferSchema","true").option("header","false")
      //设置是否处理头信息，false代表不处理
      .csv(s"file:///home/spark/file/project/${i}visit.txt")  // ""前的 s 不能少,引用变量
      .toDF("mac","phone_brand","enter_time","first_time","last_time","region","screen","stay_time") //将读取的数据转换为df,并设置字段名

    data.createTempView(s"table_${i}")

    spark.sql("use project_1".stripMargin)

    spark.sql(
      s"""
         |create external table if not exists ${i}visit
         |mac string, phone_brand string, enter_time timestamp, first_time timestamp, last_time timestamp,
         | region strring, screen string, stay_time int) stored as parquet
         | location "hdfs://master:9000/project_dest/${i}"
       """.stripMargin)
    //（1）对于row format delimited fields terminated by '\t'这语句只支持存储文件格式为textFile,对于parquet文件格式不支持
    //（2）对于location这里，一定要写hdfs的全路径，如果向上面这样写，系统不认识，切记

    spark.sql(s"insert overwrite table ${i}visit select * from table_${i}".stripMargin)

  }


}
