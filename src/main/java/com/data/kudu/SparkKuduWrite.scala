package com.data.kudu

import org.apache.kudu.spark.kudu._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession



object SparkKuduWrite {

  def main(args:Array[String]) {
    if(args.length < 2){
      println("Usage:SparkKuduWrite <data_path><kudu_table_name><kudu_master_hosts>")
      System.exit(1)
    }
    var data_path = args(0)
    var kudu_table_name = args(1)
    var kudu_master_hosts = args(2)

    println(data_path)
    println(kudu_table_name)
    println(kudu_master_hosts)

    var conf = new SparkConf().setAppName("stra_platform_test")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    val sc = spark.sparkContext

    val kuduContext = new KuduContext(kudu_master_hosts, sc)
    var df = spark.read.load(data_path)
//    # 通过kuduContext可以操作kudu的所有功能
    kuduContext.upsertRows(df, kudu_table_name)
  }

}
