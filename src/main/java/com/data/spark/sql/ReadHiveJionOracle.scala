package com.data.spark.sql

import java.io.File

import org.apache.spark.sql.SparkSession

/**
  * Created by 92421 on 2018/4/5.
  */
object ReadHiveJionOracle {

  def main(args: Array[String]): Unit = {

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    val format = "yyyy-MM-dd'T'HH:mm:ssz"
    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("ReadHiveJionOracle")
      .config("spark.sql.warehouse.dir",warehouseLocation).enableHiveSupport().getOrCreate()


    val hiveDF =spark.table("default.emp").createOrReplaceTempView("emp")


    val oracleDriverUrl = "jdbc:oracle:thin:@//192.168.137.251:1521/devdb"
    val oracleDF = Map("url" -> oracleDriverUrl,
      "user" -> "scott",
      "password" -> "tiger",
      "dbtable" -> "dept",
      "driver" -> "oracle.jdbc.driver.OracleDriver")

    val jdbcDF1 = spark.read.options(oracleDF).format("jdbc").load.createOrReplaceTempView("dept")

    val resultDF=spark.sql("select * from emp e join  dept d on e.deptno = d.deptno " +
      "where e.comm is not null"
    ).show()


    spark.stop()


  }

}