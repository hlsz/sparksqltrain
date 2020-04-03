package com.data.spark.sql

import org.apache.spark.sql.SparkSession

object ReadOracle {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("ReadOracle")
      .master("local[2]")
      .getOrCreate()


    val oracleDriverUrl = "jdbc:oracle:thin:@//192.168.137.251:1521/devdb"
    val jdbcMap1 = Map("url" -> oracleDriverUrl,
      "user" -> "scott",
      "password" -> "tiger",
      "dbtable" -> "emp",
      "driver" -> "oracle.jdbc.driver.OracleDriver")


    val jdbcMap2 = Map("url" -> oracleDriverUrl,
      "user" -> "scott",
      "password" -> "tiger",
      "dbtable" -> "dept",
      "driver" -> "oracle.jdbc.driver.OracleDriver")

    val jdbcDF1 = spark.read.options(jdbcMap1).format("jdbc").load.createOrReplaceTempView("emp")
    val jdbcDF2 = spark.read.options(jdbcMap2).format("jdbc").load.createOrReplaceTempView("dept")
    spark.sql(" select * from dept  d join emp e on e.deptno = d.deptno where e.sal > 2000").show()
    spark.stop()

  }
}
