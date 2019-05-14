package com.data.etl

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object jdbc {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir","d:/hadoop")
    val conf = new SparkConf().setAppName("Mysql")
      .setMaster("local")
      .set("spark.executor.memory","1G")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .setJars(Array("hdfs://spark-jars/ojdbc14.jar",
      "hdfs://spark-jars/mysql-connector-java.5.1.39.jar"))

    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val mysql = sqlContext.read.format("jdbc")
      .option("url","jdbc:mysql://192.168.66.66:3306/test")
      .option("user","root")
      .option("password","admin")
      .load()

    val mysql2 = sqlContext.read.format("jdbc")
      .options(Map(
        "driver" -> "com.mysql.jdbc.Driver",
        "url" -> "jdbc:mysql://192.168.66.66:3306",
        "dbtable" -> "test.student",
        "user" -> "root",
        "password" -> "admin",
        "fetchsize" -> "3")).load()

    mysql2.show()
    mysql.registerTempTable("student")
    mysql.sqlContext.sql("select * from student")
      .collect()
      .foreach(println)


    //oracle
    val oracle = sqlContext.read.format("jdbc")
      .options(
        Map(
          "driver" -> "oracle.jdbc.driver.OracleDriver",
          "url" -> "jdbc:oracle:thin:@11.11.11.11:1521:BIGDATA",
          "dbtable" -> "tag",
          "user" -> "lx",
          "password" -> "addd",
          "fetchsize" -> "3")).load()
    oracle.show()
    oracle.registerTempTable("tag")
    oracle.sqlContext.sql("select * from tag limit 10")
      .collect()
      .foreach(println)
  }

}
