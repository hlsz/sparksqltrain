package com.data.service

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ScalaSqlSimple {



  def main(args: Array[String]): Unit = {

    //          val properties = new Properties()
    //          properties.load(this.getClass.getResourceAsStream("/client.properties"))
    //          val configuration = ClientUtils.initConfiguration()
    //          ClientUtils.initKerberosENV(configuration, false, properties)

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("SparkSqlJob").setMaster("yarn-client").set("spark.shuffle.compress", "false")

    val spark: SparkSession =  SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("spark.sql.join.preferSortMergeJoin", true)


    spark.sql("use default")
    spark.sql("show tables").collect().foreach(println(_))

    import spark.implicits._

    val df = spark.sql("select * from amount_transaction limit 10000")
    df.printSchema()
    df.select($"c_custno").show()
    df.select($"c_custno", $"appro_months_amount", ($"appro_months_amount" + 1).alias("appro_months_amount2")).show()
    df.groupBy($"appro_months_amount").count().show()

    df.createOrReplaceTempView("amount_transaction")
    val sqlDF = spark.sql("select * from amount_transaction")
  //        sqlDF.show()
    import spark.implicits._
    println("sqlDF:")
  //        sqlDF.printSchema()
    println("sqlDF.c_custno:")
    sqlDF.select($"c_custno").show()
    println("sqlDF.c_custno, sqlDF.appro_months_amount:")
    sqlDF.select($"c_custno",($"appro_months_amount" + 1).alias("appro_months_amount2")).show()
    println("filter sqlDF.appro_months_amount > 1000000 :")
    sqlDF.filter($"appro_months_amount" > 1000000).show()
    println("groupby sqlDF.branch_no:")
    sqlDF.groupBy($"branch_no").count().show()

  }

}
