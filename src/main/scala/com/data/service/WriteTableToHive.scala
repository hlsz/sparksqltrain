package com.data.service

import com.data.dao.ProductData
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

case class Datablock(ip: String, time:String, phoneNum:String)

class WriteTableToHive{

  def writeTableToHive(): Unit =
  {
    val spark = SparkSession
      .builder()
      .master("yarn-client")
      .appName("WriteTableToHive")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val schemaString = "ip time phoneNum"
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))

    val schema = StructType(fields)

    val datablockDS =
      Seq(List(Datablock("192.168.1.1","2019-01-01 12:00:00","18866669999"))).toDS()

    datablockDS.show()

    datablockDS.toDF().createOrReplaceTempView("dataBlock")

    spark.sql("select * from dataBlock")
      .write.mode("append")
      .saveAsTable("hadoop10.data_block")

  }

}

object WriteTableToHive {



  def main(args: Array[String]): Unit = {

    val wt = new WriteTableToHive().writeTableToHive()
    println(wt)

  }

}

