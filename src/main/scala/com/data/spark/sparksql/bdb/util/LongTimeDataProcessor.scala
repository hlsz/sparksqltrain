/**
  * 通联数据机密
  * --------------------------------------------------------------------
  * 通联数据股份公司版权所有 © 2013-2020
  *
  * 注意：本文所载所有信息均属于通联数据股份公司资产。本文所包含的知识和技术概念均属于
  * 通联数据产权，并可能由中国、美国和其他国家专利或申请中的专利所覆盖，并受商业秘密或
  * 版权法保护。
  * 除非事先获得通联数据股份公司书面许可，严禁传播文中信息或复制本材料。
  *
  * DataYes CONFIDENTIAL
  * --------------------------------------------------------------------
  * Copyright © 2013-2020 DataYes, All Rights Reserved.
  *
  * NOTICE: All information contained herein is the property of DataYes
  * Incorporated. The intellectual and technical concepts contained herein are
  * proprietary to DataYes Incorporated, and may be covered by China, U.S. and
  * Other Countries Patents, patents in process, and are protected by trade
  * secret or copyright law.
  * Dissemination of this information or reproduction of this material is
  * strictly forbidden unless prior written permission is obtained from DataYes.
  */

package com.data.spark.sparksql.bdb.util

/**
  * Created by liang.zhao on 2016/12/28.
  */


import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.joda.time.DateTime




object LongTimeDataProcessor {

  var startDate :String = null
  var endDate :String = null

  var conf: SparkConf = null
  var session: SQLContext = null

  var otcHoldDF : DataFrame = null
  var holdDF : DataFrame = null

  def setConf(selfConf: SparkConf, sessionInit: SQLContext): Unit = {
    conf = selfConf
    session = sessionInit
  }

  def readData(fileName: String): DataFrame = {
    var loadPath = conf.get("spark.data." + fileName)
    var df:DataFrame = null
    if(DataProcessor.customerFiles.contains(fileName) || DataProcessor.noDataFile.contains(fileName)) {
      val schemaString = conf.get("spark.schema."+fileName)

      var fileSchema = new StructType()
      for (column <- schemaString.split(" ")) {
        val items = column.split(":")
        val df = items(2) match {
          case "string" => StringType
          case "String" => StringType
          case "number" => FloatType
          case "double" => DoubleType
          case "Double" => DoubleType
          case "int" => IntegerType
          case "Date" => DateType
          case _ => NullType
        }
        val nullable = if(items(3).equalsIgnoreCase("true")) true else false
        fileSchema = fileSchema.add(items(1), df, nullable)
      }

      if (!DataProcessor.noDataFile.contains(fileName)) {
        // 认为cliennt和clientinfo两张表不需要处理成日期格式的,直接根据path读取就可以
        loadPath = buildLoadPath(loadPath)
      }

      df = session.read.format("com.databricks.spark.csv")
        .option("header", "false")
        .option("delimiter","\u0001")

        .option("quote","\"")
        .option("nullValue","null")
        .option("mode", "DROPMALFORMED")
        .schema(fileSchema)
        .load(loadPath)
    }else {
      df = session.read.parquet(loadPath)
    }
    df
  }


  def buildLoadPath (loadPath : String):String = {
    val begin = Integer.parseInt(startDate.substring(0, 4))
    val end = DateTime.now.getYear

    val resultPath = loadPath + "/{" + (begin to end).map(_.toString).mkString("*,") + "*}/"
    resultPath
  }



  def loadData(session: HiveContext, mode: Int = 0, startDate: String = "20100101", endDate: String): Unit = {
    this.startDate = startDate
    this.endDate = endDate

    otcHoldDF = readData("otcFundHold")
      .repartition(col("userID"))





  }


}