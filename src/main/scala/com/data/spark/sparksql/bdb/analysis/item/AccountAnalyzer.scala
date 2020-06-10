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

package com.data.spark.sparksql.bdb.analysis.item

import com.data.spark.sparksql.bdb.util.{DataProcessor, FileOperator}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.joda.time.{DateTime, Months}
import org.joda.time.format.DateTimeFormat

import scala.collection.mutable


class AccountAnalyzer(session: SQLContext) {
  val logger = Logger.getLogger(getClass.getName)

  val getMonthBetweenFunc = udf { dateStr: String =>
    try {
      Months.monthsBetween(DateTimeFormat.forPattern("yyyyMMdd").parseDateTime(dateStr), DateTime.now).getMonths()
    } catch {
      case e: Exception => Months.monthsBetween(DateTimeFormat.forPattern("yyyyMMdd").parseDateTime("29990101"), DateTime.now).getMonths()
    }
  }


  def analyse(): Unit = {

    val fundaccount : DataFrame = DataProcessor.readData("fundAccount")
      .select("userID","CANCEL_DATE","ASSET_PROP","OPEN_DATE","FUNDACCT_STATUS","FUND_ACCOUNT")
      .repartition(col("userID"))

    val cbsAccount = DataProcessor.readData("cbsStockHolder")
      .select("userID","OPEN_DATE","EXCHANGE_TYPE")
      .repartition(col("userID"))

    val stockHolder = DataProcessor.readData("stockHolder")
      .select("userID","OPEN_DATE","HOLDER_RIGHTS","EXCHANGE_TYPE")
      .repartition(col("userID"))

    val optStockHolder = DataProcessor.readData("optStockHolder")
      .select("userID","OPEN_DATE")
      .repartition(col("userID"))

    val secumHolder = DataProcessor.readData("secumHolder")
      .select("userID","OPEN_DATE")
      .repartition(col("userID"))

    val bankmholder = DataProcessor.readData("bankmHolder")
      .select("userID","OPEN_DATE")
      .repartition(col("userID"))


    // 融资融券是否开通和开通时间
    val margin = fundaccount
      .filter(col("ASSET_PROP") === "7")
      .filter(col("FUNDACCT_STATUS") !== "3")
      .withColumn("marginOpen",lit(1))
      .withColumnRenamed("OPEN_DATE","marginOpenDate")
      .withColumn("rank",row_number().over(Window.partitionBy("userID").orderBy(col("marginOpenDate").asc)))
      .filter(col("rank") <= 1)
      .withColumn("marginOpenDate", getMonthBetweenFunc(col("marginOpenDate")))
      .select("userID","marginOpenDate","marginOpen")



    // 沪港通是否开通和开通时间
    val HGT = cbsAccount.filter(col("EXCHANGE_TYPE") === "G")
        .select("userID","OPEN_DATE")
        .withColumnRenamed("OPEN_DATE", "HGTOpenDate")
      .withColumn("HGTOpen", lit(1))
      .withColumn("rank",row_number().over(Window.partitionBy("userID").orderBy(col("HGTOpenDate").asc)))
      .filter(col("rank") <= 1)
      .withColumn("HGTOpenDate", getMonthBetweenFunc(col("HGTOpenDate")))
      .select("userID","HGTOpen","HGTOpenDate")

    // 申港通是否开通和开通时间
    val SGT = cbsAccount.filter(col("EXCHANGE_TYPE") === "S")
      .select("userID","OPEN_DATE")
      .withColumnRenamed("OPEN_DATE", "SGTOpenDate")
      .withColumn("SGTOpen", lit(1))
      .withColumn("rank",row_number().over(Window.partitionBy("userID").orderBy(col("SGTOpenDate").asc)))
      .filter(col("rank") <= 1)
      .withColumn("SGTOpenDate", getMonthBetweenFunc(col("SGTOpenDate")))
      .select("userID","SGTOpen","SGTOpenDate")

    // 创业板是否开通和开通时间
    val cyb = stockHolder.filter(col("HOLDER_RIGHTS").contains("j"))
      .withColumn("CYBOpen",lit(1))
      .withColumnRenamed("OPEN_DATE","CYBOpenDate")
      .withColumn("rank",row_number().over(Window.partitionBy("userID").orderBy(col("CYBOpenDate").asc)))
      .filter(col("rank") <= 1)
      .withColumn("CYBOpenDate", getMonthBetweenFunc(col("CYBOpenDate")))
      .select("userID","CYBOpen","CYBOpenDate")

    // 新三版是否开通和开通时间
    val xsb = stockHolder.filter(col("HOLDER_RIGHTS").contains("a") || col("HOLDER_RIGHTS").contains("b"))
      .filter(col("EXCHANGE_TYPE") === "9" || col("EXCHANGE_TYPE") === "A")
      .withColumn("XSBOpen",lit(1))
      .withColumnRenamed("OPEN_DATE","XSBOpenDate")
      .withColumn("rank",row_number().over(Window.partitionBy("userID").orderBy(col("XSBOpenDate").asc)))
      .filter(col("rank") <= 1)
      .withColumn("XSBOpenDate", getMonthBetweenFunc(col("XSBOpenDate")))
      .select("userID","XSBOpen","XSBOpenDate")

    // 期权是否开通和开通时间
    val opt = optStockHolder.select("userID","OPEN_DATE")
      .withColumnRenamed("OPEN_DATE","OPTOpenDate")
      .withColumn("OPTOpen",lit(1))
      .withColumn("rank",row_number().over(Window.partitionBy("userID").orderBy(col("OPTOpenDate").asc)))
      .filter(col("rank") <= 1)
      .withColumn("OPTOpenDate", getMonthBetweenFunc(col("OPTOpenDate")))
      .select("userID","OPTOpen","OPTOpenDate")

    val finance = secumHolder.unionAll(bankmholder)
      .groupBy("userID")
      .agg(min("OPEN_DATE").alias("finOpenDate"))
      .withColumn("finOpen",lit(1))
      .select("userID","finOpen","finOpenDate")

    if (!FileOperator.checkExist("accountOpen/_SUCCESS")) {
      val accountOpen =  margin.join(HGT, Seq("userID"), "outer")
        .join(SGT, Seq("userID"), "outer")
        .join(cyb, Seq("userID"), "outer")
        .join(xsb, Seq("userID"), "outer")
        .join(opt, Seq("userID"), "outer")
        .join(finance, Seq("userID"), "outer")
        .filter(col("userID") !== "CLIENT_ID")
        .select("userID","marginOpenDate","marginOpen","HGTOpen","HGTOpenDate","SGTOpen","SGTOpenDate",
          "CYBOpen","CYBOpenDate","XSBOpen","XSBOpenDate","OPTOpen","OPTOpenDate","finOpen","finOpenDate")
        .withColumn("marginOpen",when(col("marginOpen") === 1, col("marginOpen")).otherwise(0))
        .withColumn("HGTOpen",when(col("HGTOpen") === 1, col("HGTOpen")).otherwise(0))
        .withColumn("SGTOpen",when(col("SGTOpen") === 1, col("SGTOpen")).otherwise(0))
        .withColumn("CYBOpen",when(col("CYBOpen") === 1, col("CYBOpen")).otherwise(0))
        .withColumn("XSBOpen",when(col("XSBOpen") === 1, col("XSBOpen")).otherwise(0))
        .withColumn("OPTOpen",when(col("OPTOpen") === 1, col("OPTOpen")).otherwise(0))
        .withColumn("finOpen",when(col("finOpen") === 1, col("finOpen")).otherwise(0))

      DataProcessor.dumpResult("accountOpen", accountOpen)
    }


    if (!FileOperator.checkExist("questionnaire/_SUCCESS")) {

      val idyPreRecord = DataProcessor.readData("idyPreRecord")
          .withColumnRenamed("user_id","userID")
        .select("userID","paper_answer","insert_time")

      val questionnaire = idyPreRecord
        .withColumn("rank", row_number().over(Window.partitionBy("userID").orderBy(col("insert_time").desc)))
        .filter(col("rank") <= 1)
        .filter(col("paper_answer").isNotNull)
        .withColumn("tuple", analyseIdyPreRecord(col("paper_answer")))
        .withColumn("bearLoss",col("tuple").getItem("2"))
        .withColumn("preferDuration",col("tuple").getItem("1"))
        .withColumn("preferReturn",col("tuple").getItem("13"))
        .select("userID","bearLoss","preferDuration","preferReturn")

      DataProcessor.dumpResult("questionnaire", questionnaire)
    }

    if (!FileOperator.checkExist("tg/_SUCCESS")) {
      // 投顾指标
      val tgDate = DataProcessor.readData("tgTblCustomerConfig")
        .withColumnRenamed("CAPITAL_ACCOUNT","FUND_ACCOUNT")
        .withColumn("end_date",changeTimeFormatInTgTable(col("end_date")))
        .withColumn("create_date",changeTimeFormatInTgTable(col("create_date")))
        .join(fundaccount,Seq("FUND_ACCOUNT"),"inner")
        .filter(col("end_date") > DataProcessor.endDate)
        .withColumn("rank",row_number().over(Window.partitionBy("userID").orderBy(col("create_date").asc)))
        .filter(col("rank") <= 1)
        .withColumnRenamed("create_date","openDate")
        .withColumn("open",lit(1))
        .withColumn("openDate", getMonthBetweenFunc(col("openDate")))
        .select("userID","openDate","open")
      DataProcessor.dumpResult("tg", tgDate)
    }

  }

  val analyseIdyPreRecord = udf((records: String) => {
    val arrays = records.split("\\|")
    var result = mutable.Map[String,String]()

    for (record <- arrays) {
      val detailArray = record.split(":")
      if (detailArray.length >= 3) {
        val index = detailArray(0)
        val answer = detailArray(1).substring(0,1)
        result += (index->answer)
      }
    }
    result
  })

  val changeTimeFormatInTgTable = udf((time: String) => {
//    val arrays = time.split("/")
//    if (arrays.size != 3) {
//      null
//    } else {
//      val year :String = arrays(0)
//      var month : String = arrays(1)
//      var day : String = arrays(2)
//      if (month.length <= 1) {
//        month = "0" + month
//      }
//      if (day.length <= 1) {
//        day = "0" + day
//      }
//      year + month + day
//    }

       val arrays = time.split(" ")
        if (arrays.size != 2) {
          null
        } else {
          arrays(0).replace("-", "")
        }
  })
}
