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

package com.data.spark.sparksql.bdb.analysis

import com.data.spark.sparksql.bdb.util.{DataProcessor, FileOperator}
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, _}
import org.apache.spark.sql.{SQLContext}
import org.joda.time.{DateTime, Months}
import org.joda.time.format.DateTimeFormat

class UserAnalyzer(session: SQLContext) {
  val logger = Logger.getLogger(getClass.getName)
  import session.implicits._

  def analyse(): Unit = {
    val accountDF = DataProcessor.accountState
    val tradeDF = DataProcessor.tradeDF.filter(col("tradeVal")>0)
      .filter(col("date")>=DataProcessor.startDate && col("date")<=DataProcessor.endDate)


    if (!FileOperator.checkExist("accountInfo/_SUCCESS")) {
      val stockAgeFunc = udf { dateStr: String =>
        try {
          (Months.monthsBetween(DateTimeFormat.forPattern("yyyyMMdd").parseDateTime(dateStr), DateTime.now).getMonths() / 12.0).formatted("%.2f")
        } catch {
          case e: Exception => (Months.monthsBetween(DateTimeFormat.forPattern("yyyyMMdd").parseDateTime("29990101"), DateTime.now).getMonths() / 12.0).formatted("%.2f")
        }
      }
      val userStockAgeDF = accountDF
        .withColumn("stockAge", stockAgeFunc(col("openDate")))
        .groupBy("userID").agg(round(max("stockAge"), 4).alias("stockAge"))

      DataProcessor.dumpResult("accountInfo", userStockAgeDF)
    }
    /*-------计算用户选择偏好------------*/
    if (!FileOperator.checkExist("userPrefer/_SUCCESS")) {
      val columnNameF = Seq("pe", "pb", "roe", "rsi")
      val columnNameN = Seq("price")
      val columnNames = columnNameF ++ columnNameN
      var merged = tradeDF.join(DataProcessor.factorDF, Seq("stockID", "date"), "left_outer")
        .withColumn("allValue", sum("tradeVal").over(Window.partitionBy("userID")))
        .withColumn("weight", col("tradeVal") / col("allValue"))
        .withColumn("flag", when((col("pe") > 0 && col("pb") > 0 && col("pe") < 500 && col("pb") < 12), 1).otherwise(0))
        .withColumn("tradeValFilter", col("tradeVal") * col("flag"))
        .withColumn("allValFilter", sum("tradeValFilter").over(Window.partitionBy("userID")))
        .withColumn("weightFilter", col("tradeValFilter") / col("allValFilter"))
      columnNameF.map((x: String) => merged = merged.withColumn(x, col(x) * col("weightFilter")))
      columnNameN.map((x: String) => merged = merged.withColumn(x, col(x) * col("weight")))
      val aggSumCols = columnNames.map(
        colName => {
          round(sum(colName), 2).as(colName)
        }
      ).toList :+ count("*").alias("cnt")

      val aggCols = aggSumCols
      val preferFiltered = merged.groupBy("userID").agg(aggCols.head, aggCols.tail: _*).filter(col("cnt") > 2).drop("cnt")
      DataProcessor.dumpResult("userPrefer", preferFiltered)
    }

    /*-------计算用户风格------------*/
    val factorDF = tradeDF.join(DataProcessor.riskDF, Seq("stockID", "date"), "left_outer")
    if (!FileOperator.checkExist("userStyleLongTerm/_SUCCESS")) {
      val columnNames = Seq("beta", "momentum", "size", "earnyild", "resvol", "growth", "btop", "leverage", "liquidity", "sizenl")
      var vdf = factorDF
        .withColumn("sumVal", sum("tradeVal").over(Window.partitionBy("userID")))
        .withColumn("weight", col("tradeVal") / col("sumVal")).drop("tradeVal").drop("sumVal").drop("date")
      val aggSumCols = columnNames.map(
        colName => round(sum(colName), 4).as(colName)
      ).toList
      columnNames.map((x: String) => vdf = vdf.withColumn(x, col(x) * col("weight")))

      val aggCols = aggSumCols
      val styledf = vdf.groupBy("userID").agg(aggCols.head, aggCols.tail: _*)
      DataProcessor.dumpResult("userStyleLongTerm", styledf)
    }


    /*-------计算用户行业偏好------------*/
    if (!FileOperator.checkExist("userIndustryPrefer/_SUCCESS")) {
      val userIndustryPrefer = tradeDF.join(DataProcessor.industryDF, "stockID").groupBy("userID", "industryName1ST")
        .agg(sum("tradeVal").alias("tradeVal"))
        .withColumn("totalVal", sum("tradeVal").over(Window.partitionBy("userID")))
        .withColumn("weight", round($"tradeVal" / $"totalVal", 3)).select("userID", "industryName1ST", "weight")
        .withColumn("rank", row_number().over(Window.partitionBy("userID").orderBy(col("weight").desc)))
        .filter($"rank" <= DataProcessor.keepNum)
        .groupBy("userID").agg(collect_list("industryName1ST").alias("industryIDs"), collect_list("weight").alias("weights"))
        .withColumn("industryIDs", DataProcessor.joinStrings(col("industryIDs")))
        .withColumn("weights", DataProcessor.joinDoubles(col("weights")))
        .select("userID","industryIDs","weights")
      DataProcessor.dumpResult("userIndustryPrefer", userIndustryPrefer)
    }

  }
}

