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

import com.data.spark.sparksql.bdb.util.{FileOperator, DataProcessor}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SQLContext

/**
  * Created by hongnan.li on 2018/3/14.
  */
class HoldAnalyzer(session: SQLContext) {
  def analyse(): Unit = {

    val currentHoldDF : DataFrame = DataProcessor.stockHoldDF.filter(col("date") === DataProcessor.lastDay)
      .cache()


    if (!FileOperator.checkExist("holdStockIds/_SUCCESS")) {
      val keep : Integer = 20
      val holdStocksA = currentHoldDF
        .filter(col("exchangeCD") === "1" || col("exchangeCD") === "2")
        .groupBy("userID","stockID").agg(sum("mktVal").alias("mktVal"))
        .withColumn("rank",row_number().over(Window.partitionBy("userID").orderBy(col("mktVal").desc)))
        .filter(col("rank") <= keep)
        .groupBy("userID")
        .agg(collect_list("stockID").alias("stockIDsA"))
        .withColumn("A", DataProcessor.joinStrings(col("stockIDsA")))
        .select("userID","A")

      val holdStocksB = currentHoldDF
        .filter(col("exchangeCD") === "D" || col("exchangeCD") === "H")
        .groupBy("userID","stockID").agg(sum("mktVal").alias("mktVal"))
        .withColumn("rank",row_number().over(Window.partitionBy("userID").orderBy(col("mktVal").desc)))
        .filter(col("rank") <= keep)
        .groupBy("userID")
        .agg(collect_list("stockID").alias("stockIDsB"))
        .withColumn("B", DataProcessor.joinStrings(col("stockIDsB")))
        .select("userID","B")

      val holdStocksHK = currentHoldDF
        .filter(col("exchangeCD") === "G")
        .groupBy("userID","stockID").agg(sum("mktVal").alias("mktVal"))
        .withColumn("rank",row_number().over(Window.partitionBy("userID").orderBy(col("mktVal").desc)))
        .filter(col("rank") <= keep)
        .groupBy("userID")
        .agg(collect_list("stockID").alias("stockIDsHK"))
        .withColumn("HK", DataProcessor.joinStrings(col("stockIDsHK")))
        .select("userID","HK")

      val holdStockIds = holdStocksA
        .join(holdStocksB, Seq("userID"), "outer")
        .join(holdStocksHK, Seq("userID"), "outer")
        .select("userID","A","B","HK")

      DataProcessor.dumpResult("holdStockIds", holdStockIds)
    }

    /*-------计算用户当前持仓股票，重仓行业-----------*/
    val mdf1 = currentHoldDF.join(DataProcessor.industryDF, "stockID")
    if (!FileOperator.checkExist("holdIndustries/_SUCCESS")) {
      val userDF2 = mdf1.groupBy("userID","industryName1ST").agg(sum("stockMktVal").alias("industryVal"))
        .select("userID","industryName1ST","industryVal")
        .withColumn("rank",row_number().over(Window.partitionBy("userID").orderBy(col("industryVal").desc)))
        .filter(col("rank") <= DataProcessor.keepNum)
        .groupBy("userID").agg(collect_list("industryName1ST").alias("industryName1STs"))

        .withColumn("industryName1STs", DataProcessor.joinStrings(col("industryName1STs")))
        .select("userID", "industryName1STs")
      DataProcessor.dumpResult("holdIndustries", userDF2)
    }

    /*-------计算用户当前持仓股票，重仓主题-----------*/
    if (!FileOperator.checkExist("holdTheme/_SUCCESS")) {
      val userHoldThemeDF2 = currentHoldDF
        .join(DataProcessor.themeSecRel.select("theme_name","stockID"), Seq("stockId"), "left")
        .groupBy("userID","theme_name").agg(sum("stockMktVal").alias("themeValue"))
        .withColumn("rank",row_number().over(Window.partitionBy("userID").orderBy(col("themeValue").desc)))
        .filter(col("rank") <= DataProcessor.keepNum)
        .groupBy("userID").agg(collect_list("theme_name").alias("themeNames"))
        .withColumn("themeNames", DataProcessor.joinStrings(col("themeNames")))
        .select("userID","themeNames")
      DataProcessor.dumpResult("holdTheme", userHoldThemeDF2)
    }

    if (!FileOperator.checkExist("holdStockStyle/_SUCCESS")) {
      val stockInvestStyle = DataProcessor.readData("equInvestStyle")
        .withColumnRenamed("ticker_symbol","stockID")
        .select("stockID","vertical","horizontal")

      val holdStockStyle = currentHoldDF.join(stockInvestStyle, Seq("stockID"),"inner")
        .withColumn("style",DataProcessor.stringAddWithBlank(col("vertical"),col("horizontal")))
        .groupBy("userID","style")
        .agg(sum("mktVal").alias("typeVal"))
        .withColumn("rank", row_number().over(Window.partitionBy("userID").orderBy(col("typeVal").desc)))
        .filter(col("rank") <= 1)
        .select("userID","style")

      DataProcessor.dumpResult("holdStockStyle", holdStockStyle)
    }

    DataProcessor.releaseCache(currentHoldDF)
  }
}
