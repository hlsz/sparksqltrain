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
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

/**
 * Created by liang.zhao on 2017/1/4.
 */
class StrategyAnalyzer(session: SQLContext) {
  val logger = Logger.getLogger(getClass.getName)

  val timeFmt = "yyyyMMdd"

  def analyse(): Unit = {
    val getYear = udf((x: String) => {
      try {
        x.substring(0, 4)
      } catch {
        case e: Exception => "1990"
      }
    })
    //　只关心买入
    val strategyDF = DataProcessor.tradeDF.filter(col("tradeVal") > 0)
      .filter(col("date")>=DataProcessor.startDate && col("date")<=DataProcessor.endDate)
      .withColumn("year", getYear(col("date")))
    //limit strategy
    val sign: (Float => Float) = (i: Float) => {
      if (1 / i == Float.PositiveInfinity) 1 else -1
    }
    val signFunc = udf(sign)
    if (!FileOperator.checkExist("limitStrategy/_SUCCESS")) {
      // 涨跌停策略
      val limitStrategy = strategyDF
        .join(DataProcessor.limitPriceDF, strategyDF("stockID") === DataProcessor.limitPriceDF("stockID")
          && strategyDF("date") === DataProcessor.limitPriceDF("date"))
        .drop(strategyDF("date")).drop(strategyDF("stockID"))
        .withColumn("limited", (col("price") - col("limitUpPrice")) * (col("price") - col("limitDownPrice")))
        .withColumn("tradeWay", col("num") / abs(col("num")))
        // 这里区分涨跌停,目前没有用到
        .withColumn("limitedWay", signFunc(col("limited")))
        .withColumn("strategyTradeVal", when(col("tradeWay") === 1 && col("limited") === 0, col("tradeVal")).otherwise(0))
        .withColumn("strategyTradeFlag", when(col("tradeWay") === 1 && col("limited") === 0, 1).otherwise(0))
        .groupBy("userID").agg(round(sum("strategyTradeVal") / sum("tradeVal"), 4).alias("limitValRatio"),
        round(sum("strategyTradeFlag") / count("*"), 4).alias("limitCountRatio"),
        round(sum("strategyTradeVal"),2).alias("strategyTradeValue"),
        sum("strategyTradeFlag").alias("strategyTradeNum")).na.fill(0)
        .withColumn("strategyTradeValue",when(col("strategyTradeValue") >= 1e13, lit(1e13 - 1)).otherwise(col("strategyTradeValue")))
        .select("userID","limitValRatio","limitCountRatio","strategyTradeValue","strategyTradeNum")
      DataProcessor.dumpResult("limitStrategy", limitStrategy)

    }


    val mrDF = strategyDF.join(DataProcessor.factorDF.select("stockID", "date", "rsi"), Seq("stockID", "date"), "left_outer")

    // 反转策略
    if (!FileOperator.checkExist("reverseStrategy/_SUCCESS")) {
      val reverseStrategy = mrDF
        .withColumn("reverseTradeVal", when(col("rsi") <= 20, col("tradeVal")).otherwise(0))
        .withColumn("reverseTradeFlag", when(col("rsi") <= 20, 1).otherwise(0))
        .groupBy("userID")
        .agg(round(sum("reverseTradeVal") / sum("tradeVal"), 4).alias("valRatio"),
          round(sum("reverseTradeFlag") / count("*"), 4).alias("countRatio"),
          round(sum("reverseTradeVal"),2).alias("strategyTradeValue"),
          sum("reverseTradeFlag").alias("strategyTradeNum"))
        .na.fill(0)
        .withColumn("strategyTradeValue",when(col("strategyTradeValue") >= 1e13, lit(1e13 - 1)).otherwise(col("strategyTradeValue")))
        .select("userID","valRatio","countRatio","strategyTradeValue","strategyTradeNum")
      DataProcessor.dumpResult("reverseStrategy", reverseStrategy)

    }


    if (!FileOperator.checkExist("momentumStrategy/_SUCCESS")) {
      // 动量策略
      val momentumStrategy = mrDF
        .withColumn("momentumTradeVal", when(col("rsi") >= 70, col("tradeVal")).otherwise(0))
        .withColumn("momentumTradeFlag", when(col("rsi") >= 70, 1).otherwise(0))
        .groupBy("userID")
        .agg(round(sum("momentumTradeVal") / sum("tradeVal"), 4).alias("valRatio"),
          round(sum("momentumTradeFlag") / count("*"), 4).alias("countRatio"),
          round(sum("momentumTradeVal"),2).alias("strategyTradeValue"),
          sum("momentumTradeFlag").alias("strategyTradeNum"))
        .na.fill(0)
        .withColumn("strategyTradeValue",when(col("strategyTradeValue") >= 1e13, lit(1e13- 1)).otherwise(col("strategyTradeValue")))
        .select("userID","valRatio","countRatio","strategyTradeValue","strategyTradeNum")
      DataProcessor.dumpResult("momentumStrategy", momentumStrategy)

    }


    //new stock strategy
    if (!FileOperator.checkExist("newStockStrategy/_SUCCESS")) {
      val newStockStrategy = DataProcessor.stockIPODF.join(strategyDF, DataProcessor.stockIPODF("stockID") === strategyDF("stockID"), "rightouter")
        .drop(DataProcessor.stockIPODF("stockID")).filter(col("num") > 0)
        .withColumn("lag", (unix_timestamp(col("date"), timeFmt) - unix_timestamp(col("listDate"), timeFmt)) / 86400)
        .withColumn("stage", ceil(col("lag") / 50))
        .withColumn("strategyTradeVal", when(col("stage") <= 4, col("tradeVal")).otherwise(0))
        .withColumn("strategyTradeFlag", when(col("stage") <= 4, 1).otherwise(0))
        .groupBy("userID").agg(round(sum("strategyTradeVal") / sum("tradeVal"), 4).alias("newStockValRatio"),
        round(sum("strategyTradeFlag") / count("*"), 4).alias("newStockCountRatio"),
        round(sum("strategyTradeVal"),2).alias("strategyTradeValue"),
        sum("strategyTradeFlag").alias("strategyTradeNum")).na.fill(0)
        .withColumn("strategyTradeValue",when(col("strategyTradeValue") >= 1e13, lit(1e13 - 1)).otherwise(col("strategyTradeValue")))
        .select("userID","newStockValRatio","newStockCountRatio","strategyTradeValue","strategyTradeNum")
      DataProcessor.dumpResult("newStockStrategy", newStockStrategy)

    }

    //announcement strategy
    if (!FileOperator.checkExist("announcementStrategy/_SUCCESS")) {
      val announcementDF = DataProcessor.announcementDF
      val announcementStrategy = announcementDF.join(strategyDF, announcementDF("stockID") === strategyDF("stockID")
        && announcementDF("publishDate") === strategyDF("date"), "rightouter").drop(strategyDF("stockID"))
        .withColumn("announcementTradeVal", when(col("publishDate").isNotNull, col("tradeVal")).otherwise(0))
        .withColumn("announcementTradeFlag", when(col("publishDate").isNotNull, 1).otherwise(0))
        .groupBy("userID").agg(round(sum("announcementTradeVal") / sum("tradeVal"), 4).alias("announcementValRatio"),
        round(sum("announcementTradeFlag") / count("*"), 4).alias("announcementCountRatio"),
        round(sum("announcementTradeVal"),2).alias("strategyTradeValue"),
        sum("announcementTradeFlag").alias("strategyTradeNum")).na.fill(0)
        .withColumn("strategyTradeValue",when(col("strategyTradeValue") >= 1e13, lit(1e13 - 1)).otherwise(col("strategyTradeValue")))
        .select("userID","announcementValRatio","announcementCountRatio","strategyTradeValue","strategyTradeNum")
      DataProcessor.dumpResult("announcementStrategy", announcementStrategy)

    }

    // calc cash flow strategy
    if (!FileOperator.checkExist("cashFlowStrategy/_SUCCESS")) {
      val mktRankListStocksDF = DataProcessor.billboardDF
        .groupBy("stockID", "tradeDate").agg(count("*"))
        .withColumn("lagDay1", date_format((unix_timestamp(col("tradeDate"), timeFmt) + 86400).cast("timestamp"), timeFmt))
      val cashFlowStrategyDF = mktRankListStocksDF.join(strategyDF, mktRankListStocksDF("stockID") === strategyDF("stockID")
        && mktRankListStocksDF("lagDay1") === strategyDF("date"), "rightouter")
        .withColumn("strategyTradeValue", when(col("lagDay1").isNotNull, col("tradeVal")).otherwise(0))
        .withColumn("strategyFlag", when(col("lagDay1").isNotNull, 1).otherwise(0))
        .groupBy("userID").agg(round(sum("strategyTradeValue") / sum("tradeVal"), 4).alias("valRatio"),
        round(sum("strategyFlag") / count("*"), 4).alias("countRatio"),
        round(sum("strategyTradeValue"),2).alias("strategyTradeValue"),
        sum("strategyFlag").alias("strategyTradeNum"))
        .withColumn("strategyTradeValue",when(col("strategyTradeValue") >= 1e13, lit(1e13 - 1)).otherwise(col("strategyTradeValue")))
        .select("userID", "valRatio", "countRatio","strategyTradeValue","strategyTradeNum")
      DataProcessor.dumpResult("cashFlowStrategy", cashFlowStrategyDF)

    }


    // calc analyst strategy
    if (!FileOperator.checkExist("analystStrategy/_SUCCESS")) {
      val analystForecastDF = DataProcessor.analystForecastDF
        .filter(col("scoreID") > 3).groupBy("stockID", "intoDate").agg(mean("scoreID").alias("scoreID"))
        .withColumn("lagDay1", date_format((unix_timestamp(col("intoDate"), timeFmt) + 86400).cast("timestamp"), timeFmt))
      val analystTradeDF = analystForecastDF.join(strategyDF, analystForecastDF("stockID") === strategyDF("stockID")
        && analystForecastDF("lagDay1") === strategyDF("date"), "rightouter")
        .withColumn("strategyTradeValue", when(col("lagDay1").isNotNull, col("tradeVal")).otherwise(0))
        .withColumn("strategyFlag", when(col("lagDay1").isNotNull, 1).otherwise(0))
        .groupBy("userID").agg(round(sum("strategyTradeValue") / sum("tradeVal"), 4).alias("valRatio"),
        round(sum("strategyFlag") / count("*"), 4).alias("countRatio"),
        round(sum("strategyTradeValue"),2).alias("strategyTradeValue"),
        sum("strategyFlag").alias("strategyTradeNum"))
        .withColumn("strategyTradeValue",when(col("strategyTradeValue") >= 1e13, lit(1e13 - 1)).otherwise(col("strategyTradeValue")))
        .select("userID", "valRatio", "countRatio","strategyTradeValue","strategyTradeNum")
      DataProcessor.dumpResult("analystStrategy", analystTradeDF)

    }

    /*-------计算用户国债逆回购策略-----------*/
    if (!FileOperator.checkExist("gznhg/_SUCCESS")) {
      val gznhgdf = strategyDF
        .withColumn("strategyFlag", when(col("category") === "repurchase",1).otherwise(0))
        .withColumn("strategyTradeValue", col("strategyFlag") * col("tradeVal"))
        .groupBy("userID").agg(round(sum("strategyTradeValue") / sum("tradeVal"), 4).alias("valRatio"),
        round(sum("strategyFlag") / count("*"), 4).alias("countRatio"),
        round(sum("strategyTradeValue"),2).alias("strategyTradeValue"),
        sum("strategyFlag").alias("strategyTradeNum"))
        .select("userID","valRatio","countRatio","strategyTradeValue","strategyTradeNum")
        .filter(col("strategyTradeNum") > 0)
        .withColumn("strategyTradeValue", when(col("strategyTradeValue") >= 1e13, lit(1e13 - 1)).otherwise(col("strategyTradeValue")))
      DataProcessor.dumpResult("gznhg", gznhgdf)

    }

    /*-------计算用户可转换债券策略-----------*/
    if (!FileOperator.checkExist("convertibleBonds/_SUCCESS")) {
      val df = strategyDF
        .withColumn("strategyFlag",when(col("stockType") === "Y", 1).otherwise(0))
        .withColumn("strategyTradeValue", col("strategyFlag")*col("tradeVal"))
        .groupBy("userID").agg(round(sum("strategyTradeValue") / sum("tradeVal"), 4).alias("valRatio"),
        round(sum("strategyFlag") / count("*"), 4).alias("countRatio"),
        round(sum("strategyTradeValue"),2).alias("strategyTradeValue"),
        round(sum("strategyFlag"),2).alias("strategyTradeNum"))
        .withColumn("strategyTradeValue",when(col("strategyTradeValue") >= 1e13, lit(1e13 - 1)).otherwise(col("strategyTradeValue")))
      DataProcessor.dumpResult("convertibleBonds", df)
    }

  }

}
