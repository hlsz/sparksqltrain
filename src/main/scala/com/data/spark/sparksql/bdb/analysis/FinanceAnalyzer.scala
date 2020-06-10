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

import com.data.spark.sparksql.bdb.bean.ProfitBeanNew
import com.data.spark.sparksql.bdb.util.{ProfitUtil, DataProcessor, FileOperator}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{abs, _}
import org.joda.time.{DateTime, Months}
import org.joda.time.format.DateTimeFormat
import org.apache.log4j.Logger
/**
  * Created by liang on 17-5-25.
  */
class FinanceAnalyzer(session: SQLContext) {
  val logger = Logger.getLogger(getClass.getName)

  val calCumReturn = udf { (rates: Seq[Double]) => {
    var annReturn : Double = 1d
    for (i <- rates) {
      annReturn = annReturn * i
    }
    Map("annReturn" -> annReturn)
  }
  }
  def analyse(): Unit = {
    import session.implicits._

    val wStockTradeValDesc = Window.partitionBy(col("userID")).orderBy(col("totalFloatVal").desc)
    val wStockTradeVal = Window.partitionBy(col("userID")).orderBy(col("totalFloatVal"))

    val managementHoldDetail = DataProcessor.managementHoldDetail.cache()


    val managementInnerTradeDetail = DataProcessor.otcFundTrade
      .withColumnRenamed("init_date", "date")
      .withColumn("category", DataProcessor.changeCategoryFunc(col("stock_type")))
      .filter(col("stockID").startsWith("BB"))
      .withColumn("tradeVal", col("nav") * col("shares"))

      .filter(col("business_flag") === 122 || col("business_flag") === 124
        || col("business_flag") === 130 || col("business_flag") === 142
        // 强行调增强行调减
        || col("business_flag") === 144 || col("business_flag") === 145
        || col("business_flag") === 133
        || col("business_flag") === 143)
      .withColumn("outCapital", when(col("business_flag") === 130 || col("business_flag") === 122
        || col("business_flag") === 144, col("tradeVal")).otherwise(-col("tradeVal")))
      // 143的红股入账，如果本很少，1000快，入账500，一下子收益率５０％
      .withColumn("outCapital", when(col("business_flag") === 143, -col("balance")).otherwise(col("outCapital")))

      .select("userID", "date", "stockID", "category", "tradeVal", "outCapital")
      .cache()

    val managementTradeDetail = DataProcessor.secumDeliver
      .filter(col("PROD_CODE").startsWith("BB"))
      .select("userID","date","PROD_CODE","entrust_balance")
      .withColumnRenamed("PROD_CODE","stockID")
      .withColumn("outCapital",col("entrust_balance"))
      .select("userID","date","stockID","outCapital")
      .unionAll(managementInnerTradeDetail.select("userID","date","stockID","outCapital"))




    val managementHoldFund = managementHoldDetail
      .groupBy("userID", "date").agg(sum(col("market_value")).alias("totalNetAsset"))
      .select("userID", "date", "totalNetAsset")

    val bankHold = DataProcessor.bankmShare
      .select("userID", "date", "begin_amount")
      .withColumnRenamed("begin_amount", "totalNetAsset")

    val secuHold = DataProcessor.secumShare
      .select("userID", "date", "marketValue")
      .withColumnRenamed("marketValue", "totalNetAsset")

    val financeHoldAsset = DataProcessor.financeHoldAsset
      .select("userID","date","totalNetAsset").cache()


    val financeHoldUserIDset = financeHoldAsset.select("userID").distinct()


    val managementInnerCapitalDF = managementInnerTradeDetail
      .groupBy("userId", "date").agg(sum(col("outCapital")).alias("outCapital"))
      .select("userId", "date", "outCapital")

    val managementCapitalDF = managementTradeDetail
      .groupBy("userId", "date").agg(sum(col("outCapital")).alias("outCapital"))
      .select("userId", "date", "outCapital")

    val bankCapital = DataProcessor.bankmDeliver
      // outCapital调整成买入为正卖出为负
      .groupBy("userID", "date").agg(sum(-col("entrust_balance")).alias("outCapital"))



    //////////////////////////////////TODO 44133是BB1005的快取，entrustbalance字段为0，可能需要特殊处理ia

    val secuCapital = DataProcessor.secumDeliver
      // outCapital调整成买入为正卖出为负
      .groupBy("userID", "date").agg(sum(-col("entrust_balance")).alias("outCapital"))

    val finaneCapitalDF = managementInnerCapitalDF
      .unionAll(bankCapital)
      .unionAll(secuCapital)
      .groupBy("userID", "date").agg(sum(col("outCapital")).alias("outCapital"))

    val financeProfitDF: DataFrame = DataProcessor.cashDF
      .select("userId", "date")
      .distinct()
      .join(financeHoldUserIDset, Seq("userId"), "inner")
      .join(financeHoldAsset, Seq("userId", "date"), "left")
      .withColumn("totalNetAsset", when(col("totalNetAsset").isNull, 0d).otherwise(col("totalNetAsset")))
      .join(finaneCapitalDF, Seq("userId", "date"), "left")
      .withColumn("outCapital", when(col("outCapital").isNull, 0).otherwise(col("outCapital")))
      .withColumn("capital", col("outCapital"))
      .withColumn("totalNetAsset", when(col("totalNetAsset").isNull, 0).otherwise(col("totalNetAsset")))
      .select("userID", "date", "totalNetAsset", "capital")
      .withColumn("share", col("totalNetAsset"))
      .withColumn("net", lit(1.0))
      .select("userID", "date", "totalNetAsset", "capital", "share", "net")
      .rdd.map(row => new ProfitBeanNew(row.getString(0), row.getString(1), row.getDouble(2), row.getDouble(3), row.getDouble(4), row.getDouble(5), 0, 0))
      .groupBy(row => row.getUserID)
      .map(x => ProfitUtil.calManagementRateRecursive(x._2)).flatMap(x => x).toDF()
      .withColumnRenamed("dailyReturn", "dailyProfit")
      .withColumnRenamed("totalNetAsset", "mktVal")
      .select("userID", "date", "rate", "dailyProfit", "mktVal")
      .withColumn("rate", round(col("rate"), 5))
      .withColumn("dailyProfit", round(col("dailyProfit"), 2))
      .withColumn("mktVal", round(col("mktVal"), 2))
      .cache()

    if (!FileOperator.checkExist("financeAsset/_SUCCESS")) {
      val financeAsset = financeProfitDF
        .withColumn("dailyReturn", col("rate") - lit(1))
        .groupBy("userID")
        .agg(round(sum("dailyProfit"), 2).alias("profit"),
          calCumReturn(collect_list("dailyReturn")).alias("tuple"))
        .withColumn("cumReturn", round(col("tuple").getItem("annReturn"), 4))
        .withColumn("annCumReturn", DataProcessor.convert2Year(col("cumReturn"), lit(DataProcessor.dayNum)))
        .withColumn("annCumReturnCompound", DataProcessor.convert2YearCompund(col("cumReturn"), lit(DataProcessor.dayNum)))
        .select("userID", "cumReturn", "annCumReturn", "annCumReturnCompound")

        .withColumn("cumReturn", when(col("cumReturn") >= 9e7, 9e7).otherwise(col("cumReturn")))
        .withColumn("cumReturn", when(col("cumReturn") <= -9e7, -9e7).otherwise(col("cumReturn")))
        .withColumn("annCumReturn", when(col("annCumReturn") >= 9e7, 9e7).otherwise(col("annCumReturn")))
        .withColumn("annCumReturn", when(col("annCumReturn") <= -9e7, -9e7).otherwise(col("annCumReturn")))
        .withColumn("annCumReturnCompound", when(col("annCumReturnCompound") >= 9e7, 9e7).otherwise(col("annCumReturnCompound")))
        .withColumn("annCumReturnCompound", when(col("annCumReturnCompound") <= -9e7, -9e7).otherwise(col("annCumReturnCompound")))

      DataProcessor.dumpResult("financeAsset", financeAsset)
    }


    if (!FileOperator.checkExist("df/financeprofitdf/_SUCCESS")) {
      DataProcessor.writeToDB(financeProfitDF, "financeprofitdf", true)
    }
    DataProcessor.releaseCache(financeProfitDF)
    DataProcessor.releaseCache(financeHoldAsset)



    if (!FileOperator.checkExist("bankProfit/_SUCCESS")) {
      val bankProfit = DataProcessor.cashDF
        .select("userID", "date")
        .distinct()
        .join(bankHold.select("userID").distinct(), Seq("userID"))
        .join(bankHold, Seq("userID", "date"), "left")
        .join(bankCapital, Seq("userID", "date"), "left")
        .withColumnRenamed("outCapital", "capital")
        .select("userID", "date", "totalNetAsset", "capital")
        .na.fill(0d)
        .withColumn("share", col("totalNetAsset"))
        .withColumn("net", lit(1.0))
        .select("userID", "date", "totalNetAsset", "capital", "share", "net")
        .rdd.map(row => new ProfitBeanNew(row.getString(0), row.getString(1), row.getDouble(2), row.getDouble(3), row.getDouble(4), row.getDouble(5), 0, 0))
        .groupBy(row => row.getUserID)
        .map(x => ProfitUtil.calManagementRateRecursive(x._2)).flatMap(x => x).toDF()
        .withColumnRenamed("dailyReturn", "dailyProfit")
        .groupBy("userID")
        .agg(round(sum("dailyProfit"), 2).alias("profit"),
          calCumReturn(collect_list("rate")).alias("tuple"),
          count("*").alias("tradeDays"))
        .withColumn("annReturn", col("tuple").getItem("annReturn"))
        .withColumn("annCumReturn", round(DataProcessor.convert2Year(col("annReturn"), col("tradeDays")), 4))
        .select("userId", "annCumReturn", "profit")
      DataProcessor.dumpResult("bankProfit", bankProfit)
    }


    if (!FileOperator.checkExist("manageProfit/_SUCCESS")) {
      val manageProfit = DataProcessor.cashDF
        .select("userID", "date")
        .distinct()
        .join(managementHoldFund.select("userID").distinct(), Seq("userID"))
        .join(managementHoldFund, Seq("userID", "date"), "left")
        .join(managementCapitalDF, Seq("userID", "date"), "left")
        .withColumnRenamed("outCapital", "capital")
        .na.fill(0d)
        .select("userID", "date", "totalNetAsset", "capital")
        .withColumn("share", col("totalNetAsset"))
        .withColumn("net", lit(1.0))
        .select("userID", "date", "totalNetAsset", "capital", "share", "net")
        .rdd.map(row => new ProfitBeanNew(row.getString(0), row.getString(1), row.getDouble(2), row.getDouble(3), row.getDouble(4), row.getDouble(5), 0, 0))
        .groupBy(row => row.getUserID)
        .map(x => ProfitUtil.calManagementRateRecursive(x._2)).flatMap(x => x).toDF()
        .withColumnRenamed("dailyReturn", "dailyProfit")
        .groupBy("userID")
        .agg(round(sum("dailyProfit"), 2).alias("profit"),
          calCumReturn(collect_list("rate")).alias("tuple"),
          count("*").alias("tradeDays"))
        .withColumn("annReturn", col("tuple").getItem("annReturn"))
        .withColumn("annCumReturn", round(DataProcessor.convert2Year(col("annReturn"), col("tradeDays")), 4))

        .withColumn("annCumReturn", when(col("annCumReturn") >= 9e7, 9e7).otherwise(col("annCumReturn")))
        .withColumn("annCumReturn", when(col("annCumReturn") <= -9e7, -9e7).otherwise(col("annCumReturn")))

        .select("userId", "annCumReturn", "profit")
      DataProcessor.dumpResult("manageProfit", manageProfit)
    }

    if (!FileOperator.checkExist("receiptProfit/_SUCCESS")) {
      val receiptHold = DataProcessor.rawSecumShare
        .filter(col("PRODTA_NO") === "CZZ")
        .groupBy("userID", "date").agg(sum("marketValue").alias("totalNetAsset"))
        .select("userID", "date", "totalNetAsset")
        .cache()
      val receiptCapital = DataProcessor.secumDeliver
        .filter(col("PRODTA_NO") === "CZZ")
        // outCapital调整成买入为正卖出为负
        .groupBy("userID", "date").agg(sum(-col("entrust_balance")).alias("capital"))
        .select("userID", "date", "capital")


      val receiptProfit = DataProcessor.cashDF
        .select("userID", "date")
        .distinct()
        .join(receiptHold.select("userID").distinct(), Seq("userID"))
        .join(receiptHold, Seq("userID", "date"), "left")
        .join(receiptCapital, Seq("userID", "date"), "left")
        .na.fill(0d)
        .select("userID", "date", "totalNetAsset", "capital")
        .withColumn("share", col("totalNetAsset"))
        .withColumn("net", lit(1.0))
        .select("userID", "date", "totalNetAsset", "capital", "share", "net")
        .rdd.map(row => new ProfitBeanNew(row.getString(0), row.getString(1), row.getDouble(2), row.getDouble(3), row.getDouble(4), row.getDouble(5), 0, 0))
        .groupBy(row => row.getUserID)
        .map(x => ProfitUtil.calManagementRateRecursive(x._2)).flatMap(x => x).toDF()
        .withColumnRenamed("dailyReturn", "dailyProfit")
        .groupBy("userID")
        .agg(round(sum("dailyProfit"), 2).alias("profit"),
          calCumReturn(collect_list("rate")).alias("tuple"),
          count("*").alias("tradeDays"))
        .withColumn("annReturn", col("tuple").getItem("annReturn"))
        .withColumn("annCumReturn", round(DataProcessor.convert2Year(col("annReturn"), col("tradeDays")), 4))
        .select("userId", "annCumReturn", "profit")
      DataProcessor.dumpResult("receiptProfit", receiptProfit)

      DataProcessor.releaseCache(receiptHold)
    }




    if (!FileOperator.checkExist("finanTrade/_SUCCESS")) {
      val financeHold = DataProcessor.managementInnerHoldDetail
        .filter(col("market_value") > 0)
        .select("userID", "stockID", "date")
        .unionAll(DataProcessor.rawBankmShare
          .filter(col("begin_amount") > 0)
          .withColumnRenamed("prod_code", "stockID").select("userID", "stockID", "date"))
        .unionAll(DataProcessor.rawSecumShare
          .filter(col("marketValue") > 0)
          .withColumnRenamed("prod_code", "stockID").select("userID", "stockID", "date"))

      val financeHoldNum = financeHold.groupBy("userID", "date")
        .agg(count("*").alias("count"))
        .filter(col("date")>=DataProcessor.startDate && col("date")<=DataProcessor.endDate)
        .groupBy("userID")
        .agg(round(mean("count"), 1).alias("holdNum"))

      val finanTrade = financeHold.groupBy("userID", "stockID")
        .agg(count("*").alias("count"))
        .groupBy("userID")
        .agg(mean("count").alias("durMean"))
        .join(financeHoldNum, Seq("userID"), "inner")
        .select("userID", "holdNum", "durMean")

      DataProcessor.dumpResult("finanTrade", finanTrade)
    }




    //资管产品　　银行理财和证券理财要算盈利的产品
    //第一天的持仓当作成本，最后一天的持仓当作收益，将交易金额除第一天外相加，减去持仓加上收益，就是每个产品的盈利情况
    val managermentCost = managementHoldDetail
      .filter(col("date") === DataProcessor.firstDay)
      .groupBy("userID", "stockID").agg(sum(col("market_value")).alias("cost"))
      .select("userID", "stockID", "cost")

    val managermentGain = managementHoldDetail
      .filter(col("date") === DataProcessor.lastDay)
      .groupBy("userID", "stockID").agg(sum(col("market_value")).alias("gain"))
      .select("userID", "stockID", "gain")

    val managementTradeValTotal = managementTradeDetail
      .filter(col("date") !== DataProcessor.firstDay)
      // outCapital设置成买为负
      .groupBy("userID", "stockID").agg(sum(-col("outCapital")).alias("outCapital"))

    val profitableManagment = managementTradeValTotal
      .join(managermentGain, Seq("userID", "stockID"), "outer")
      .join(managermentCost, Seq("userID", "stockID"), "outer")
      .na.fill(0)
      .withColumn("totalFloatVal", col("gain") - col("cost") + col("outCapital"))
      .select("userID", "stockID", "totalFloatVal")

    val bankCost = DataProcessor.rawBankmShare
      .filter(col("date") === DataProcessor.firstDay)
      .groupBy("userID", "prod_code").agg(sum(col("begin_amount")).alias("cost"))
      .select("userID", "prod_code", "cost")

    val bankGain = DataProcessor.rawBankmShare
      .filter(col("date") === DataProcessor.lastDay)
      .groupBy("userID", "prod_code").agg(sum(col("begin_amount")).alias("gain"))
      .select("userID", "prod_code", "gain")

    val bankTrade = DataProcessor.bankmDeliver
      .filter(col("date") !== DataProcessor.firstDay)
      // 这里的entrust_balance 买入为负，卖出为正
      .select("userId", "date", "prod_code", "entrust_balance")
      .groupBy("userId", "prod_code").agg(sum(col("entrust_balance")).alias("entrust_balance"))

    val profitableBank = bankCost
      .join(bankGain, Seq("userID", "prod_code"), "outer")
      .join(bankTrade, Seq("userID", "prod_code"), "outer")
      .na.fill(0)
      .withColumn("totalFloatVal", col("gain") - col("cost") + col("entrust_balance"))
      .withColumnRenamed("prod_code", "stockID")
      .select("userID", "stockID", "totalFloatVal")

    val secumCost = DataProcessor.rawSecumShare
      .filter(col("date") === DataProcessor.firstDay)
      .select("userID", "PROD_CODE", "marketValue")
      .groupBy("userID", "PROD_CODE").agg(sum(col("marketValue")).alias("cost"))

    val secumGain = DataProcessor.rawSecumShare
      .filter(col("date") === DataProcessor.lastDay)
      .select("userID", "PROD_CODE", "marketValue")
      .groupBy("userID", "PROD_CODE").agg(sum(col("marketValue")).alias("gain"))

    val secumTrade = DataProcessor.secumDeliver
      .filter(col("date") !== DataProcessor.firstDay)
      // entrust_balance这里买入为负，卖出为正
      .select("userID", "date", "PROD_CODE", "entrust_balance")
      .groupBy("userID", "PROD_CODE").agg(sum(col("entrust_balance")).alias("entrust_balance"))

    val profitSecum = secumCost
      .join(secumGain, Seq("userID", "PROD_CODE"), "outer")
      .join(secumTrade, Seq("userID", "PROD_CODE"), "outer")
      .na.fill(0)
      .withColumn("totalFloatVal", col("gain") - col("cost") + col("entrust_balance"))
      .withColumnRenamed("PROD_CODE", "stockID")
      .select("userID", "stockID", "totalFloatVal")

    val profitablefinancedf = profitSecum
      .unionAll(profitableBank)
      .unionAll(profitableManagment)
      .select("userID", "stockID", "totalFloatVal")
      .withColumn("initRank", row_number().over(wStockTradeValDesc))
      .withColumn("invertRank", row_number().over(wStockTradeVal))
      .select("userID", "stockID", "initRank", "invertRank", "totalFloatVal")

    if (!FileOperator.checkExist("df/profitablefinancedf/_SUCCESS")) {
      DataProcessor.writeToDB(profitablefinancedf, "profitablefinancedf", true)
    }



    DataProcessor.releaseCache(managementHoldDetail)
    DataProcessor.releaseCache(managementInnerTradeDetail)

  }
}
