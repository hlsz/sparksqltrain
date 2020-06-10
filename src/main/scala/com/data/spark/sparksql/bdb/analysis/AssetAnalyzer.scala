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

import java.time.LocalDate
import java.time.format.DateTimeFormatter


import com.data.spark.sparksql.bdb.util._
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import com.data.spark.sparksql.bdb.bean._

/**
  * Created by liang on 17-7-18.
  */
class AssetAnalyzer(session: SQLContext,sc: SparkContext) {

  val calMaxDrawdown = udf { (dailyReturns: Seq[Double]) => {
    var maxDrawdown = 0.0
    var annReturn = 0.0
    // 回撤时间占比
    var drawDownDayRatio = 0.0
    if (dailyReturns.size > 0) {
      var i = 0
      var drawdown = 0.0
      val cumReturns = dailyReturns.map(_ + 1.0).scanLeft(1.0)(_ * _)
      var globalMin = cumReturns(0)
      var pastMaxValue = cumReturns(0)
      var drawDaysNum : Integer = 0
      for (i <- 1 to dailyReturns.size) {
        drawdown = (pastMaxValue - cumReturns(i)) / pastMaxValue
        if (drawdown > maxDrawdown) {
          maxDrawdown = drawdown
          globalMin = cumReturns(i)
        }
        if (drawdown < 0) {
          pastMaxValue = cumReturns(i)
        } else if (drawdown > 0) {
          drawDaysNum = drawDaysNum + 1
        }
      }
      annReturn = cumReturns.last

    }
    Map("maxDrawdown" -> maxDrawdown, "annReturn" -> annReturn)
  }
  }



  val calMaxDrawdownDetail = udf { (dailyReturns: Seq[Double]) => {
    var maxDrawdown = 0.0
    var annReturn = 0.0
    // 回撤时间占比
    var drawDownDayRatio = 0.0
    // 平均回撤值
    var annDrawDownValue : Double = 0.0
    if (dailyReturns.size > 0) {
      var i = 0
      var drawdown = 0.0
      val cumReturns = dailyReturns.map(_ + 1.0).scanLeft(1.0)(_ * _)
      var globalMin = cumReturns(0)
      var pastMaxValue = cumReturns(0)
      var drawDaysNum : Integer = 0
      for (i <- 1 to dailyReturns.size) {
        drawdown = (pastMaxValue - cumReturns(i)) / pastMaxValue
        if (drawdown > maxDrawdown) {
          maxDrawdown = drawdown
          globalMin = cumReturns(i)
        }
        if (drawdown < 0) {
          pastMaxValue = cumReturns(i)
        } else if (drawdown > 0) {
          drawDaysNum = drawDaysNum + 1
          annDrawDownValue = annDrawDownValue + drawdown
        }
      }
      annReturn = cumReturns.last
      drawDownDayRatio = drawDaysNum * 1.0 / dailyReturns.size
      annDrawDownValue = annDrawDownValue / dailyReturns.size
    }
    Map("maxDrawdown" -> maxDrawdown, "annReturn" -> annReturn, "drawDownDayRatio" -> drawDownDayRatio, "annDrawDownValue" -> annDrawDownValue)
  }
  }


  val cov = session.udf.register("cov", new Covariance)

  def analyse(): Unit = {
    import session.implicits._


    val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")

    var dateSeq = Seq[String]()
    var start: LocalDate = LocalDate.parse(DataProcessor.startDate, formatter)
    val localEnd: LocalDate = LocalDate.parse(DataProcessor.endDate, formatter).plusDays(1)
    while (start.isBefore(localEnd)) {
      dateSeq = dateSeq :+ start.format(formatter)
      start = start.plusDays(1)
    }

    val caredDate: DataFrame = session.createDataset(dateSeq).toDF()
    caredDate.registerTempTable("caredDate")
    caredDate.printSchema()



    val idxNameMap = Map("上证综指" -> "sz", "中证500" -> "zz500", "沪深300" -> "hs300")
    val idxNameMaping = udf { (i: String) => idxNameMap.get(i) }

    val mktBasicIdxDF = DataProcessor.mktBasicIdxDF
      .withColumn("shortName", idxNameMaping(col("shortName")))
      .groupBy("date").pivot("shortName").agg(sum("chgPCT"))


    // 负债相关流水
    val capitalFlowDF = DataProcessor.capitalFlowDF
      .select("date", "userID", "FUND_ACCOUNT", "moneyType", "EXCHANGE_TYPE", "business_flag", "occur_balance", "post_balance", "CURR_TIME", "SERIAL_NO")
      .filter(col("business_flag") === 2700 || col("business_flag") === 2711 || col("business_flag") === 2712
        || col("business_flag") === 2713 || col("business_flag") === 2717 || col("business_flag") === 2718 || col("business_flag") === 2719
        || col("business_flag") === 2720 || col("business_flag") === 2721 || col("business_flag") === 2722 || col("business_flag") === 2723
        || col("business_flag") === 2724 || col("business_flag") === 2725 || col("business_flag") === 2731 || col("business_flag") === 2732
        || col("business_flag") === 2740 || col("business_flag") === 2741 || col("business_flag") === 4436 || col("business_flag") === 4437
        || col("business_flag") === 4198 || col("business_flag") === 4183 || col("business_flag") === 4185
        || col("business_flag") === 2041 || col("business_flag") === 2042 || col("business_flag") === 2055 || col("business_flag") === 2056
        || col("business_flag") === 2629 || col("business_flag") === 2630 || col("business_flag") === 2007 || col("business_flag") === 2008
        || col("business_flag") === 4139 || col("business_flag") === 4166 || col("business_flag") === 31282 || col("business_flag") === 31283
        || col("business_flag") === 173 || col("business_flag") === 174
        || col("business_flag") === 44120 || col("business_flag") === 44122 || col("business_flag") === 44124
        || col("business_flag") === 44130 || col("business_flag") === 44142 || col("business_flag") === 44150
        || col("business_flag") === 43130 || col("business_flag") === 43142
      ).cache()

    val cashDF = DataProcessor.cashDF.select("userID", "date", "current_balance").repartition($"userID")


    val otcFundDF = DataProcessor.otcFundHold
      .groupBy("userID", "date").agg(sum("market_value").alias("otcFundMktVal"))


    // 逆回购相关计算
    val reverseRepoDailyDF = DataProcessor.holdDF.filter(col("category")==="repurchase")
      .groupBy("userID", "date").agg(sum("repoAsset").alias("repoAsset"))
      .select("userID", "date", "repoAsset")

    val reverseRepoUserSet = reverseRepoDailyDF.select("userID").distinct
    val reverseRepoCapital = DataProcessor.tradeDF
      .filter((col("business_flag")===4004) || (col("business_flag")===4005) ||
        (col("business_flag")===4105) || (col("business_flag")===4104))
      // 逆回购涉及到利息,所以用clearbalance算,clearbalance买入为正，卖出为负
      .withColumn("clear_balance", -col("clear_balance"))
      .select("userID","date","clear_balance")
      .groupBy("userID","date").agg(sum(col("clear_balance")).alias("capital"))

    val reverseRepoProfitDF = DataProcessor.cashDF
      .select("userId", "date")
      .distinct()
      .join(reverseRepoUserSet, Seq("userID"), "inner")
      .join(reverseRepoDailyDF,Seq("userId","date"), "left")
      .join(reverseRepoCapital,Seq("userId","date"), "left")
      .na.fill(0)
      .select("userID","date","repoAsset","capital")
      .withColumnRenamed("repoAsset", "totalNetAsset")
      .withColumn("share", col("totalNetAsset"))
      .withColumn("net", lit(1.0))
      .select("userID", "date", "totalNetAsset", "capital", "share", "net")
      .rdd.map(row => new ProfitBeanNew(row.getString(0), row.getString(1), row.getDouble(2), row.getDouble(3), row.getDouble(4), row.getDouble(5), 0, 0))
      .groupBy(row => row.getUserID)
      .map(x => ProfitUtil.calRepoRateRecursive(x._2)).flatMap(x => x).toDF()
      .withColumnRenamed("totalNetAsset", "repoAsset")
      .withColumnRenamed("dailyReturn", "dailyProfit")
      .select("userID", "date", "rate", "repoAsset", "dailyProfit")
      .withColumn("repoAsset",round(col("repoAsset"),2))
      .withColumn("rate",round(col("rate"),5))
      .withColumn("dailyProfit",round(col("dailyProfit"),2))
      .cache()

    if (!FileOperator.checkExist("df/repoprofitdf/_SUCCESS")) {
      DataProcessor.writeToDB(reverseRepoProfitDF, "repoprofitdf", true)
    }

    if (!FileOperator.checkExist("repoAsset/_SUCCESS")) {
      val repoAsset = reverseRepoProfitDF
        .withColumn("dailyReturn", col("rate") - lit(1))
        .groupBy("userID")
        .agg(round(sum("dailyProfit"), 2).alias("profit"),
          calMaxDrawdown(collect_list("dailyReturn")).alias("tuple"))
        .withColumn("cumReturn", round(col("tuple").getItem("annReturn"), 4))
        .withColumn("annCumReturn", DataProcessor.convert2Year(col("cumReturn"), lit(DataProcessor.dayNum)))
        .withColumn("annCumReturnCompound", DataProcessor.convert2YearCompund(col("cumReturn"), lit(DataProcessor.dayNum)))
        .select("userID","cumReturn","annCumReturn","annCumReturnCompound")

        .withColumn("cumReturn",when(col("cumReturn") >= 9e7, 9e7).otherwise(col("cumReturn")))
        .withColumn("cumReturn",when(col("cumReturn") <= -9e7, -9e7).otherwise(col("cumReturn")))
        .withColumn("annCumReturn",when(col("annCumReturn") >= 9e7, 9e7).otherwise(col("annCumReturn")))
        .withColumn("annCumReturn",when(col("annCumReturn") <= -9e7, -9e7).otherwise(col("annCumReturn")))
        .withColumn("annCumReturnCompound",when(col("annCumReturnCompound") >= 9e7, 9e7).otherwise(col("annCumReturnCompound")))
        .withColumn("annCumReturnCompound",when(col("annCumReturnCompound") <= -9e7, -9e7).otherwise(col("annCumReturnCompound")))

      DataProcessor.dumpResult("repoAsset", repoAsset)
    }

    DataProcessor.releaseCache(reverseRepoProfitDF)



    // 计算债券收益市值
    val bondMktDF = DataProcessor.holdDF.filter(col("category")==="bond")
      .groupBy("userID", "date").agg(sum("bondMktVal").alias("bondMktVal"))
      .select("userID", "date", "bondMktVal")

    val bondUserIdSet = bondMktDF.select("userID").distinct()

    val bondCapitalDF = DataProcessor.tradeDF.filter(col("category")==="bond")
      .withColumn("capital", -col("clear_balance"))
      // 新股处理
      .withColumn("capital", when(col("business_flag") === 4016, col("num") * col("price")).otherwise(col("capital")))
      .groupBy("userID", "date").agg(sum("capital").alias("capital"))
      .select("userID", "date", "capital")

    val bondProfitDF = DataProcessor.cashDF
      .select("userId", "date")
      .distinct()
      .join(bondUserIdSet,Seq("userID"),"inner")
      .join(bondMktDF,Seq("userId","date"), "left")
      .join(bondCapitalDF,Seq("userId","date"), "left")
      .na.fill(0)
      .select("userID","date","bondMktVal","capital")
      .withColumnRenamed("bondMktVal", "totalNetAsset")
      .withColumn("share", col("totalNetAsset"))
      .withColumn("net", lit(1.0))
      .select("userID", "date", "totalNetAsset", "capital", "share", "net")
      .rdd.map(row => new ProfitBeanNew(row.getString(0), row.getString(1), row.getDouble(2), row.getDouble(3), row.getDouble(4), row.getDouble(5), 0, 0))
      .groupBy(row => row.getUserID)
      .map(x => ProfitUtil.calBondRateRecursive(x._2)).flatMap(x => x).toDF()
      .withColumnRenamed("dailyReturn","dailyProfit")
      .cache()

    if (!FileOperator.checkExist("df/bondprofitdf/_SUCCESS")) {
      DataProcessor.writeToDB(bondProfitDF.withColumnRenamed("totalNetAsset", "bondMktVal")
        .withColumn("dailyProfit", round(col("dailyProfit"), 2))
        .withColumn("bondMktVal", round(col("bondMktVal"), 2))
        .withColumn("rate", round(col("rate"), 4))
        .select("userID", "date", "rate", "dailyProfit", "bondMktVal"), "bondprofitdf", true)
    }

    if (!FileOperator.checkExist("bondAsset/_SUCCESS")) {
      val bondAsset = bondProfitDF.withColumn("dailyReturn", col("rate") - lit(1))
        .groupBy("userID")
        .agg(round(sum("dailyProfit"), 2).alias("profit"),
          calMaxDrawdown(collect_list("dailyReturn")).alias("tuple"))
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
      DataProcessor.dumpResult("bondAsset", bondAsset)
    }
    DataProcessor.releaseCache(bondProfitDF)


    val securityFinancingDailyDF = DataProcessor.secumShare
        .withColumnRenamed("marketValue","securityFinance")
        .select("userID","date","securityFinance")


    // 根据持仓计算银行理财资产
    val bankFinancingDailyDF = DataProcessor.bankmShare
      .withColumnRenamed("begin_amount", "bankFinance")
      .select("userId","date","bankFinance")


    //bond + lof + etf + stock +repo
    val innerHoldDF = DataProcessor.holdDF
      // 只算普通帐户,因为信用账户的资产已经在userAssetdf里面有了
      .filter(col("ASSET_PROP") === 0)
      .groupBy("userID", "date").agg(sum("mktVal").alias("innerMktVal"), sum("stockMktVal").alias("stockMktVal"),
      sum("innerFundMktVal").alias("innerFundMktVal"), sum("bondMktVal").alias("bondMktVal"))
      .repartition(col("userID"))




    val debtResultDF = DataProcessor.userAsset
       .select("userID", "date", "debt", "optAsset", "crdtNetAsset","totalNetAsset", "totalAsset")

    if (!FileOperator.checkExist("debtInfo/_SUCCESS")) {
      val debtInfo = debtResultDF
        .filter(col("date")>=DataProcessor.startDate && col("date")<=DataProcessor.endDate)
        .select("userID", "debt")
        .groupBy("userID")
        .agg(mean("debt").alias("meanDebt"),
          max("debt").alias("maxDebt"))
        .select("userID","meanDebt","maxDebt")
        .withColumn("meanDebt",round(col("meanDebt"),2))
        .withColumn("maxDebt",round(col("maxDebt"),2))

      DataProcessor.dumpResult("debtInfo", debtInfo)
    }




    val assetDFLocal = DataProcessor.assetDF
      .withColumn("prePrice", lag("asset_price",1).over(Window.partitionBy("exchangeCD","stockID").orderBy("date")))
      .withColumn("prePrice", when(col("prePrice").isNull, col("asset_price")).otherwise(col("prePrice")))

    // 担保品
    val collateralDF = DataProcessor.rawTradeDF
      // 从信用帐户取担保品
      .filter(col("asset_prop") === "7")
      .filter(col("business_flag") === 4721 || col("business_flag") === 4722)
      .select("userID","date","stockID","num","exchangeCD")
      .join(assetDFLocal,Seq("date","exchangeCD","stockID"), "left")
      .withColumn("collateral", col("num") * col("prePrice") * lit(-1))
      .groupBy("userID","date").agg(sum(col("collateral")).alias("collateral"))






    // 计算银行的流水流入流出,存入为正,取出为负，这里应该不需要考虑资金帐户
    val bankFlowIO: DataFrame = capitalFlowDF.filter(col("business_flag") === 2041
      || col("business_flag") === 2042
      || col("business_flag") === 2007 || col("business_flag") === 2008
      || col("business_flag") === 31282 || col("business_flag") === 31283)
      .groupBy("userID", "date").agg(sum(col("occur_balance")).alias("capital"))
      /////////////////////////////证券理财清盘赎回都回到帐户，目前先注释掉这个
//        .join(bankFlowIOInTrade, Seq("userID","date"),"outer")
//        .na.fill(0)
//        .withColumn("capital", col("capital") + col("tradeIO"))
      .withColumn("capital", when(col("capital").isNull, 0d).otherwise(col("capital")))
      .select("userID", "date", "capital")
      .join(collateralDF, Seq("userID", "date"), "outer")
      .na.fill(0)
      .withColumn("capital", col("capital") + col("collateral"))
      .select("userID", "date", "capital")




    val stockCollateralDF = DataProcessor.tradeDF
      .filter(col("category")==="stock")
      .filter(col("business_flag") === 4721 || col("business_flag") === 4722)
      // 这里是对普通帐户的担保平划入和划出做特殊处理，赋予tradeVal值，这样在ability里面计算个股收益的时候，不会因为担保品的划入和划出出问题
      .withColumn("tradeVal",col("price") * col("num"))
      // 存入为正,取出为负
      .groupBy("userID","date").agg(sum(col("tradeVal")).alias("capitalInStockColla"))





    val stockCapitalFlowDF = DataProcessor.tradeDF
      .filter(col("category") === "stock")
      .withColumn("capital", -col("clear_balance"))
      // 新股处理
      .withColumn("capital", when(col("business_flag") === 4016, col("num") * col("price")).otherwise(col("capital")))
      .groupBy("userId", "date").agg(sum("capital").alias("capital"))
      .select("userId", "date", "capital")
      .join(stockCollateralDF,Seq("userID","date"),"outer")
      .na.fill(0)
      .withColumn("capital",col("capital") + col("capitalInStockColla"))
      .select("userId", "date", "capital")


    val stockNetAssetDF : DataFrame = DataProcessor.holdDF.filter(col("category")==="stock")
      .groupBy("userID", "date").agg(sum("stockMktVal").alias("totalNetAsset"))


    val stockUserIdSet = stockNetAssetDF.select("userID").distinct()

    val stockProfitDF = DataProcessor.cashDF
      .select("userID", "date")
      .join(stockUserIdSet, Seq("userID"), "inner")
      .join(stockNetAssetDF, Seq("userID", "date"), "left")
      .join(stockCapitalFlowDF, Seq("userID", "date"), "left")
      .withColumn("capital", when(col("capital").isNull, 0).otherwise(col("capital")))
      .select("userID", "date", "totalNetAsset", "capital")
      .withColumn("totalNetAsset", when(col("totalNetAsset").isNull, 0d).otherwise(col("totalNetAsset")))
      .withColumn("share", col("totalNetAsset"))
      .withColumn("net", lit(1.0))
      .select("userID", "date", "totalNetAsset", "capital", "share", "net")
      .rdd.map(row => new ProfitBeanNew(row.getString(0), row.getString(1), row.getDouble(2), row.getDouble(3), row.getDouble(4), row.getDouble(5), 0, 0))
      .groupBy(row => row.getUserID)
      .map(x => ProfitUtil.calStockRateRecursive(x._2)).flatMap(x => x).toDF()
      .withColumnRenamed("dailyReturn","dailyProfit")
      .withColumnRenamed("totalNetAsset","mktVal")
      .select("userID","date","rate","dailyProfit","mktVal")
      .withColumn("rate", when(col("rate") > 1.5,1).otherwise(col("rate")))
      .withColumn("rate", when(col("rate") < 0.5,1).otherwise(col("rate")))
      .cache()

    if (!FileOperator.checkExist("df/stockprofitdf/_SUCCESS")) {
      DataProcessor.writeToDB(stockProfitDF
        .withColumn("rate", round(col("rate"), 4))
        .withColumn("mktVal", round(col("mktVal"), 2))
        .withColumn("dailyProfit", round(col("dailyProfit"), 2)), "stockprofitdf", true)
    }

    if (!FileOperator.checkExist("stockAsset/_SUCCESS")) {
      val stockAssetDF = stockProfitDF
        .withColumn("dailyReturn", col("rate") - lit(1))
        .groupBy("userID")
        .agg(round(sum("dailyProfit"), 2).alias("profit"),
          calMaxDrawdownDetail(collect_list("dailyReturn")).alias("tuple"),
          round(stddev("dailyReturn") * math.sqrt(250), 4).alias("annStd"))
        .withColumn("maxDrawdown", round(col("tuple").getItem("maxDrawdown"), 4))
        .withColumn("cumReturn", round(col("tuple").getItem("annReturn"), 4))
        .withColumn("annDrawDownValues", round(col("tuple").getItem("annDrawDownValue"), 4))

        .withColumn("annCumReturn", DataProcessor.convert2Year(col("cumReturn"), lit(DataProcessor.dayNum)))
        .withColumn("annCumReturnCompound", DataProcessor.convert2YearCompund(col("cumReturn"), lit(DataProcessor.dayNum)))
        .withColumn("profit",when(col("profit") >= 9e13, 9e13).otherwise(col("profit")))
        .withColumn("profit",when(col("profit") <= -9e13, -9e13).otherwise(col("profit")))
        .withColumn("annStd",when(col("annStd") >= 9e7, 9e7).otherwise(col("annStd")))
        .withColumn("annStd",when(col("annStd") <= -9e7, -9e7).otherwise(col("annStd")))
        .withColumn("cumReturn",when(col("cumReturn") >= 9e7, 9e7).otherwise(col("cumReturn")))
        .withColumn("cumReturn",when(col("cumReturn") <= -9e7, -9e7).otherwise(col("cumReturn")))
        .withColumn("annCumReturn",when(col("annCumReturn") >= 9e7, 9e7).otherwise(col("annCumReturn")))
        .withColumn("annCumReturn",when(col("annCumReturn") <= -9e7, -9e7).otherwise(col("annCumReturn")))
        .withColumn("maxDrawdown",when(col("maxDrawdown") >= 9e7, 9e7).otherwise(col("maxDrawdown")))
        .withColumn("maxDrawdown",when(col("maxDrawdown") <= -9e7, -9e7).otherwise(col("maxDrawdown")))
        .withColumn("annCumReturnCompound",when(col("annCumReturnCompound") >= 9e7, 9e7).otherwise(col("annCumReturnCompound")))
        .withColumn("annCumReturnCompound",when(col("annCumReturnCompound") <= -9e7, -9e7).otherwise(col("annCumReturnCompound")))

        .select("userId", "annStd", "maxDrawdown", "profit","annCumReturn","annCumReturnCompound","cumReturn","annDrawDownValues")
      DataProcessor.dumpResult("stockAsset", stockAssetDF)
    }

    DataProcessor.releaseCache(stockProfitDF)

    /**
      * 基金收益计算
      */
    val holdFundAsset = DataProcessor.fundHoldDf
      // 总资产
      .groupBy("userID", "date").agg(sum("market_value").alias("totalNetAsset"))
      .select("userID","date","totalNetAsset")

    val holdFundUserIdSet = holdFundAsset.select("userID").distinct()

    val fundInnerCapitalDF = DataProcessor.rawTradeDF
      .filter(col("asset_prop") !== "7")
      .withColumn("category",DataProcessor.changeCategoryFunc(col("stockType")))
      .filter(col("category") === "fund")
      .filter(col("business_flag") === 4001 || col("business_flag") === 4002
        // 转入转出
        || col("business_flag") === 4009 || col("business_flag") === 4010
        // 货币基金的申购和赎回
        || col("business_flag") === 4176 || col("business_flag") === 4175
        // 股息入帐
        || col("business_flag") === 4018)
      .withColumn("capital", -col("clear_balance"))
      // 托管转入 和托管转出转入为正转出为负
      .withColumn("capital", when(col("business_flag") === 4009 || col("business_flag") === 4010, col("num") * col("price")).otherwise(col("capital")))
      .groupBy("userId", "date").agg(sum("capital").alias("inCapital"))
      .select("userId", "date", "inCapital")

    val fundOuterCapitalDF = DataProcessor.otcFundTrade
      .withColumnRenamed("init_date","date")
      .withColumn("category", DataProcessor.changeCategoryFunc(col("stock_type")))
      .filter(col("category") === "fund")
      .filter(!col("stockID").startsWith("BB"))
      .withColumn("tradeVal", col("nav") * col("shares"))
      .filter(col("business_flag") === 122 || col("business_flag") === 124
        || col("business_flag") === 130 || col("business_flag") === 142
        // 强行调增强行调减
        || col("business_flag") === 144 || col("business_flag") === 145)


      .withColumn("outCapital", when(col("business_flag") === 130 || col("business_flag") ===  122
        || col("business_flag") === 144, col("tradeVal")).otherwise(-col("tradeVal")))
      .groupBy("userId", "date").agg(sum("outCapital").alias("outCapital"))
      .select("userId", "date", "outCapital")


    val fundAssetDF : DataFrame = DataProcessor.cashDF
      .select("userId", "date")
      .distinct()
      // 只取买过基金的
      .join(holdFundUserIdSet, Seq("userId"), "inner")
      .join(holdFundAsset, Seq("userId", "date"), "left")
      .withColumn("totalNetAsset", when(col("totalNetAsset").isNull, 0d).otherwise(col("totalNetAsset")))
      .join(fundInnerCapitalDF, Seq("userId","date"),"left")
      .withColumn("inCapital", when(col("inCapital").isNull,0).otherwise(col("inCapital")))
      .join(fundOuterCapitalDF, Seq("userId","date"),"left")
      .withColumn("outCapital", when(col("outCapital").isNull,0).otherwise(col("outCapital")))
      .withColumn("capital", col("inCapital") + col("outCapital"))
      .withColumn("totalNetAsset", when(col("totalNetAsset").isNull,0).otherwise(col("totalNetAsset")))
      .select("userID","date","totalNetAsset","capital")


    val fundProfitDF = fundAssetDF.withColumn("share", col("totalNetAsset"))
      .withColumn("net", lit(1.0))
      .select("userID", "date", "totalNetAsset", "capital", "share", "net")
      .rdd.map(row => new ProfitBeanNew(row.getString(0), row.getString(1), row.getDouble(2), row.getDouble(3), row.getDouble(4), row.getDouble(5), 0, 0))
      .groupBy(row => row.getUserID)
      .map(x => ProfitUtil.calFundRateRecursive(x._2)).flatMap(x => x).toDF()
      .withColumnRenamed("dailyReturn", "dailyProfit")
      .withColumn("rate", when(col("rate") > 1.5,1).otherwise(col("rate")))
      .withColumn("rate", when(col("rate") < 0.5,1).otherwise(col("rate")))
      .cache()

    if (!FileOperator.checkExist("fundAsset/_SUCCESS")) {
      val fundAssetDF = fundProfitDF
        .withColumn("dailyReturn", col("rate") - lit(1))
        .groupBy("userID")
        .agg(round(sum("dailyProfit"), 2).alias("profit"),
          calMaxDrawdown(collect_list("dailyReturn")).alias("tuple"),
          round(stddev("dailyReturn") * math.sqrt(250), 4).alias("annStd"))
        .withColumn("maxDrawdown", round(col("tuple").getItem("maxDrawdown"), 4))
        .withColumn("cumReturn", round(col("tuple").getItem("annReturn"), 4))
        .withColumn("annCumReturn", DataProcessor.convert2Year(col("cumReturn"), lit(DataProcessor.dayNum)))
        .withColumn("annCumReturnCompound", DataProcessor.convert2YearCompund(col("cumReturn"), lit(DataProcessor.dayNum)))
        .select("userId", "annStd", "maxDrawdown", "profit","cumReturn","annCumReturn","annCumReturnCompound")

        .withColumn("annStd",when(col("annStd") >= 9e7, 9e7).otherwise(col("annStd")))
        .withColumn("annStd",when(col("annStd") <= -9e7, -9e7).otherwise(col("annStd")))
        .withColumn("cumReturn",when(col("cumReturn") >= 9e7, 9e7).otherwise(col("cumReturn")))
        .withColumn("cumReturn",when(col("cumReturn") <= -9e7, -9e7).otherwise(col("cumReturn")))
        .withColumn("annCumReturn",when(col("annCumReturn") >= 9e7, 9e7).otherwise(col("annCumReturn")))
        .withColumn("annCumReturn",when(col("annCumReturn") <= -9e7, -9e7).otherwise(col("annCumReturn")))
        .withColumn("annCumReturnCompound",when(col("annCumReturnCompound") >= 9e7, 9e7).otherwise(col("annCumReturnCompound")))
        .withColumn("annCumReturnCompound",when(col("annCumReturnCompound") <= -9e7, -9e7).otherwise(col("annCumReturnCompound")))

      DataProcessor.dumpResult("fundAsset", fundAssetDF)

      if (!FileOperator.checkExist("df/fundprofitdf/_SUCCESS")) {
        DataProcessor.writeToDB(fundProfitDF.withColumnRenamed("totalNetAsset", "fundMktVal")
          .withColumn("dailyProfit", round(col("dailyProfit"), 2))
          .withColumn("fundMktVal", round(col("fundMktVal"), 2))
          .withColumn("rate", round(col("rate"), 4))
          .select("userID", "date", "rate", "dailyProfit", "fundMktVal"), "fundprofitdf", true)
      }
    }

    DataProcessor.releaseCache(fundProfitDF)


    val financeHoldAsset = DataProcessor.financeHoldAsset
      .withColumnRenamed("totalNetAsset","financeMktVal")
      .select("userID","date","financeMktVal")


    //total asset
    val assetInfoDF = cashDF.join(otcFundDF, Seq("userID", "date"), "left")
      .join(debtResultDF, Seq("userID", "date"), "left")
      .join(securityFinancingDailyDF,Seq("userID", "date"), "left")
      .join(bankFinancingDailyDF,Seq("userID", "date"), "left")
      .join(innerHoldDF, Seq("userID", "date"), "left").na.fill(0)
      .withColumn("totalAsset", col("totalNetAsset") - col("crdtNetAsset"))
      .withColumn("totalNetAsset", col("totalAsset"))
      .drop("jiao").drop("fund_balance").drop("crdtNetAsset").drop("optAsset").drop("newStockMktVal")
      .cache()

    if (!FileOperator.checkExist("managePosition/_SUCCESS")) {
      val managePosition = DataProcessor.otcFundHold
        .filter(col("date") === DataProcessor.lastDay)
        .filter(col("category") === "manageProd")
        .groupBy("userID").agg(sum("market_value").alias("manageMktVal"))
        .select("userID","manageMktVal")
        .join(assetInfoDF.filter(col("date") === DataProcessor.lastDay)
          .select("userID", "totalAsset"), Seq("userID"),"left")
        .withColumn("managePosition", when(col("totalAsset") <= 0, 0).otherwise(round(col("manageMktVal") / col("totalAsset"),4)))
        .withColumn("managePosition", when(col("managePosition") > 1,1).otherwise(col("managePosition")))
        .select("userID", "managePosition")
      DataProcessor.dumpResult("managePosition",managePosition)
    }

    DataProcessor.releaseCache(DataProcessor.otcFundHold)

    if (!FileOperator.checkExist("receiptPosition/_SUCCESS")) {

      val receiptPosition = DataProcessor.rawSecumShare
        .filter(col("date") === DataProcessor.lastDay)
        .filter(col("PRODTA_NO") === "CZZ")
        .groupBy("userID", "date").agg(sum("marketValue").alias("marketValue"))
        .select("userID","marketValue")
        .join(assetInfoDF.filter(col("date") === DataProcessor.lastDay)
          .select("userID", "totalAsset"), Seq("userID"),"left")
        .withColumn("receiptPosition", when(col("totalAsset") === 0, 0).otherwise(round(col("marketValue") / col("totalAsset"),4)))
        .select("userID", "receiptPosition")

      DataProcessor.dumpResult("receiptPosition",receiptPosition)
    }


    if (!FileOperator.checkExist("assetInfo/_SUCCESS")) {
      DataProcessor.dumpResult("assetInfo",
        assetInfoDF.filter(col("date") === DataProcessor.lastDay)
          .join(reverseRepoDailyDF.filter(col("date") === DataProcessor.lastDay)
          .select("userID", "repoAsset"), Seq("userID"), "left")
          .join(holdFundAsset.filter(col("date") === DataProcessor.lastDay)
            .select("userID","totalNetAsset")
            .withColumnRenamed("totalNetAsset", "fundMktVal"),
            Seq("userID"),"left")
          .join(financeHoldAsset.filter(col("date") === DataProcessor.lastDay)
            .select("userID","financeMktVal"),Seq("userID"),"left")
          .withColumnRenamed("current_balance","currentBalance")
          .withColumn("repoPosition", when(col("totalAsset") <= 0, 0).otherwise(round(col("repoAsset") / col("totalAsset"),4)))
          .withColumn("repoPosition", round(col("repoPosition"),4))
          .withColumn("repoPosition", when(col("repoPosition") > 1,1).otherwise(col("repoPosition")))
          .withColumn("bondPosition", when(col("totalAsset") <= 0, 0).otherwise(round(col("bondMktVal") / col("totalAsset"),4)))
          .withColumn("bondPosition", round(col("bondPosition"),4))
          .withColumn("bondPosition", when(col("bondPosition") > 1,1).otherwise(col("bondPosition")))
          .withColumn("bankPosition", when(col("totalAsset") <= 0, 0).otherwise(round(col("bankFinance") / col("totalAsset"),4)))
          .withColumn("bankPosition", when(col("bankPosition") > 1,1).otherwise(col("bankPosition")))
          .withColumn("fundPosition", when(col("totalAsset") <= 0, 0).otherwise(round(col("fundMktVal") / col("totalAsset"),4)))
          .withColumn("fundPosition", round(col("fundPosition"),4))
          .withColumn("fundPosition", when(col("fundPosition") > 1,1).otherwise(col("fundPosition")))
          .withColumn("financePosition", when(col("totalAsset") <= 0, 0).otherwise(round(col("financeMktVal") / col("totalAsset"),4)))
          .withColumn("financePosition", round(col("financePosition"),4))
          .withColumn("financePosition", when(col("financePosition") > 1,1).otherwise(col("financePosition")))
          .withColumn("totalAsset", round(col("totalAsset"), 2))
          .withColumn("currentBalance", round(col("currentBalance"), 2))
          .withColumn("bondMktVal", round(col("bondMktVal"), 2))
          .withColumn("repoAsset", round(col("repoAsset"), 2))
          .withColumn("stockMktVal", round(col("stockMktVal"), 2))
          .withColumn("otcFundMktVal", round(col("otcFundMktVal"), 2))
          .withColumn("innerFundMktVal", round(col("innerFundMktVal"), 2))
          .withColumn("debt", round(col("debt"), 2))
          .withColumn("totalAsset",when(col("totalAsset") >= 9e13, 9e13).otherwise(col("totalAsset")))
          .withColumn("totalAsset",when(col("totalAsset") <= -9e13, -9e13).otherwise(col("totalAsset")))
          .withColumn("stockMktVal",when(col("stockMktVal") >= 9e13, 9e13).otherwise(col("stockMktVal")))
          .withColumn("stockMktVal",when(col("stockMktVal") <= -9e13, -9e13).otherwise(col("stockMktVal")))
          .withColumn("financePosition",when(col("financePosition") >= 9e7, 9e7).otherwise(col("financePosition")))
          .withColumn("financePosition",when(col("financePosition") <= -9e7, -9e7).otherwise(col("financePosition")))
          .select("userID", "totalAsset","currentBalance","bondMktVal","repoAsset","stockMktVal","otcFundMktVal",
            "innerFundMktVal","debt","repoPosition","bondPosition","bankPosition","fundPosition","financePosition"))
    }


    if (!FileOperator.checkExist("capitalUtilize/_SUCCESS")) {
      val capitalUtilize = assetInfoDF.filter(col("date") >= DataProcessor.month1AgoDate)
        .select("userID","current_balance","totalAsset")
        .groupBy("userID").agg(mean("current_balance").alias("currentBalance"), mean("totalAsset").alias("totalAsset"))
        .withColumn("utilize", when(col("totalAsset") === 0, 0).otherwise(col("currentBalance") / col("totalAsset")))
        .withColumn("utilize", round(lit(1) - col("utilize"),4))
        .select("userID", "utilize")
      DataProcessor.dumpResult("capitalUtilize",capitalUtilize)
    }

    if (!FileOperator.checkExist("assetAnalysis/_SUCCESS")) {
      val assetAnalysis = assetInfoDF
        .filter(col("date")>=DataProcessor.startDate && col("date")<=DataProcessor.endDate)
        .select("userID","totalAsset","totalNetAsset")
        .groupBy("userID")
        .agg(max("totalAsset").alias("maxAsset"),
          mean("totalAsset").alias("meanAsset"),
          max("totalNetAsset").alias("maxNetAsset"),
          mean("totalNetAsset").alias("meanNetAsset"))
        .select("userID", "maxAsset","meanAsset","maxNetAsset","meanNetAsset")
        .withColumn("maxAsset", round(col("maxAsset"),2))
        .withColumn("maxAsset",when(col("maxAsset") >= 9e13, 9e13).otherwise(col("maxAsset")))
        .withColumn("meanAsset", round(col("meanAsset"),2))
        .withColumn("meanAsset",when(col("meanAsset") >= 9e13, 9e13).otherwise(col("meanAsset")))
        .withColumn("maxNetAsset", round(col("maxNetAsset"),2))
        .withColumn("maxNetAsset",when(col("maxNetAsset") >= 9e13, 9e13).otherwise(col("maxNetAsset")))
        .withColumn("meanNetAsset", round(col("meanNetAsset"),2))
        .withColumn("meanNetAsset",when(col("meanNetAsset") >= 9e13, 9e13).otherwise(col("meanNetAsset")))
      DataProcessor.dumpResult("assetAnalysis",assetAnalysis)
    }


    if (!FileOperator.checkExist("stockPosition/_SUCCESS")) {
      // 港股A股B股的持仓
      val holdABHKDF: DataFrame = DataProcessor.holdDF
        .filter(col("date") === DataProcessor.lastDay)
        .filter(col("category")==="stock")
        .filter(col("exchangeCD") === "1" || col("exchangeCD") === "2"
          || col("exchangeCD") === "D" || col("exchangeCD") === "H"
          || col("exchangeCD") === "G")
        .withColumn("mktValA", when(col("exchangeCD") === "1" || col("exchangeCD") === "2", col("stockMktVal")).otherwise(0))
        .withColumn("mktValB", when(col("exchangeCD") === "D" || col("exchangeCD") === "H", col("stockMktVal")).otherwise(0))
        .withColumn("mktValHK", when(col("exchangeCD") === "G", col("stockMktVal")).otherwise(0))
        .groupBy("userID")
        .agg(sum("mktValA").alias("mktValA"),sum("mktValB").alias("mktValB"),sum("mktValHK").alias("mktValHK"))
        .select("userID","mktValA","mktValB","mktValHK")

      val stockPosition =  assetInfoDF.filter(col("date") === DataProcessor.lastDay)
        .select("userID","totalAsset")
        .join(holdABHKDF, Seq("userID"),"left")
        .withColumn("positionA",when(col("totalAsset") === 0, 0).otherwise(col("mktValA") / col("totalAsset")))
        .withColumn("positionB",when(col("totalAsset") === 0, 0).otherwise(col("mktValB") / col("totalAsset")))
        .withColumn("positionHK",when(col("totalAsset") === 0, 0).otherwise(col("mktValHK") / col("totalAsset")))
        .withColumn("positionA", round(col("positionA"),4))
        .withColumn("positionB", round(col("positionB"),4))
        .withColumn("positionHK", round(col("positionHK"),4))
        .select("userID","positionA","positionB","positionHK")
      DataProcessor.dumpResult("stockPosition",stockPosition)
    }


    //收益率df
    val profitDF = assetInfoDF.withColumn("share", col("totalNetAsset"))
      .withColumn("net", lit(1.0))
      .select("userID", "date", "totalNetAsset", "share", "net")
      .join(bankFlowIO, Seq("userID", "date"), "left")
      .select("userID", "date", "totalNetAsset", "capital", "share", "net")
      .withColumn("capital" ,when(col("capital").isNull, 0d).otherwise(col("capital")))

      .rdd.map(row => new ProfitBeanNew(row.getString(0), row.getString(1), row.getDouble(2), row.getDouble(3), row.getDouble(4), row.getDouble(5), 0, 0))
      .groupBy(row => row.getUserID)
      .map(x => ProfitUtil.calRateRecursive(x._2)).flatMap(x => x).toDF()
      .repartition(col("userID"))
      .withColumnRenamed("dailyReturn","dailyProfit")
      // 这里做一下容错
      .withColumn("rate", when(col("rate") > 1.5,1).otherwise(col("rate")))
      .withColumn("rate", when(col("rate") < 0.5,1).otherwise(col("rate")))

    val assetDF = assetInfoDF
      .join(profitDF.select("userID", "date", "rate", "dailyProfit"), Seq("userID", "date"), "left")
      .withColumn("dailyReturn", col("rate") - lit(1))
      .withColumn("position", col("stockMktVal") / col("totalAsset"))


      .withColumn("glRatio", col("debt") / col("totalAsset"))
      .withColumn("ordRatio", col("totalNetAsset") / col("totalAsset"))
      .join(mktBasicIdxDF.select("date","hs300"), Seq("date"), "leftouter")
      .na.fill(0)
      .withColumn("dailyReturnHS300", col("dailyReturn") - col("hs300")).na.fill(0)
      .withColumn("label", col("dailyReturn") - lit(0.03))
      .withColumn("returnDir", when(col("dailyReturn") < 0, "loss").otherwise("win"))
      .drop(col("securityFinance")).drop(col("bankFinance"))

      .repartition(col("userID"))
      .cache()

    DataProcessor.releaseCache(assetInfoDF)

    if (!FileOperator.checkExist("df/assetdf/_SUCCESS")) {
      DataProcessor.writeToDB(assetDF
        .withColumn("totalAsset", round(col("totalAsset"), 2))
        .withColumn("totalNetAsset", round(col("totalNetAsset"), 2))
        .withColumn("rate", round(col("rate"), 4))
        .withColumnRenamed("current_balance", "currentBalance")
        .withColumn("currentBalance", round(col("currentBalance"), 2))
        .withColumn("dailyProfit", round(col("dailyProfit"), 2))
        .select("date", "userID", "totalAsset", "totalNetAsset",
          "rate", "dailyProfit", "currentBalance", "position"), "assetdf", true)
    }


    if (!FileOperator.checkExist("profitAnalysis/_SUCCESS")) {
      // 正常的protfitAnalysis
      val profitAnalysis = getProfitAnalysisByAssetDF(assetDF)
      DataProcessor.dumpResult("profitAnalysis", profitAnalysis)
    }
    if (sc.getConf.get("spark.schedule.batch").equals("1") && !FileOperator.checkExist("profitAnalysisDetail/_SUCCESS")) {
      // 需要的1月以来3月以来6月以来
      val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
      val now = LocalDate.parse(DataProcessor.endDate, formatter)
      val month1before = now.minusMonths(1).format(formatter)
      val month3before = now.minusMonths(3).format(formatter)
      val month6before = now.minusMonths(6).format(formatter)
      val profitAnalysis1 = getProfitAnalysisByAssetDF(assetDF, month1before)
        .select("userID", "maxDrawdown", "irHS300", "annStd","annCumReturn")
        .withColumnRenamed("maxDrawdown" ,"maxDrawdown1M")
        .withColumnRenamed("irHS300" ,"ir1M")
        .withColumnRenamed("annStd" ,"annStd1M")
        .withColumnRenamed("annCumReturn" ,"annCumReturn1M")
      val profitAnalysis3 = getProfitAnalysisByAssetDF(assetDF, month3before)
        .select("userID", "maxDrawdown", "irHS300", "annStd","annCumReturn")
        .withColumnRenamed("maxDrawdown" ,"maxDrawdown3M")
        .withColumnRenamed("irHS300" ,"ir3M")
        .withColumnRenamed("annStd" ,"annStd3M")
        .withColumnRenamed("annCumReturn" ,"annCumReturn3M")
      val profitAnalysis6 = getProfitAnalysisByAssetDF(assetDF, month6before)
        .select("userID", "maxDrawdown", "irHS300", "annStd","annCumReturn")
        .withColumnRenamed("maxDrawdown" ,"maxDrawdown6M")
        .withColumnRenamed("irHS300" ,"ir6M")
        .withColumnRenamed("annStd" ,"annStd6M")
        .withColumnRenamed("annCumReturn" ,"annCumReturn6M")
      val profitAnalysisDetail = profitAnalysis6.join(profitAnalysis3 ,Seq("userID"), "outer")
        .join(profitAnalysis1, Seq("userID"), "outer")
        .na.fill(0)
      DataProcessor.dumpResult("profitAnalysisDetail", profitAnalysisDetail)
    }


    // 根据asset算仓位
    var positionSummary : DataFrame = null
    if (!FileOperator.checkExist("positionSummary/_SUCCESS")) {
      positionSummary =  assetDF.select("userID","date","totalAsset","innerMktVal","otcFundMktVal", "current_balance","bondMktVal")
        .filter(col("date")>=DataProcessor.startDate && col("date")<=DataProcessor.endDate)
        .join(stockNetAssetDF.select("userID","date","totalNetAsset").withColumnRenamed("totalNetAsset", "stockMktVal"), Seq("userID","date"),"left")
        .join(holdFundAsset.select("userID","date","totalNetAsset").withColumnRenamed("totalNetAsset", "fundMktVal"), Seq("userID","date"),"left")
        .join(reverseRepoDailyDF, Seq("userID","date"), "left")
        .join(financeHoldAsset, Seq("userID","date"), "left").na.fill(0)
        .withColumn("totalPosition", when(col("totalAsset") === 0, 0).otherwise(lit(1) - col("current_balance") / col("totalAsset")))
        .withColumn("totalPosition", when(col("totalPosition") > 1,1).otherwise(col("totalPosition")))
//      // 基金每天仓位
        .withColumn("fundPosition", when(col("totalAsset") <= 0, 0).otherwise(col("fundMktVal") / col("totalAsset")))
        .withColumn("fundPosition", when(col("fundPosition") > 1,1).otherwise(col("fundPosition")))
        // 股票每天仓位
        .withColumn("stockPosition", when(col("totalAsset") <= 0, 0).otherwise(col("stockMktVal") / col("totalAsset")))
        .withColumn("stockPosition", when(col("stockPosition") > 1,1).otherwise(col("stockPosition")))
        // 债券每天仓位
        .withColumn("bondPosition", when(col("totalAsset") <= 0, 0).otherwise(col("bondMktVal") / col("totalAsset")))
        .withColumn("bondPosition", when(col("bondPosition") > 1,1).otherwise(col("bondPosition")))
        // 逆回购每天仓位
        .withColumn("repoPosition", when(col("totalAsset") <= 0, 0).otherwise(col("repoAsset") / col("totalAsset")))
        .withColumn("repoPosition", when(col("repoPosition") > 1,1).otherwise(col("repoPosition")))
        // 理财每天仓位
        .withColumn("financePosition", when(col("totalAsset") <= 0, 0).otherwise(col("financeMktVal") / col("totalAsset")))
        .withColumn("financePosition", when(col("financePosition") > 1,1).otherwise(col("financePosition")))

        .select("userID","date","totalAsset","totalPosition","financePosition","fundPosition","stockPosition","bondPosition","repoPosition")
        .groupBy("userID")
        .agg(round(mean("totalPosition"),4).alias("meanPosition"),
          round(max("totalPosition"),4).alias("maxPosition"),
          round(stddev("totalPosition"),4).alias("std"),
          round(mean("financePosition"),4).alias("finanMeanPosition"),
          round(max("financePosition"),4).alias("finanMaxPosition"),
          round(mean("fundPosition"),4).alias("fundMeanPosition"),
          round(max("fundPosition"),4).alias("fundMaxPosition"),
          round(mean("bondPosition"),4).alias("bondMeanPosition"),
          round(max("bondPosition"),4).alias("bondMaxPosition"),
          round(mean("repoPosition"),4).alias("repoMeanPosition"),
          round(max("repoPosition"),4).alias("repoMaxPosition"),
          round(mean("stockPosition"),4).alias("stockMeanPosition"),
          round(max("stockPosition"),4).alias("stockMaxPosition"))
        .withColumn("stability",round(col("std")/col("meanPosition"),4))

        .select("userID", "meanPosition", "maxPosition", "stability","finanMeanPosition","finanMaxPosition"
          ,"fundMeanPosition", "fundMaxPosition","stockMeanPosition","stockMaxPosition","bondMeanPosition","bondMaxPosition",
        "repoMeanPosition","repoMaxPosition")
      DataProcessor.dumpResult("positionSummary", positionSummary)
    }

    if (!FileOperator.checkExist("capitalAnalysis/_SUCCESS")) {
      var potential = assetDF.select("userID", "date", "totalAsset")
        .filter(col("date")>=DataProcessor.startDate && col("date")<=DataProcessor.endDate)
        .groupBy("userID")
        .agg(round(mean("totalAsset"), 4).alias("meanTotalAsset"), round(max("totalAsset"), 4).alias("maxTotalAsset"))
        .withColumn("potential", when(col("meanTotalAsset") > 0, round((col("maxTotalAsset") - col("meanTotalAsset")) / col("meanTotalAsset"),4)).otherwise(0))
        .withColumn("potential",when(col("potential") > 1E6, 1E6 - 1).otherwise(col("potential")))
        .withColumn("potential",round(col("potential"),4))
        .select("userID", "potential")

      //
      val bankFlowOut: DataFrame = capitalFlowDF
        .filter(col("date")>=DataProcessor.startDate && col("date")<=DataProcessor.endDate)
        .filter(
        col("business_flag") === 2041 || col("business_flag") === 2042
          || col("business_flag") === 2007 || col("business_flag") === 2008
          || col("business_flag") === 31282 || col("business_flag") === 31283
        )
        .groupBy("userID").count()
        .withColumnRenamed("count", "bankOutFreq")
        .withColumn("bankOutFreq",round(col("bankOutFreq") / 12,4))

      potential = potential.join(bankFlowOut, Seq("userID"), "outer")
        .select("userID", "potential", "bankOutFreq")

      DataProcessor.dumpResult("capitalAnalysis", potential)
    }
  }


  def getProfitAnalysisByAssetDF(assetDF: DataFrame, date : String): DataFrame = {
    val tmp = assetDF.filter(col("date") > date)
    getProfitAnalysisByAssetDF(tmp)
  }

  def getProfitAnalysisByAssetDF(assetDF: DataFrame): DataFrame = {
    assetDF.groupBy("userID")
      .agg(round(sum("dailyProfit"), 2).alias("profit"),
        calMaxDrawdownDetail(collect_list("dailyReturn")).alias("tuple"),
        calMaxDrawdown(collect_list("dailyReturnHS300")).alias("tupleHS300"),
        count("*").alias("tradeDays"),
        (stddev("dailyReturn") * math.sqrt(250)).alias("annStd"),
        (stddev("dailyReturnHS300") * math.sqrt(250)).alias("annStdHS300"),
        (mean("dailyReturnHS300") * 250).alias("annExpRetHS300")
       )

      .withColumn("cumReturn", round(col("tuple").getItem("annReturn"), 4))
      .withColumn("cumReturn", when(col("cumReturn") > 1000, lit(1000)).otherwise(col("cumReturn")))
      .withColumn("cumReturn", when(col("cumReturn") < -1000, lit(-1000)).otherwise(col("cumReturn")))

      .withColumn("cumReturnHS300", round(col("tupleHS300").getItem("annReturn"), 4))

      .withColumn("maxDrawdown", round(col("tuple").getItem("maxDrawdown"), 4))
      .withColumn("maxDrawdown", when(col("maxDrawdown") <= -1e8, lit(-1e8 + 1)).otherwise(col("maxDrawdown")))
      .withColumn("maxDrawdown", when(col("maxDrawdown") >= 1e8, lit(1e8 - 1)).otherwise(col("maxDrawdown")))
      .withColumn("annCumReturn", round(DataProcessor.convert2Year(col("cumReturn"), col("tradeDays")), 4))
      .withColumn("annCumReturn", when(col("annCumReturn") <= -1e8, lit(-1e8 + 1)).otherwise(col("annCumReturn")))
      .withColumn("annCumReturn", when(col("annCumReturn") >= 1e8, lit(1e8 - 1)).otherwise(col("annCumReturn")))
      .withColumn("annActiveCumReturn", round(DataProcessor.convert2Year(col("cumReturnHS300"), col("tradeDays")), 4))
      .withColumn("annActiveCumReturn", when(col("annActiveCumReturn") <= -1e6, lit(-1e6 + 1)).otherwise(col("annActiveCumReturn")))
      .withColumn("annActiveCumReturn", when(col("annActiveCumReturn") >= 1e6, lit(1e6 - 1)).otherwise(col("annActiveCumReturn")))
      .withColumn("sharpRatio", round((col("annCumReturn") - lit(0.03)) / col("annStd"), 4))
      .withColumn("sharpRatio", when(col("sharpRatio") <= -1e8, lit(-1e8 + 1)).otherwise(col("sharpRatio")))
      .withColumn("sharpRatio", when(col("sharpRatio") >= 1e8, lit(1e8 - 1)).otherwise(col("sharpRatio")))



      .withColumn("drawDownDayRatio", round(col("tuple").getItem("drawDownDayRatio"), 4))
      .withColumn("drawDownDayRatio", when(col("drawDownDayRatio") <= -1e8, lit(-1e8 + 1)).otherwise(col("drawDownDayRatio")))
      .withColumn("drawDownDayRatio", when(col("drawDownDayRatio") >= 1e8, lit(1e8 - 1)).otherwise(col("drawDownDayRatio")))
      .withColumn("annDrawDownValue", round(col("tuple").getItem("annDrawDownValue"), 4))
      .withColumn("annDrawDownValue", when(col("annDrawDownValue") <= -1e8, lit(-1e8 + 1)).otherwise(col("annDrawDownValue")))
      .withColumn("annDrawDownValue", when(col("annDrawDownValue") >= 1e8, lit(1e8 - 1)).otherwise(col("annDrawDownValue")))


      .withColumn("irHS300", round(col("annExpRetHS300") / col("annStdHS300"), 4)).na.fill(0)
      .withColumn("irHS300", when(col("irHS300") <= -1e8, lit(-1e8 + 1)).otherwise(col("irHS300")))
      .withColumn("irHS300", when(col("irHS300") >= 1e8, lit(1e8 - 1)).otherwise(col("irHS300")))
      .withColumn("annStd", round(col("annStd"), 4))
      .withColumn("annStd", when(col("annStd") <= -1e8, lit(-1e8 + 1)).otherwise(col("annStd")))
      .withColumn("annStd", when(col("annStd") >= 1e8, lit(1e8 - 1)).otherwise(col("annStd")))

      .withColumn("profit", when(col("profit") <= -1e11, lit(-1e11 + 1)).otherwise(col("profit")))
      .withColumn("profit", when(col("profit") >= 1e11, lit(1e11 - 1)).otherwise(col("profit")))

      .withColumn("annCumReturnCompound", round(DataProcessor.convert2YearCompund(col("cumReturn"), col("tradeDays")), 4))
      .withColumn("annCumReturnCompound", when(col("annCumReturnCompound") <= -1e8, lit(-1e8 + 1)).otherwise(col("annCumReturnCompound")))
      .withColumn("annCumReturnCompound", when(col("annCumReturnCompound") >= 1e8, lit(1e8 - 1)).otherwise(col("annCumReturnCompound")))
      .withColumn("sharpRatioCompound", round((col("annCumReturnCompound") - lit(0.03)) / col("annStd"), 4))
      .withColumn("sharpRatioCompound", when(col("sharpRatioCompound") <= -1e8, lit(-1e8 + 1)).otherwise(col("sharpRatioCompound")))
      .withColumn("sharpRatioCompound", when(col("sharpRatioCompound") >= 1e8, lit(1e8 - 1)).otherwise(col("sharpRatioCompound")))

      .select("userID", "profit", "cumReturn", "cumReturnHS300","annCumReturn", "annActiveCumReturn", "annCumReturnCompound", "annStd",
        "maxDrawdown","sharpRatio","sharpRatioCompound","irHS300","drawDownDayRatio","annDrawDownValue")
  }

}
