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

import com.data.spark.sparksql.bdb.util.{ProfitUtil, DataProcessor, FileOperator}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{abs, _}
import org.apache.log4j.Logger
/**
  * Created by liang on 17-5-25.
  */
class FundAnalyzer(session: SQLContext,sc: SparkContext) {
  val logger = Logger.getLogger(getClass.getName)

  val calMaxDrawdown = udf { (dailyReturns: Seq[Double]) => {
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

  val mapFundType = udf { (fundType: String) => {
    if (fundType != null) {
      fundType match {
        case "创新封闭式基金" => "封闭式基金"
        case "传统封闭式基金" => "封闭式基金"
        case "平衡混合型" => "混合型"
        case "混合债券型基金（一级）" => "混合型"
        case "被动指数型" => "股票型"
        case "偏股混合型" => "混合型"
        case "增强指数型" => "股票型"
        case "混合债券型基金（二级）" => "混合型"
        case "货币型" => "货币型"
        case "保本基金" => "混合型"
        case "偏债混合型" => "混合型"
        case "普通股票型" => "股票型"
        case "中短期纯债券型" => "债券型"
        case "QDII基金" => "QDII型"
        case "长期纯债券型" => "债券型"
        case "商品型基金" => "商品型"
        case "FOF" => "FOF"
        case _ => null
      }
    } else {
      null
    }
  }
  }


  def analyse(): Unit = {
    import session.implicits._

    val wStockTradeValDesc = Window.partitionBy(col("userID")).orderBy(col("totalFloatVal").desc)
    val wStockTradeVal = Window.partitionBy(col("userID")).orderBy(col("totalFloatVal"))

    // 基金大中小盘成长价值风格
    val fundInvestStyle = DataProcessor.readData("fundInvestStyle")
      .withColumnRenamed("ticker_symbol","stockID")
      .select("stockID","vertical","horizontal")


    val getMonth = udf{s: String => s.substring(0,6)}
      val otcFundTrade = DataProcessor.otcFundTrade
          .withColumn("tradeVal",when((col("business_flag") === 122) || (col("business_flag") === 120) || (col("business_flag") === 139),lit(-1)*col("balance")).otherwise(col("balance")))
        .withColumn("num",when((col("business_flag") === 122) || (col("business_flag") === 120) || (col("business_flag") === 139) || (col("business_flag") === 143),lit(-1)*col("shares")).otherwise(col("shares")))
        .join(DataProcessor.otcFundCategory,Seq("stockID"))
        .withColumnRenamed("category","stockType")
        .withColumnRenamed("init_date","date")
        .select("userID","stockID","stockType","themeCategory","tradeVal","business_flag","date","num","current_share","establishDate","managementCompany")
        .cache()




    // 这里的筛选条件需要注意
      val floorFundTrade = DataProcessor.tradeDF
        .filter(col("category") === "fund")
        .filter(col("num") > 0)
        .select("userID","stockID","stockType","tradeVal","date","num")

    val otcFundBuy = otcFundTrade.filter((col("business_flag") === 122) || (col("business_flag") === 120) || (col("business_flag") === 139))
      .select("userID","stockID","stockType","themeCategory","tradeVal","date","num","establishDate","managementCompany")

    if (!FileOperator.checkExist("fundScatter/_SUCCESS")) {

        val fundScatter = DataProcessor.otcFundHold.groupBy("userID","date","stockID").agg(sum("market_value").alias("mktVal"))
            .withColumn("dailyMktVal",sum("mktVal").over(Window.partitionBy("userID","date")))
            .withColumn("position",col("mktVal")/col("dailyMktVal"))
          .withColumn("entropy", col("position") * log(2, col("position")) * lit(-1))
          .groupBy("userID", "date").agg(sum("entropy").alias("dailyEntropy"))
          .groupBy("userID").agg(round(mean("dailyEntropy"), 4).alias("fundScatter")).na.fill(0)
        DataProcessor.dumpResult("fundScatter", fundScatter)


    }

    /*-------基金年限偏好---------*/
    if(!FileOperator.checkExist("fundHoldDuration/_SUCCESS")){
      val castItem = sc.broadcast(DataProcessor.tradeDateList)

      val calcHoldDuration = udf((ys: Seq[String]) => {
        var totalHoldDays = 0.
        var holdTimes = 0.
        var maxHoldDays = 0.
        var tmpMaxHoldDays = 0.
        val tradeDateList = castItem.value

        // 0代表未持有 1代表持有
        var status = 0
        for (i <- 0 to tradeDateList.size() - 1) {
          val tmpDate = tradeDateList.get(i)

          if (ys.contains(tmpDate)) {
            // 认为当天持有
            if (status == 0) {
              totalHoldDays = totalHoldDays + 1
              status = 1
              tmpMaxHoldDays = 1
            } else if (status == 1) {
              totalHoldDays = totalHoldDays + 1
              tmpMaxHoldDays = tmpMaxHoldDays + 1
            }
          } else {
            if (status == 1) {
              holdTimes = holdTimes + 1
              status = 0
              if (tmpMaxHoldDays >= maxHoldDays) {
                maxHoldDays = tmpMaxHoldDays
              }
              tmpMaxHoldDays = 0
            } else if (status == 0) {

            }
          }
        }

        if (ys.last.equalsIgnoreCase(tradeDateList.get(tradeDateList.size() - 1)) || ys.last > tradeDateList.get(tradeDateList.size() - 1)) {
          holdTimes += 1
          if (tmpMaxHoldDays > maxHoldDays) {
            maxHoldDays = tmpMaxHoldDays
          }
        }
        Map("max" -> maxHoldDays, "mean" -> totalHoldDays / holdTimes)
      })

      val fundHoldDuration = DataProcessor.fundHoldDf
        .filter(col("market_value") > 0)
        .select("userID","date","stockID")
        .repartition(col("userID"), col("stockID"))
        .sortWithinPartitions("userID", "stockID", "date")
        .groupBy("userID", "stockID")
        .agg(collect_list("date").alias("dateList"))
        .withColumn("result", calcHoldDuration(col("dateList")))
        .withColumn("singleDurationMax", col("result").getItem("max"))
        .withColumn("singleDurationMean", col("result").getItem("mean"))
        .groupBy("userID")
        .agg(round(mean("singleDurationMean"), 1).alias("meanDuration"), round(max("singleDurationMax"),1).alias("maxDuration"))
        .select("userID","meanDuration","maxDuration")

      DataProcessor.dumpResult("fundHoldDuration",fundHoldDuration)
    }


    if(!FileOperator.checkExist("fundHoldNum/_SUCCESS")){
      val fundHoldNum = DataProcessor.fundHoldDf
        .filter(col("market_value") > 0)
        .groupBy("userID", "date")
        .agg(count("*").alias("count"))
        .groupBy("userID")
        .agg(round(mean("count"), 1).alias("meanNum"), max("count").alias("maxNum"))
        .select("userID", "meanNum", "maxNum")
      DataProcessor.dumpResult("fundHoldNum",fundHoldNum)
    }

    /*-------基金公司偏好---------*/
    if(!FileOperator.checkExist("fundCompanyPrefer/_SUCCESS")){
      val fundCompanyPrefer = otcFundBuy
        .groupBy("userID","managementCompany").agg(sum("tradeVal").alias("tradeVal"))
        .withColumn("rowNum",row_number().over(Window.partitionBy("userID").orderBy(col("tradeVal").desc)))
        .filter(col("rowNum") <= DataProcessor.keepNum)
        .groupBy("userID").agg(collect_list("managementCompany").alias("managementCompany"))
        .withColumn("preferFundCompany",DataProcessor.joinStrings(col("managementCompany")))
        .select("userID", "preferFundCompany")
      DataProcessor.dumpResult("fundCompanyPrefer",fundCompanyPrefer)
    }


    if(!FileOperator.checkExist("fundStyle/_SUCCESS")) {
      val fundStyle = otcFundBuy.select("userID","stockID","tradeVal")
        .join(fundInvestStyle,Seq("stockID"),"inner")
        .withColumn("fundStyle",DataProcessor.stringAddWithBlank(col("vertical"),col("horizontal")))
        .groupBy("userID","fundStyle")
        .agg(sum("tradeVal").alias("typeVal"))
        .withColumn("rank", row_number().over(Window.partitionBy("userID").orderBy(col("typeVal").desc)))
        .filter(col("rank") <= 1)
        .select("userID","fundStyle")
      DataProcessor.dumpResult("fundStyle",fundStyle)
    }

    /*-------基金评级偏好---------*/
    if(!FileOperator.checkExist("fundRating/_SUCCESS")){
      val fundRating = otcFundBuy.withColumn("month",getMonth(col("date")))
        .join(DataProcessor.otcFundRating,Seq("stockID","month"))
        .filter(col("overallRating").isNotNull)
        .groupBy("userID").agg(round(mean("overallRating"),4).alias("overallRating"))
        .select("userID","overallRating")
      DataProcessor.dumpResult("fundRating",fundRating)
    }

    /*-------用户选基能力---------*/
    if(!FileOperator.checkExist("fundSelect/_SUCCESS")){

        val fundSelect = DataProcessor.otcFundHold.select("userID", "stockID", "date")
          .join(DataProcessor.otcFundRank.select("stockID","date","rankReturnRate3M"), Seq("stockID", "date"))
          // 手动过滤掉置为-1的
          .filter(col("rankReturnRate3M") !== -1)
          .groupBy("userID").agg(round(mean("rankReturnRate3M"),4).alias("fundSelect"))
          .select("userID", "fundSelect")
        DataProcessor.dumpResult("fundSelect", fundSelect)

    }

    /*-------用户基金短期业绩偏好---------*/
    if(!FileOperator.checkExist("fundPerformancePrefer/_SUCCESS")){

        val fundPerformancePrefer = otcFundBuy.select("userID", "stockID", "date")
          .join(DataProcessor.otcFundRank.select("stockID","date","rankReturnRate1M"), Seq("stockID", "date"))
          // 手动过滤掉置为-1的
          .filter(col("rankReturnRate1M") !== -1)
          .groupBy("userID").agg(round(mean("rankReturnRate1M"),4).alias("fundPerformancePrefer"))
          .select("userID", "fundPerformancePrefer")
        DataProcessor.dumpResult("fundPerformancePrefer", fundPerformancePrefer)

    }

    /*-------计算用户基金品种偏好-----------*/
    if (!FileOperator.checkExist("userPreferFund/_SUCCESS")) {
      val fundDF = floorFundTrade.unionAll(otcFundBuy.select("userID","stockID","stockType","tradeVal","date","num"))
      val preferFundDF = fundDF
        .withColumn("stockType",mapFundType(col("stockType")))
        .filter(col("stockType").isNotNull)
        .withColumn("typeVal", sum("tradeVal").over(Window.partitionBy("userID", "stockType")))
        .select("userID", "stockType", "typeVal").dropDuplicates
        .withColumn("typeVal",abs(col("typeVal")))
        .withColumn("sumVal", sum("typeVal").over(Window.partitionBy("userID")))
        .withColumn("weight", round(col("typeVal") / col("sumVal"), 4)).drop("typeVal").drop("sumVal")
        .withColumn("rank",row_number().over(Window.partitionBy("userID").orderBy(col("weight").desc)))
        .filter($"rank"<=DataProcessor.keepNum)
        .groupBy("userID").agg(collect_list("stockType").alias("stockTypes"), collect_list("weight").alias("weights"))
        .withColumn("stockTypes", DataProcessor.joinStrings(col("stockTypes")))
        .withColumn("weights", DataProcessor.joinDoubles(col("weights")))
        .select("userID","stockTypes","weights")
      DataProcessor.dumpResult("userPreferFund", preferFundDF)
    }
    /*-------计算用户基金主题偏好-----------*/
    if (!FileOperator.checkExist("userPreferFundTheme/_SUCCESS")) {
      val preferFundThemeDF = otcFundBuy.withColumn("typeVal", sum("tradeVal").over(Window.partitionBy("userID", "themeCategory")))
        .select("userID", "themeCategory", "typeVal").dropDuplicates
        .withColumn("typeVal",abs(col("typeVal")))
        .withColumn("sumVal", sum("typeVal").over(Window.partitionBy("userID")))
        .withColumn("weight", round(col("typeVal") / col("sumVal"), 4)).drop("typeVal").drop("sumVal")
        .withColumn("rank",row_number().over(Window.partitionBy("userID").orderBy(col("weight").desc)))
        .filter($"rank"<=DataProcessor.keepNum)
        .groupBy("userID").agg(collect_list("themeCategory").alias("themeCategories"), collect_list("weight").alias("weights"))
        .withColumn("themeCategories", DataProcessor.joinStrings(col("themeCategories")))
        .withColumn("weights", DataProcessor.joinDoubles(col("weights")))
        .select("userID", "themeCategories", "weights")
      DataProcessor.dumpResult("userPreferFundTheme", preferFundThemeDF)
    }
    /*-------计算用户定投时间偏好-----------*/
    if(!FileOperator.checkExist("fundInvestBehavior/_SUCCESS")){

      def mapStage = udf((t: Int) => {
        t match {
          case t if t == 0 => "stage1"
          case t if t == 1 => "stage2"
          case t if t == 2 => "stage3"
          case _ => "stage4"
        }
      })

      val fixedInvestDetail = otcFundTrade.withColumn("fixedFlag",when(col("business_flag") === 139,1).otherwise(0))
        .withColumn("fixedVal",$"fixedFlag"*$"tradeVal")
       val fixedInvest = fixedInvestDetail.groupBy("userID")
        .agg(sum("tradeVal").alias("totalVal"),sum("fixedVal").alias("fixedVal"))
        .withColumn("fixedBuyRatio",$"fixedVal"/$"totalVal")
      val fixedDF = fixedInvest
        .withColumn("fixedBuyRatio", round(col("fixedBuyRatio"),4))
        .select("userID","fixedBuyRatio")

      DataProcessor.dumpResult("fundInvestBehavior", fixedDF)
    }



      // 场外基金计算收益吧
      val otcGain = DataProcessor.outHoldFundDetail.filter(col("date") === DataProcessor.lastDay)
        .select("userID","date","stockID","market_value")
        .withColumnRenamed("market_value","gain")
      val otcCost = DataProcessor.outHoldFundDetail.filter(col("date") === DataProcessor.firstDay)
        .select("userID","date","stockID","market_value")
        .withColumnRenamed("market_value","cost")

      val otcTrade = DataProcessor.otcFundTrade
        .withColumnRenamed("init_date","date")
        .withColumn("category", DataProcessor.changeCategoryFunc(col("stock_type")))
        .filter(col("category") === "fund")
        .filter(!col("stockID").startsWith("BB"))
        .withColumn("tradeVal", col("nav") * col("shares"))
        .filter(col("business_flag") === 122 || col("business_flag") === 124
          || col("business_flag") === 130 || col("business_flag") === 142
          // 强行调增强行调减
          || col("business_flag") === 144 || col("business_flag") === 145
          || col("business_flag") === 133
          || col("business_flag") === 143)
        .withColumn("outCapital", when(col("business_flag") === 130 || col("business_flag") === 122
          || col("business_flag") === 144, col("tradeVal")).otherwise(-col("tradeVal")))

        .withColumn("outCapital", when(col("business_flag") === 143, -col("balance")).otherwise(col("outCapital")))
        .select("userID","date","stockID","tradeVal","outCapital")
        .filter(col("date") !== DataProcessor.firstDay)
        .groupBy("userID","stockID")
        .agg(sum("outCapital").alias("outCapital"))
        .select("userID","stockID","outCapital")


     val otcFundProfit = otcGain.join(otcCost,Seq("userID","stockID"),"outer")
        .select("userID","stockID","gain","cost")
        .join(otcTrade,Seq("userID","stockID"),"outer")
        .na.fill(0)
        .withColumn("profit", col("gain") - col("cost") + col("outCapital"))
        .select("userID","stockID","profit")



    val innerFundGain = DataProcessor.holdDF.filter(col("date") === DataProcessor.lastDay)
      .filter(col("category") === "fund")
      .select("userID","date","stockID","mktVal")
      .withColumnRenamed("mktVal","gain")

    val innerFundCost = DataProcessor.holdDF.filter(col("date") === DataProcessor.firstDay)
      .filter(col("category") === "fund")
      .select("userID","date","stockID","mktVal")
      .withColumnRenamed("mktVal","cost")


    val assetDFLocal = DataProcessor.assetDF
      .withColumn("prePrice", lag("asset_price",1).over(Window.partitionBy("exchangeCD","stockID").orderBy("date")))
      .withColumn("prePrice", when(col("prePrice").isNull, col("asset_price")).otherwise(col("prePrice")))

    // 担保品
    val collateralDF = DataProcessor.rawTradeDF

      .withColumn("category",DataProcessor.changeCategoryFunc(col("stockType")))
      .repartition(col("userID"))
      // 从信用帐户取担保品
      .filter(col("asset_prop") === "7")
      .filter(col("category") === "fund")
      .filter(col("business_flag") === 4721 || col("business_flag") === 4722)
      .select("userID","date","stockID","num","exchangeCD")
      .join(assetDFLocal,Seq("date","exchangeCD","stockID"), "left")
      // 这里的collateral是存入为正，取出为负
      .withColumn("collateral", col("num") * col("prePrice") * lit(-1))
      .groupBy("userID","stockID")
      .agg(sum(col("collateral")).alias("collateral"))

    val fundCapitalFlowDF = DataProcessor.tradeDF
      .filter(col("date") !== DataProcessor.firstDay)
      .filter(col("category") === "fund")
      // 这里clearbalance卖出为正，capital需要和outcapital保持一致
      .withColumn("capital", col("clear_balance"))
      .groupBy("userId", "stockID").agg(sum("capital").alias("capital"))
      .select("userId", "stockID", "capital")
      // 这里需要把担保品划进去
      .join(collateralDF, Seq("userID","stockID"),"outer")
      .na.fill(0)
      .withColumn("outCapital",col("capital") - col("collateral"))
      .select("userId","stockID","outCapital")


    val innerFundProfitDF = innerFundGain.join(innerFundCost,Seq("userID","stockID"),"outer")
      .select("userID","stockID","gain","cost")
      .join(fundCapitalFlowDF,Seq("userID","stockID"),"outer")
      .na.fill(0)
      .withColumn("totalFloatVal", col("gain") - col("cost") + col("outCapital"))
      .select("userID","stockID","totalFloatVal")



      // 这里存在每只基金上的收益
      val userOtcFundProfit = otcFundProfit
        .withColumnRenamed("profit", "totalFloatVal")
        .unionAll(innerFundProfitDF)
        .withColumn("initRank", row_number().over(wStockTradeValDesc))
        .withColumn("invertRank", row_number().over(wStockTradeVal))
        .select("userID","stockID","initRank","invertRank","totalFloatVal")
        .cache()

      if(!FileOperator.checkExist("df/profitablefunddf/_SUCCESS")) {
        DataProcessor.writeToDB(userOtcFundProfit, "profitablefunddf", true)
      }

      if (!FileOperator.checkExist("profitableFund/_SUCCESS")) {
        val goodFunds = userOtcFundProfit.filter(col("totalFloatVal") > 0 && col("initRank") <= DataProcessor.keepNum)
          .groupBy("userID").agg(collect_list("stockID").alias("stockIDList"))
          .withColumn("goodFund", DataProcessor.joinStrings(col("stockIDList")))
          .select("userID","goodFund")
        val badFunds = userOtcFundProfit.filter(col("totalFloatVal") < 0 && col("invertRank") <= DataProcessor.keepNum)
          .groupBy("userID").agg(collect_list("stockID").alias("stockIDList"))
          .withColumn("badFund", DataProcessor.joinStrings(col("stockIDList")))
          .select("userID","badFund")
        val winRatio = userOtcFundProfit.select("userID","stockID","totalFloatVal")
          .withColumn("win",when(col("totalFloatVal") > 0,1).otherwise(0))
          .groupBy("userID")
          .agg(sum(col("win")).alias("win"), count("*").alias("num"))
          .withColumn("winRatio", col("win") / col("num"))
          .withColumn("winRatio", round(col("winRatio"),4))
          .select("userID","winRatio")

        val fundSelect = goodFunds
          .join(badFunds,Seq("userID"),"outer")
          .join(winRatio,Seq("userID"),"outer")
          .select("userID","goodFund","badFund","winRatio")

        DataProcessor.dumpResult("profitableFund",fundSelect)
      }
      DataProcessor.releaseCache(userOtcFundProfit)



    //剔除券商集合理财
    /*-------计算用户交易持有基金情况-----------*/
    if(!FileOperator.checkExist("fundTurnOver/_SUCCESS")){
      val otcHold = DataProcessor.otcFundHold
        .groupBy("userID","stockID","date").agg(sum("current_share").alias("shares"))
        .filter($"shares">0)
        .groupBy("userID","date").agg(sum("shares").alias("shares"),count("*").alias("num"))
        .groupBy("userID").agg(mean("shares").alias("meanMktVal"),mean("num").alias("meanNum"))
      val otcTradeVal = otcFundTrade.filter(col("business_flag")!==143).groupBy("userID").agg(sum(abs($"tradeVal")).alias("tradeVal"))
      val fundTurnOver = otcHold.join(otcTradeVal, Seq("userID"))
        .withColumn("turnover",col("tradeVal")/col("meanMktVal"))
        .select("userID","turnover")
      DataProcessor.dumpResult("fundTurnOver",fundTurnOver)
    }

    DataProcessor.releaseCache(otcFundTrade)



    // 场内基金最新持仓
    val inLatestHoldFund = DataProcessor.holdDF.filter(col("category")==="fund")
      .filter(col("date") === DataProcessor.lastDay)
      .withColumnRenamed("innerFundMktVal", "market_value")
      .select("userId","date","stockID","market_value","stockType")

    // 暂时把场外基金当作公募基金
    val outlLatestHoldFund = DataProcessor.otcFundHold.filter(col("date") === DataProcessor.lastDay)
      .filter(col("market_value") > 0)
      .withColumnRenamed("stock_type", "stockType")
      .select("userID","date","stockID","market_value","stockType")

    val latestHoldFund = inLatestHoldFund.unionAll(outlLatestHoldFund)
      .cache()

    if  (!FileOperator.checkExist("latestFundTheme/_SUCCESS")) {
      // 当前持有基金的主题
      val holdFundTheme = latestHoldFund.join(DataProcessor.otcFundCategory, Seq("stockID"))
        .groupBy("userId", "themeCategory").agg(sum("market_value").alias("typeVal"))
        .withColumn("sumVal", sum("typeVal").over(Window.partitionBy("userID")))
        .withColumn("weight", round(col("typeVal") / col("sumVal"), 4))
        .drop("typeVal").drop("sumVal")
        .withColumn("rank", row_number().over(Window.partitionBy("userID").orderBy(col("weight").desc)))
        .filter(col("rank") <= DataProcessor.keepNum)
        .groupBy("userID")
        .agg(collect_list("themeCategory").alias("themeCategories"))
        .withColumn("themeCategories", DataProcessor.joinStrings(col("themeCategories")))
        // 少写几个数据总归是好的
        .filter(col("themeCategories") !== "")
        .select("userID", "themeCategories")
      DataProcessor.dumpResult("latestFundTheme", holdFundTheme)
    }

    if (!FileOperator.checkExist("latestFundType/_SUCCESS")) {
      // 当前持有基金的type
      val fundTypeDF = latestHoldFund
        .withColumn("stockType",mapFundType(col("stockType")))
        .filter(col("stockType").isNotNull)
        .groupBy("userID", "stockType").agg(sum("market_value").alias("typeVal"))
        .withColumn("sumVal", sum("typeVal").over(Window.partitionBy("userID")))
        .withColumn("weight", round(col("typeVal") / col("sumVal"), 4)).drop("typeVal").drop("sumVal")
        .filter(col("weight") > 0.2)
        .withColumn("rank",row_number().over(Window.partitionBy("userID").orderBy(col("weight").desc)))
        .filter($"rank"<=DataProcessor.keepNum)
        .groupBy("userID").agg(collect_list("stockType").alias("stockTypes"))
        .withColumn("stockTypes", DataProcessor.joinStrings(col("stockTypes")))
        .select("userID","stockTypes")
      DataProcessor.dumpResult("latestFundType", fundTypeDF)
    }

    if (!FileOperator.checkExist("latestFundCompany/_SUCCESS")) {
      // 当前持有基金的company
      val fundCompanyDF = latestHoldFund.join(DataProcessor.otcFundCategory, Seq("stockID"))
        .groupBy("userId", "managementCompany").agg(sum("market_value").alias("value"))
        .withColumn("sumVal", sum("value").over(Window.partitionBy("userID")))
        .withColumn("weight", round(col("value") / col("sumVal"), 4))
        .drop("value").drop("sumVal")
        .withColumn("rank", row_number().over(Window.partitionBy("userID").orderBy(col("weight").desc)))
        .filter(col("rank") <= DataProcessor.keepNum)
        .groupBy("userID")
        .agg(collect_list("managementCompany").alias("companies"))
        .withColumn("companies", DataProcessor.joinStrings(col("companies")))
        // 少写几个数据总归是好的
        .select("userID", "companies")
      DataProcessor.dumpResult("latestFundCompany", fundCompanyDF)
    }

    if(!FileOperator.checkExist("latestFundRating/_SUCCESS")){
      val fundRating = latestHoldFund.withColumn("month",getMonth(col("date")))
        .join(DataProcessor.otcFundRating,Seq("stockID","month"))
        .groupBy("userID").agg(mean("overallRating").alias("overallRating"))
      DataProcessor.dumpResult("latestFundRating",fundRating)
    }


    if(!FileOperator.checkExist("latestFundStyle/_SUCCESS")) {
      val latestFundStyle = latestHoldFund.join(fundInvestStyle, Seq("stockID"),"inner")
        .withColumn("fundStyle",DataProcessor.stringAddWithBlank(col("vertical"),col("horizontal")))
        .groupBy("userID","fundStyle")
        .agg(sum("market_value").alias("typeVal"))
        .withColumn("rank", row_number().over(Window.partitionBy("userID").orderBy(col("typeVal").desc)))
        .filter(col("rank") <= 1)
        .select("userID","fundStyle")

      DataProcessor.dumpResult("latestFundStyle",latestFundStyle)
    }



    DataProcessor.releaseCache(latestHoldFund)


  }
}
