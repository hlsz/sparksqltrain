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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by liang on 17-3-30.
  */
class AbilityAnalyzer(session: SQLContext) {
  val logger = Logger.getLogger(getClass.getName)
  var totalDF: DataFrame = null
  var flagDF: DataFrame = null
  var stageDF: DataFrame = null
  var staticDF: DataFrame = null
  var benchmarkDF: DataFrame = null


  import session.implicits._

  def analyse(): Unit = {

    val mktBasicIdxDF = DataProcessor.mktBasicIdxDF.filter(col("ticker") === "000300")
      .withColumn("calOrder", row_number().over(Window.partitionBy("ticker").orderBy("date")))
      .select("date", "closeIndex","calOrder")
      .repartition(col("date"))


    val holdW = Window.partitionBy("userID", "stockID").orderBy($"date")
    val holdSplitW = Window.partitionBy("userID", "stockID","pivot").orderBy($"date")
    val holdWDesc = Window.partitionBy("userID", "stockID").orderBy($"date".desc)

    val wStockTradeValDesc = Window.partitionBy($"userID").orderBy($"totalFloatVal".desc)
    val wStockTradeVal = Window.partitionBy($"userID").orderBy($"totalFloatVal")

    val calTotalRate = udf { res_list: Seq[Double] =>
      try {
        var result = 1d
        for (a <- res_list) {
          result = result * a
        }
        result
      } catch {
        case e: Exception => 1d
      }
    }
    val convert2ReverseString = udf { res_list: Seq[String] =>
      try {
        res_list.reverse.mkString(" ")
      } catch {
        case e: Exception => null
      }
    }

    val convert2String = udf { res_list: Seq[String] =>
      try {
        res_list.mkString(" ")
      } catch {
        case e: Exception => null
      }
    }


    totalDF = DataProcessor.mergedDF
        .groupBy("userID","date","stockID")
        .agg(sum("mktVal").alias("mktVal"),sum("tradeVal").alias("tradeVal"),sum("holdNum").alias("holdNum"),
          sum("num").alias("num"))
        .filter((col("holdNum")>=100) || (col("holdNum")===0))
      .withColumn("price",round(abs(col("tradeVal")/col("num")),2))
      .repartition(col("userID"))
      .sortWithinPartitions("userID", "stockID", "date")
      .withColumn("totalMktVal", sum("mktVal").over(Window.partitionBy("userID", "date")))
      .withColumn("mktPosition", col("mktVal") / col("totalMktVal"))
      .withColumn("tradeVal", lit(-1) * col("tradeVal"))
      .withColumn("flag", col("holdNum") / col("num"))
      .withColumn("rowNumDesc", row_number().over(holdWDesc))
      .withColumn("rowNum", row_number().over(holdW))
      // 认为这一天他全部买入，作为成本
      .withColumn("flag", when(col("rowNum") === 1, 1).otherwise(col("flag")))
      // 假设这一天他全部卖出，作为受益
      .withColumn("flag", when(col("rowNumDesc") === 1, 0).otherwise(col("flag")))
      // 认为前一天的持仓位成本，并且不考虑当天的买卖
      .withColumn("floatVal", when(col("rowNum") === 1 && col("date") === DataProcessor.preDate, lit(-1) * col("mktVal")).otherwise(col("tradeVal")))
      .withColumn("floatVal", when(col("rowNumDesc") === 1, col("mktVal") + col("floatVal")).otherwise(col("floatVal")))
      .cache()

    flagDF = totalDF.select("userID", "stockID", "date", "flag", "mktPosition")
      .filter((col("flag") === 1) || (col("flag") === 0))
      .join(mktBasicIdxDF, Seq("date"))
      .withColumn("calOrderLag", lag(col("calOrder"), 1).over(holdW))
      .withColumn("calOrderLead", lead(col("calOrder"), 1).over(holdW))
      .withColumn("holdDeltaLag", abs(col("calOrder") - col("calOrderLag")))
      .withColumn("holdDeltaLead", abs(col("calOrder") - col("calOrderLead")))

      // 这里是标记头天买第二天卖的以及头天卖第二天买的?
      .withColumn("holdDelta", when(((col("holdDeltaLag") === 1) && (col("flag") === 0)) ||
        ((col("holdDeltaLead") === 1) && (col("flag") === 1)), lit(1)))
      .withColumn("rowNum", row_number().over(holdW))
      // 同一次持有的买和卖得pivot相同
      .withColumn("pivot", col("flag") + col("rowNum"))
      .select("userID", "stockID", "date", "pivot", "holdDelta", "holdDeltaLag", "holdDeltaLead")


    stageDF = totalDF.join(flagDF, Seq("userID", "stockID", "date"), "left").na.fill(0)
      .withColumn("filled", sum("pivot").over(holdW))
      .withColumn("filledLag", lag(col("filled"), 1).over(holdW))
      .withColumn("filledLag", when(col("filledLag").isNull, 0).otherwise(col("filledLag")))
      .withColumn("filledLead", lead(col("filled"), 1).over(holdW))
      .withColumn("newPivot", when(col("filled") === col("filledLead"), col("filledLead")).otherwise(col("filledLag")))
      // 如果是持有一天的清仓记录
      .withColumn("newPivot", when(col("holdDeltaLag") === 1 && col("holdDelta") === 1, col("newPivot") - col("pivot")).otherwise(col("newPivot")))
      .withColumn("pivot",col("newPivot"))
      .drop("filled").drop("filledLag").drop("filledLead").drop("newPivot")

    staticDF = totalDF
      .groupBy("userID", "stockID").agg(sum("floatVal").alias("totalFloatVal"))
      .cache()


    DataProcessor.releaseCache(totalDF)


    /*-------根据交易计算用户止盈止损能力-----------*/
    if (!FileOperator.checkExist("userPL/_SUCCESS")) {

        val userPL = staticDF.withColumn("winOrloss", when(col("totalFloatVal") > 0, 1).otherwise(0))
          .withColumn("win",col("totalFloatVal")*col("winOrloss"))
          .groupBy("userID").agg(sum("win").alias("win"),sum(abs(col("totalFloatVal"))).alias("total")).na.fill(0)
          .withColumn("plScore", round(col("win") / col("total"), 4)).na.fill(0)
          .withColumn("win", round(col("win"), 2))
          .withColumn("loss", round(col("total") - col("win"), 2))
          .withColumn("win", when(col("win") <= -1e12, lit(-1e12 + 1)).otherwise(col("win")))
          .withColumn("win", when(col("win") >= 1e12, lit(1e12 - 1)).otherwise(col("win")))
          .withColumn("loss", when(col("loss") <= -1e12, lit(-1e12 + 1)).otherwise(col("loss")))
          .withColumn("loss", when(col("loss") >= 1e12, lit(1e12 - 1)).otherwise(col("loss")))
          .select("userID", "plScore", "win", "loss")

        DataProcessor.dumpResult("userPL", userPL)

    }
    /*-------计算用户盈亏行业个股-----------*/
    if (!FileOperator.checkExist("profitableStockIndustry/_SUCCESS")) {

        var profitableStockDF = staticDF.withColumn("initRank", rank.over(wStockTradeValDesc))
          .withColumn("invertRank", rank.over(wStockTradeVal))
          .select("userID", "stockID", "initRank", "invertRank", "totalFloatVal")
          .withColumn("totalFloatVal", round(col("totalFloatVal"),2))
          .withColumn("win",when(col("totalFloatVal")>0,1).otherwise(0))

      if(!FileOperator.checkExist("df/profitablestockdf/_SUCCESS")) {
        DataProcessor.writeToDB(profitableStockDF, "profitablestockdf", true)
      }
        profitableStockDF = profitableStockDF.groupBy("userID")
          .agg(convert2ReverseString(collect_list(when(col("initRank") <= DataProcessor.keepNum && col("totalFloatVal") > 0, col("stockID")))).alias("goodStocks"),
            convert2String(collect_list(when(col("invertRank") <= DataProcessor.keepNum && col("totalFloatVal") < 0, col("stockID")))).alias("badStocks"),
            count("*").alias("tradedNum"),sum("win").alias("winNum"))
          .withColumn("winRatio",when(col("tradedNum")===0, 0.0).otherwise(col("winNum")/col("tradedNum")))

        val profitableIndustryDF = staticDF
          .join(DataProcessor.industryDF, Seq("stockID"), "leftouter")
          .groupBy("userID", "industryName1ST")
          .agg(sum("totalFloatVal").alias("totalFloatVal"))
          .withColumn("initRank", rank.over(wStockTradeValDesc))
          .withColumn("invertRank", rank.over(wStockTradeVal))
          .select("userID", "industryName1ST", "initRank", "invertRank", "totalFloatVal")
          .groupBy("userID")
          .agg(convert2ReverseString(collect_list(when(col("initRank") <= DataProcessor.keepNum && col("totalFloatVal") > 0, col("industryName1ST")))).alias("goodIndustry"),
            convert2String(collect_list(when(col("invertRank") <= DataProcessor.keepNum && col("totalFloatVal") < 0, col("industryName1ST")))).alias("badIndustry"))

        val profitableStockIndustryDF = profitableStockDF
          .join(profitableIndustryDF, Seq("userID"), "leftouter")
          .select("userID", "goodStocks", "badStocks", "goodIndustry", "badIndustry", "tradedNum", "winRatio")
        DataProcessor.dumpResult("profitableStockIndustry", profitableStockIndustryDF)

    }

    DataProcessor.releaseCache(staticDF)


      benchmarkDF = stageDF.join(DataProcessor.industryDF, "stockID")
        .join(mktBasicIdxDF.select("date", "closeIndex"), Seq("date")).drop("rate")
        .join(DataProcessor.mktIndIdxdDF.withColumnRenamed("closeIndex", "closeIndexInd"), Seq("date", "industryName1ST")).drop("rate")
        .join(DataProcessor.mktEqudDF.select("date","stockID","closePrice"), Seq("date", "stockID"))
        .repartition(col("userID"))
        .sortWithinPartitions("userID","stockID","date")
        .withColumn("rowNum", row_number().over(holdW))
        .withColumn("fakeHoldNum", when(col("num") < 0, col("holdNum") - col("num")).otherwise(col("holdNum")))
        .withColumn("costVal", when((col("rowNum") === 1) && col("date") === DataProcessor.firstDay, col("mktVal")).otherwise(lit(-1)*col("tradeVal")))
        .drop("mktVal").drop("rowNum")
        .withColumn("costVal", sum("costVal").over(holdSplitW))
        .withColumn("costPrice", round(col("costVal") / col("holdNum"), 2))
        .drop("costVal")
        .withColumn("costPriceLag", lag(col("costPrice"), 1).over(holdSplitW))
        .withColumn("rowNumDesc", row_number().over(holdWDesc))
        .withColumn("costPrice", when((col("rowNumDesc") === 1) || (col("costPrice").isNull), col("costPriceLag")).otherwise(col("costPrice")))
        .drop("costPriceLag").drop("rowNumDesc")
        .withColumn("closePriceBench", when(col("holdNum") === 0, col("price")).otherwise(col("closePrice")))
        .sortWithinPartitions("userID","stockID","date")
        .select("userID", "stockID", "date", "pivot", "costPrice", "closePrice", "closePriceBench", "fakeHoldNum", "floatVal", "closeIndex", "closeIndexInd", "mktPosition")
        .cache()


    /*-------计算用户操盘能力-----------*/
    if (!FileOperator.checkExist("userOperate/_SUCCESS")) {

        val operateDF = benchmarkDF
          .groupBy("userID", "stockID")
          .agg(first("closePrice").alias("closePrice1"), last("closePrice").alias("closePrice2"),
            count("*").alias("cnts"), sum("floatVal").alias("profit"), mean("fakeHoldNum").alias("meanHoldNum"),
            stddev("fakeHoldNum").alias("holdChange"))
          .filter((col("cnts") > 2) && (col("holdChange") > 0))
          .drop("cnts").drop("holdChange")
          .withColumn("fakeVal", (col("closePrice2") - col("closePrice1")) * col("meanHoldNum"))
          .drop("closePrice2").drop("closePrice1").drop("meanHoldNum")
          .withColumn("chgRate", round(($"profit" - $"fakeVal") / (abs($"fakeVal") + abs($"profit")), 2))
          .groupBy("userID").agg(round(avg($"chgRate"), 2).alias("operateScore"))
          .withColumn("operateScore", when(col("operateScore") < -100, lit(-100)).otherwise(col("operateScore")))
          .withColumn("operateScore", when(col("operateScore") > 100, lit(100)).otherwise(col("operateScore")))
          .select("userID","operateScore")
        DataProcessor.dumpResult("userOperate", operateDF)

    }
    /*-------计算用户选股能力-----------*/
    if (!FileOperator.checkExist("userSelect/_SUCCESS")) {

        //这里需要复权的数据
        var selectDF = benchmarkDF
          .drop("closePrice")
          .join(DataProcessor.mktEqudDFAdj
            .select("date","stockID","closePrice"), Seq("date", "stockID"))
          .repartition(col("userID"))
          .sortWithinPartitions("userID","stockID","pivot","date")
          .groupBy("userID","stockID","pivot")
          .agg(first("closeIndexInd").alias("closeIndexInd1"), last("closeIndexInd").alias("closeIndexInd2"),
            first("closeIndex").alias("closeIndex1"), last("closeIndex").alias("closeIndex2"),
            first("closePrice").alias("closePrice1"), last("closePrice").alias("closePrice2"),
            count("*").alias("cnts"))
          .filter(col("cnts") > 2)
          .withColumn("stockRate", col("closePrice2") / col("closePrice1"))
          .withColumn("mktRate", col("closeIndex2") / col("closeIndex1"))
          .withColumn("industryRate", col("closeIndexInd2") / col("closeIndexInd1"))
          .groupBy("userID","stockID")
          .agg(collect_list("stockRate").alias("stockRate"),
            collect_list("mktRate").alias("mktRate"),
            collect_list("industryRate").alias("industryRate"))
          .withColumn("stockRate", calTotalRate(col("stockRate")) - 1)
          .withColumn("mktRate", calTotalRate(col("mktRate")) - 1)
          .withColumn("industryRate", calTotalRate(col("industryRate")) - 1)
          .withColumn("industryExtraProfit", col("industryRate") - col("mktRate"))
          .withColumn("stockExtraProfit", col("stockRate") - col("industryRate"))

      if (!FileOperator.checkExist("df/userselectdf/_SUCCESS")) {
        DataProcessor.writeToDB(selectDF
          .withColumn("stockRate", round(col("stockRate"), 4))
          .withColumn("industryRate", round(col("industryRate"), 4))
          .select("userID", "stockID", "stockRate", "industryRate"), "userselectdf", true)
      }

        selectDF = selectDF
        .groupBy("userID")
          .agg(round(avg("industryExtraProfit"), 4).alias("industryScore"), round(avg("stockExtraProfit"), 4).alias("stockScore")).na.fill(0)
        DataProcessor.dumpResult("userSelect", selectDF)

    }
    /*-------计算用户止盈止损能力-----------*/
    if (!FileOperator.checkExist("userStopWinLoss/_SUCCESS")) {

        val pivotW = Window.partitionBy("userID", "stockID", "pivot")
        val selectDF = benchmarkDF
          .withColumn("mktPositionLag", lag(col("mktPosition"), 1).over(pivotW))
          .withColumn("mktPosition", when(col("mktPosition") === 0, col("mktPositionLag")).otherwise(col("mktPosition")))
          .filter(col("costPrice") > 0)
          .withColumn("ratio", col("closePriceBench") / col("costPrice") - 1)
          .withColumn("lastRatio", last("ratio").over(pivotW))
          .withColumn("maxRatio", max("ratio").over(pivotW))
          .withColumn("minRatio", min("ratio").over(pivotW))
          .withColumn("stopWin", when((col("ratio") > 0) && (col("lastRatio") > 0) && (col("maxRatio") > 0), col("lastRatio") / col("maxRatio")))
          .withColumn("stopLoss", when(col("ratio") < 0, col("ratio") * col("mktPosition")))
          .groupBy("userID", "stockID", "pivot")
          .agg(mean("mktPosition").alias("meanMktPosition"),
            last("stopWin").alias("stopWin"), min("stopLoss").alias("stopLoss")).na.fill(0)
          .withColumn("stopWin", col("stopWin") * col("meanMktPosition"))
          .withColumn("weight", col("meanMktPosition") / sum("meanMktPosition").over(Window.partitionBy("userID")))
          .withColumn("stopWin", col("stopWin") * col("weight"))
          .withColumn("stopLoss", col("stopLoss") * col("weight"))
          .groupBy("userID").agg(round(sum("stopWin"),4).alias("stopWin"), round(sum("stopLoss"),4).alias("stopLoss"))
        DataProcessor.dumpResult("userStopWinLoss", selectDF)

    }

    DataProcessor.releaseCache(benchmarkDF)

    /*-------计算用户日内择时能力-----------*/
    if (!FileOperator.checkExist("userDailyTiming/_SUCCESS")) {

        val dailyTiming = DataProcessor.stockTradeDF
          .select("date","stockID","userID","tradeWay","price")
          .join(DataProcessor.mktEqudDF.select("date","stockID","vwap"),Seq("date","stockID"))
          .withColumn("timing",col("price")/col("vwap")-1)
          .withColumn("buyTiming",when(col("tradeWay")==="1",col("timing")).otherwise(0))
          .withColumn("buyNum",when(col("tradeWay")==="1",1).otherwise(0))
          .withColumn("sellTiming",when(col("tradeWay")!=="1",col("timing")).otherwise(0))
          .withColumn("sellNum",when(col("tradeWay")!=="1",1).otherwise(0))
          .withColumn("win", when(((col("tradeWay")==="1") && col("timing") < 0) || ((col("tradeWay")!=="1") && col("timing") > 0),1).otherwise(0))
          .groupBy("userID")
          .agg(sum("buyTiming").alias("buyTiming"),sum("sellTiming").alias("sellTiming"),
            sum("buyNum").alias("buyNum"),sum("sellNum").alias("sellNum"),sum("win").alias("win"))

          .withColumn("buyTiming", when(col("buyNum") > 0, -col("buyTiming") / col("buyNum")).otherwise(0))
          .withColumn("sellTiming", when(col("sellNum") > 0, col("sellTiming") / col("sellNum")).otherwise(0))
          .withColumn("totalTiming",round((col("buyTiming")+col("sellTiming"))/2,4))
          .withColumn("buyTiming", round(col("buyTiming"),4))
          .withColumn("sellTiming", round(col("sellTiming"),4))
          .withColumn("win",round(col("win") / (col("buyNum") + col("sellNum")),4))
          .select("userID","buyTiming","sellTiming","totalTiming","win")


        DataProcessor.dumpResult("userDailyTiming", dailyTiming)

    }

    /*-------计算用户择时能力-----------*/
    if (!FileOperator.checkExist("usertiming/_SUCCESS")) {
      val mktdf2 = mktBasicIdxDF.withColumn("next2Index", lead(col("closeIndex"), 2).over(Window.orderBy(col("date"))))
        .withColumn("next5Index", lead(col("closeIndex"), 5).over(Window.orderBy(col("date"))))
        .withColumn("next2Rate", DataProcessor.calcRate(col("closeIndex"), col("next2Index")))
        .withColumn("next5Rate", DataProcessor.calcRate(col("closeIndex"), col("next5Index"))).drop("next2Index").drop("next5Index").drop("rate").drop("closeIndex")
      val wSpec = Window.partitionBy("userID", "date") //用户在日期上操作导致资金变动情况
      val mdf1 = DataProcessor.ashareTradeDF
          .filter(col("date")>=DataProcessor.startDate && col("date")<=DataProcessor.endDate)
          .select("date", "userID", "tradeVal").withColumn("dayChg", sum(col("tradeVal")) over wSpec).select("date", "userID", "dayChg").dropDuplicates().sort("userID")
      val mdf2 = mdf1.join(mktdf2, Seq("date"), "left_outer")
        .withColumn("meanProfit", (col("next5Rate") * col("dayChg") + col("next2Rate") * col("dayChg")) / 2)
        .withColumn("ABSDAYCHG", abs(col("dayChg")))
        .groupBy("userID").agg(round(avg("meanProfit"),4).alias("timingProfit"), round(avg("ABSDAYCHG"),4).alias("meanDayChg"))
        .withColumn("timingScore", round(col("timingProfit") / col("meanDayChg"),4))
        .select("userID","timingScore")
      DataProcessor.dumpResult("usertiming", mdf2)
    }

  }
}
