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

import com.data.spark.sparksql.bdb.bean.ApplicationRow
import com.data.spark.sparksql.bdb.util.{ProfitUtil, DataProcessor, FileOperator}
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, round, sum, _}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.expressions.Window

/**
  * Created by liang.zhao on 12/21/16.
  */
class TradeBehavior(session: SQLContext,sc: SparkContext) {
  val logger = Logger.getLogger(getClass.getName)
  import session.implicits._

  val timeFmt = "yyyyMMdd"

  def analyse(): Unit = {
    val castItem = sc.broadcast(DataProcessor.tradeDateList)
    val tradeDF:DataFrame = DataProcessor.tradeDF.filter(col("tradeVal") !== 0)
      .filter(col("date")>=DataProcessor.startDate && col("date")<=DataProcessor.endDate)
    val holdDF = DataProcessor.stockHoldDF.filter($"holdNum">=100)
      .filter(col("date")>=DataProcessor.startDate && col("date")<=DataProcessor.endDate)
    val assetDF = DataProcessor.userAsset.select("userID","date","totalAsset")
      .filter(col("date")>=DataProcessor.startDate && col("date")<=DataProcessor.endDate)


    val positionDF = holdDF
      .withColumn("totalAsset", sum("mktVal").over(Window.partitionBy(col("userID"),col("date"))))
      .withColumn("position", col("mktVal") / col("totalAsset"))
      .groupBy("userID","stockID","date").agg(sum("position").alias("position"),sum("holdNum").alias("holdNum"),
      sum("mktVal").alias("mktVal"))
      .cache()


    if (!FileOperator.checkExist("industryScatter/_SUCCESS")) {
      val industryScatter = positionDF.join(DataProcessor.industryDF, Seq("stockID"), "left")
        .groupBy("userID", "date", "industryName1ST").agg(sum("position").alias("dailyIndPosition"))
        .withColumn("entropy", col("dailyIndPosition") * log(2, col("dailyIndPosition")) * lit(-1))
        .groupBy("userID", "date").agg(sum("entropy").alias("dailyEntropy"))
        .groupBy("userID").agg(round(mean("dailyEntropy"), 4).alias("industryScatter")).na.fill(0)
        .select("userID","industryScatter")
        .withColumn("industryScatter",when(col("industryScatter") >= 9e7, 9e7).otherwise(col("industryScatter")))
        .withColumn("industryScatter",when(col("industryScatter") <= -9e7, -9e7).otherwise(col("industryScatter")))
      DataProcessor.dumpResult("industryScatter", industryScatter)

    }
    if (!FileOperator.checkExist("stockScatter/_SUCCESS")) {
      val stockScatter = positionDF.withColumn("entropy", col("position") * log(2, col("position")) * lit(-1))
        .groupBy("userID", "date").agg(sum("entropy").alias("dailyEntropy"))
        .groupBy("userID").agg(round(mean("dailyEntropy"), 4).alias("stockScatter")).na.fill(0)
        .select("userID", "stockScatter")
        .withColumn("stockScatter",when(col("stockScatter") >= 9e7, 9e7).otherwise(col("stockScatter")))
        .withColumn("stockScatter",when(col("stockScatter") <= -9e7, -9e7).otherwise(col("stockScatter")))
      DataProcessor.dumpResult("stockScatter", stockScatter)

    }


    //trade avg value
    if(!FileOperator.checkExist("tradeAvgVal/_SUCCESS")) {
      val tradeAvgVal = tradeDF.groupBy("userID", "date").agg(sum(abs(col("tradeVal"))).alias("dailySum"))
        .groupBy("userID").agg(round(mean("dailySum"), 4).alias("meanTradeVal"))
        .select("userID", "meanTradeVal")
      DataProcessor.dumpResult("tradeAvgVal", tradeAvgVal)
    }

    if(!FileOperator.checkExist("holdDuration/_SUCCESS")) {

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

      val stageDF = positionDF
        .select("userID", "stockID", "date")
        .repartition(col("userID"), col("stockID"))
        .sortWithinPartitions("userID", "stockID", "date")
        .groupBy("userID", "stockID")
        .agg(collect_list("date").alias("dateList"))
        .withColumn("result", calcHoldDuration(col("dateList")))
        .withColumn("singleDurationMax", col("result").getItem("max"))
        .withColumn("singleDurationMean", col("result").getItem("mean"))

        .groupBy("userID").agg(round(mean("singleDurationMean"), 1).alias("holdDurationMean"), round(max("singleDurationMax"),1).alias("holdDurationMax"))

      DataProcessor.dumpResult("holdDuration", stageDF)

    }

    DataProcessor.releaseCache(positionDF)

    //    turnover rate
    if(!FileOperator.checkExist("turnOver/_SUCCESS")) {
      val tradeSum = tradeDF
        .filter(col("category") === "stock")
        .groupBy("userID","date")
        .agg(sum(abs(col("tradeVal"))).alias("tradeValueSum"), count("*").alias("num"))
        .select("userID","date","tradeValueSum","num")
        .groupBy("userID")
        .agg(sum(col("tradeValueSum")).alias("tradeValueSum"),
          max(col("num")).alias("max"),sum(col("num")).alias("sum"))
        .select("userID","max","sum","tradeValueSum")

      val meanAsset = assetDF.groupBy("userID").agg(mean(col("totalAsset")).alias("meanAsset"),count("*").alias("daysNum"))

      val turnOver = meanAsset.join(tradeSum, Seq("userID"))
        .withColumn("turnover", round(col("tradeValueSum") / col("meanAsset")/col("daysNum"), 4)*250)
        .withColumn("num", round(col("sum") / col("daysNum"),2))
        .select("userID","max","turnover","num")
      DataProcessor.dumpResult("turnOver", turnOver)

    }


    //trade behavior split order || t+0
    if(!FileOperator.checkExist("splitOrder/_SUCCESS")) {
      val thresholdMap = udf((x: Double) => {
        if (x > 0.1) 1 else 0
      })
      val splitOrder = tradeDF
        .groupBy("userID", "date", "stockID").pivot("tradeWay").count().na.fill(0)
        .withColumn("total", col("2") + col("1"))
        .withColumn("totalFilter", when(col("2") === 1, col("total") - 1).otherwise(col("total")))
        .withColumn("totalFilter", when(col("1") === 1, col("totalFilter") - 1).otherwise(col("totalFilter")))
        .withColumn("totalTrade", sum("total").over(Window.partitionBy("userID")))
        .groupBy("userID")
        .agg(sum("totalFilter").alias("splitTotal"), mean("totalTrade").alias("totalTrade"))
        .withColumn("splitRatio", round(col("splitTotal") / col("totalTrade"), 4))
        .withColumn("splitBehavior", thresholdMap(col("splitRatio")))
        .select("userID", "splitBehavior", "splitRatio")
      DataProcessor.dumpResult("splitOrder", splitOrder)

    }

    if(!FileOperator.checkExist("inBatches/_SUCCESS")) {
      val inBatches = DataProcessor.stockTradeDF
        .filter(col("date")>=DataProcessor.startDate && col("date")<=DataProcessor.endDate)
        .select("userID","stockID","date","num","tradeWay")
        .groupBy("userID","stockID","date","tradeWay").agg(sum("num").alias("num"))
        .join(holdDF.select("userID","stockID","date","holdNum"),Seq("userID","stockID","date"),"left").na.fill(0.0)
        .withColumn("batchFlag",when(col("holdNum")===0 || col("num")===col("holdNum"),0).otherwise(1))
        .withColumn("tradeWay", when(col("tradeWay") === "1", "b").otherwise("s"))
        .groupBy("userID").pivot("tradeWay").agg(sum("batchFlag")/count("*"))
        .select("userID","b","s")
        .withColumn("b",round(col("b"),4))
        .withColumn("s",round(col("s"),4))
        .withColumn("bAll",lit(1) - col("b"))
        .withColumn("sAll",lit(1) - col("s"))

      DataProcessor.dumpResult("inBatches", inBatches)
    }





    // 打新中了的股票和数量
    val hitDF = DataProcessor.tradeDF.filter(col("business_flag") === 4016)
      .filter(col("date")>=DataProcessor.startDate && col("date")<=DataProcessor.endDate)
      .filter(col("category") === "stock")
      .withColumn("tradeVal",col("num") * col("price"))
      .select("userID","stockID","num","tradeVal").distinct()
      .withColumnRenamed("num","totalNum")
      .withColumnRenamed("tradeVal","costVal")

    val applicationProfit = tradeDF.filter(col("tradeWay") === "2")
      .join(hitDF,Seq("userID","stockID"),"right")
      .filter(col("date").isNotNull)
      .na.fill(0d)
      .select("userID","stockID","totalNum","num","price","date","costVal")
      .withColumn("totalNum",col("totalNum") * 1d)
      .withColumn("num",abs(col("num")) * 1d)
      .withColumn("price",col("price") * 1d)
      .withColumn("costVal",col("costVal") * 1d)
      .rdd.map(row => new ApplicationRow(row.getString(0), row.getString(1), row.getDouble(2), row.getDouble(3), row.getDouble(4), row.getString(5), row.getDouble(6)))
      .groupBy(row => row.getUserID + "_" + row.getStockID)
      .map(x => ProfitUtil.calApplicationProfit(x._2))
      .toDF()
      .select("userID","stockID","gain","left")
      .join(DataProcessor.assetDF
        .filter(col("date") === DataProcessor.lastDay)
        .select("stockID","closing_price","asset_price"), Seq("stockID"), "inner")
      .withColumn("closing_price", when(col("closing_price") === 0, col("asset_price")).otherwise(col("closing_price")))
      .withColumn("gain", col("gain") + col("left") * col("closing_price"))
      .withColumn("gain",round(col("gain"),2))
      .select("userID","stockID","gain")

    if (!FileOperator.checkExist("df/applicationprofit/_SUCCESS")) {
      DataProcessor.writeToDB(applicationProfit, "applicationprofit", true)
    }



  }
}
