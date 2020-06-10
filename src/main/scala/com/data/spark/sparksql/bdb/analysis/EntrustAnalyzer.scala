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
import org.apache.spark.sql.functions.{col, round, sum, _}
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.expressions.Window


class EntrustAnalyzer(session: SQLContext) {
  val logger = Logger.getLogger(getClass.getName)
  import session.implicits._


  def analyse(): Unit = {

    val tradeDF :DataFrame = DataProcessor.readData("entrust")
      .filter(col("init_date")>=DataProcessor.startDate && col("init_date")<=DataProcessor.endDate)
      .withColumnRenamed("init_date","date")
      .withColumnRenamed("client_id","userID")
      .withColumnRenamed("CURR_TIME","tradeTime")
      .filter(col("entrust_type") === "0")
      .select("userID","date","tradeTime")
      .cache()


    //    trade freq
    if(!FileOperator.checkExist("entrustFreq/_SUCCESS")) {
      val tradeFreq = tradeDF.groupBy("userID", "date")
        .agg(count("*").alias("dailyTradeFreq"))
        .groupBy("userID").agg(round(mean("dailyTradeFreq"), 2).alias("meanEntrustFreq"), round(max("dailyTradeFreq"), 2).alias("maxEntrustFreq"))
      DataProcessor.dumpResult("entrustFreq", tradeFreq)
    }

    //    trade timing
    if(!FileOperator.checkExist("entrustTime/_SUCCESS")) {
      def mapStageUDF = udf((t: Int) => {
        t match {
          case t if t >= 91500000 && t < 92500000 => "stage0"
          case t if t >= 93000000 && t < 94500000 => "stage1"
          case t if t >= 124500000 && t < 130000000 => "stage2"
          case t if t >= 142500000 && t < 143500000 => "stage3"
          case t if t >= 144500000 && t < 150000000 => "stage4"
          case _ => "stage5"
        }
      })

      val wStageCount = Window.partitionBy("userID")
      val tradeTime = tradeDF
        .withColumn("stage", mapStageUDF($"tradeTime"))
        .groupBy("userID", "stage")
        .agg(count("*").alias("count"))
        .withColumn("totalCount", sum("count").over(wStageCount))
        .withColumn("rn", round(col("count") / col("totalCount"), 4))
        .groupBy("userID").pivot("stage")
        .agg(mean("rn")).na.fill(0)
      DataProcessor.dumpResult("entrustTime", tradeTime)
    }

    DataProcessor.releaseCache(tradeDF)

  }
}
