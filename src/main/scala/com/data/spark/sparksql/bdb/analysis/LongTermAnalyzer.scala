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
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions.{col, max, mean, round}
import org.apache.spark.sql.functions._
/**
  * Created by liang on 17-2-17.
  */
class LongTermAnalyzer(session: SQLContext) {
  val logger = Logger.getLogger(getClass.getName)
  import session.implicits._


  def analyse():Unit = {
    val holdDF = DataProcessor.stockHoldDF.filter(col("holdNum") >= 100)
      .filter(col("date")>=DataProcessor.startDate && col("date")<=DataProcessor.endDate)
      .groupBy("userID","stockID","date").agg(sum("holdNum").alias("holdNum"))

    val fundHoldDF = DataProcessor.otcFundHold.groupBy("userID","date","stockID").agg(sum("market_value").alias("holdNum"))

    if(!FileOperator.checkExist("holdNum/_SUCCESS")){

      val holdSummary = holdDF.groupBy("userID", "date").count().groupBy("userID")
        .agg(round(mean("count"), 1).alias("meanHold"), max("count").alias("maxHold"))
        .withColumn("changeHold", round($"maxHold" / $"meanHold", 4)).na.fill(0)
        .select("userID", "meanHold", "maxHold", "changeHold")
      DataProcessor.dumpResult("holdNum", holdSummary)

    }

    //
    if(!FileOperator.checkExist("fundHoldScale/_SUCCESS")){

      val holdSummary = fundHoldDF.groupBy("userID", "date").agg(sum("holdNum").alias("sum")).groupBy("userID")
        .agg(round(mean("sum"), 1).alias("meanHold"), max("sum").alias("maxHold"))
        .withColumn("changeHold", round($"maxHold" / $"meanHold", 4)).na.fill(0)
        .select($"userID", $"meanHold", $"maxHold", $"changeHold")
      DataProcessor.dumpResult("fundHoldScale", holdSummary)

    }
  }
}