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

package com.data.spark.sparksql.bdb.analysis.item

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.data.spark.sparksql.bdb.util.{DataProcessor, FileOperator}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

/**
  * Created by hongnan.li on 2018/9/15.
  */
class OpenQualificationAnalyzer(session: SQLContext) {


  def analyse(): Unit = {
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    val now = DataProcessor.endDate

    val client = DataProcessor.readData("accountState")
      .filter(col("openDate")<=DataProcessor.endDate)
      // 干掉已经销户的
      .filter(col("CLIENT_STATUS") !== "3")
      .select("userID","CORP_RISK_LEVEL","openDate").repartition(col("userID"))

    // 定投资格和理财资格和投股
    val df1 = client
      .withColumn("finQualify",when(col("CORP_RISK_LEVEL") >= "1", lit(1)).otherwise(lit(0)))
      .withColumn("fixedQualify",when(col("CORP_RISK_LEVEL") >= "1", lit(1)).otherwise(lit(0)))
      .withColumn("counselorQualify",when(col("CORP_RISK_LEVEL") >= "4", lit(1)).otherwise(lit(0)))
      .select("userID","finQualify","fixedQualify","counselorQualify")


    val twoYearBefore = LocalDate.parse(DataProcessor.endDate, formatter).minusYears(2).format(formatter)
    val halfYearBefore = LocalDate.parse(DataProcessor.endDate, formatter).minusMonths(6).format(formatter)
    // 创业板
    val df2 = client.filter(col("CORP_RISK_LEVEL") > "3")
      .filter(col("openDate") < twoYearBefore)
      .withColumn("CYBQualify",lit(1))


    val tradeDFDay10 = DataProcessor.assetDF.select("date").distinct().orderBy(col("date").desc)
      .limit(10)
    val tradeDFDay20 = DataProcessor.assetDF.select("date").distinct().orderBy(col("date").desc)
      .limit(20)
    // 新三板
    val xsbDF = DataProcessor.readResultDataFromHDFS("/df/assetdf")
      .select("userID","date","totalNetAsset")
      .join(tradeDFDay10,Seq("date"),"inner")
      .groupBy("userID")
      .agg(mean("totalNetAsset").alias("avgAsset"))
      .filter(col("avgAsset") > 5000000)
      .select("userID")
      .join(client.filter(col("openDate") < twoYearBefore), Seq("userID"), "inner")
      .select("userID")
      .withColumn("XSBQualify",lit(1))


    val margin = DataProcessor.readResultDataFromHDFS("/df/stockprofitdf")
      .select("userID","date","mktVal")
      .join(tradeDFDay20,Seq("date"),"inner")
      .groupBy("userID")
      .agg(mean("mktVal").alias("avgAsset"))
      .filter(col("avgAsset") > 500000)
      .select("userID")
      .join(client.filter(col("openDate") < halfYearBefore).filter(col("CORP_RISK_LEVEL") >= "3"), Seq("userID"), "inner")
      .select("userID")
      .withColumn("marginQualify",lit(1))


    val hk = DataProcessor.readResultDataFromHDFS("/df/assetdf")
      .select("userID","date","totalNetAsset")
      .join(tradeDFDay20,Seq("date"),"inner")
      .groupBy("userID")
      .agg(mean("totalNetAsset").alias("avgAsset"))
      .filter(col("avgAsset") > 500000)
      .select("userID")
      .withColumn("HGTQualify",lit(1))
      .withColumn("SGTQualify",lit(1))


    if (!FileOperator.checkExist("qualify/_SUCCESS")) {
      val qualify = df1.join(df2, Seq("userID"),"outer")
        .join(xsbDF, Seq("userID"),"outer")
        .join(margin, Seq("userID"),"outer")
        .join(hk, Seq("userID"),"outer")
        .select("userID","finQualify","fixedQualify","counselorQualify","XSBQualify","marginQualify","HGTQualify","SGTQualify","CYBQualify")
        .withColumn("finQualify",when(col("finQualify") === 1, 1).otherwise(0))
        .withColumn("fixedQualify",when(col("fixedQualify") === 1, 1).otherwise(0))
        .withColumn("counselorQualify",when(col("counselorQualify") === 1, 1).otherwise(0))
        .withColumn("XSBQualify",when(col("XSBQualify") === 1, 1).otherwise(0))
        .withColumn("marginQualify",when(col("marginQualify") === 1, 1).otherwise(0))
        .withColumn("HGTQualify",when(col("HGTQualify") === 1, 1).otherwise(0))
        .withColumn("SGTQualify",when(col("SGTQualify") === 1, 1).otherwise(0))
        .withColumn("CYBQualify",when(col("CYBQualify") === 1, 1).otherwise(0))

      DataProcessor.dumpResult("qualify", qualify)
    }





  }
}
