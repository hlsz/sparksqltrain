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

import com.data.spark.sparksql.bdb.util.{FileOperator, LongTimeDataProcessor, DataProcessor}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.joda.time.{DateTime, Months}
import org.joda.time.format.DateTimeFormat

/**
  * Created by hongnan.li on 2018/10/31.
  */
class LongTimeAnalyzer(session: SQLContext) {

  val stockAgeFunc = udf { dateStr: String =>
    try {
      (Months.monthsBetween(DateTimeFormat.forPattern("yyyyMMdd").parseDateTime(dateStr), DateTime.now).getMonths() / 12.0).formatted("%.2f")
    } catch {
      case e: Exception => (Months.monthsBetween(DateTimeFormat.forPattern("yyyyMMdd").parseDateTime("29990101"), DateTime.now).getMonths() / 12.0).formatted("%.2f")
    }
  }

  def analyse(): Unit = {


    if (!FileOperator.checkExist("fundAge/_SUCCESS")) {
      val fundAge =  LongTimeDataProcessor.otcHoldDF
        .select("userID","date","stock_type")
        .withColumn("category",DataProcessor.changeCategoryFunc(col("stock_type")))
        .filter(col("category") === "fund")
        .groupBy("userID")
        .agg(min("date").alias("date"))
        .withColumn("fundAge", stockAgeFunc(col("date")))
        .select("userID","fundAge")

      DataProcessor.dumpResult("fundAge", fundAge)
    }


  }
}
