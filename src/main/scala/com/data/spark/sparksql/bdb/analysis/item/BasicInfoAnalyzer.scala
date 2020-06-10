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

import com.data.spark.sparksql.bdb.util.{DataProcessor, FileOperator}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}

import scala.util.control.Breaks

/**
  * Created by hongnan.li on 2018/10/30.
  */
class BasicInfoAnalyzer(session: SQLContext) {


  val getProvinceFromAddress = udf { (address: String) => {
    if (address == null || address.isEmpty) {
      null
    } else {
      var province :String = null
      var f:String = address
      if (address.length >= 6) {
        f = address.substring(0,6)
      }

      val provinceList: List[String] = List("北京", "上海", "天津", "重庆", "黑龙江", "吉林", "辽宁", "内蒙古", "河北", "新疆", "甘肃", "青海", "陕西", "宁夏", "河南", "山东", "山西", "安徽", "湖北", "湖南", "江苏", "四川",
        "贵州", "云南", "广西", "西藏", "浙江", "江西", "广东", "福建", "台湾", "海南", "香港", "澳门")
      val loop = new Breaks
      loop.breakable(
        for (item: String <- provinceList) {
          if (f.contains(item)) {
            province = item
            loop.break()
          } else if (f.contains(item)) {
            province = item
            loop.break()
          }
        })
      province
    }
    }
  }

  val getDaysBetweenFunc = udf { dateStr: String =>
    try {
      Days.daysBetween(DateTimeFormat.forPattern("yyyyMMdd").parseDateTime(dateStr), DateTime.now).getDays()
    } catch {
      case e: Exception => Days.daysBetween(DateTimeFormat.forPattern("yyyyMMdd").parseDateTime("29990101"), DateTime.now).getDays()
    }
  }


  val getSex = udf { sex: String =>
    if (sex != null) {
      sex match {
        case "0" => "男"
        case "1" => "女"
        case _ => null
      }
    } else {
      null
    }
  }

  def analyse(): Unit = {

    val basic = DataProcessor.readData("accountInfo")
      .select("userID","ADDRESS","branch_no")
      .withColumn("province", getProvinceFromAddress(col("ADDRESS")))
//      .filter(col("province").isNotNull)
        .withColumnRenamed("branch_no","branchNo")
      .select("userID","province","branchNo")


    val account = DataProcessor.readData("accountState").filter(col("openDate")<=DataProcessor.endDate)
        // 干掉已经销户的
        .filter(col("CLIENT_STATUS") !== "3")
        .select("userID","openDate","client_gender")
      .repartition(col("userID"))
      .withColumn("sex", getSex(col("client_gender")))
      .withColumn("openDays", getDaysBetweenFunc(col("openDate")))
      .select("userID","openDays","sex")

    if (!FileOperator.checkExist("basic/_SUCCESS")) {
      DataProcessor.dumpResult("basic", basic.join(account,Seq("userID"),"outer"))
    }


  }

}
