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

import com.datayes.bdb.analysis.item.{LongTimeAnalyzer, BasicInfoAnalyzer, OpenQualificationAnalyzer, AccountAnalyzer}
import com.datayes.bdb.util.{LongTimeDataProcessor, DataProcessor, FileOperator}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
/**
  * Created by liang.zhao on 2016/12/29.
  */
object Schedule {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("label-calc")
    val sc = new SparkContext(conf)

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive","true")
    val session = new HiveContext(sc)
    FileOperator.setConf(sc)
    DataProcessor.setConf(sc.getConf)
    DataProcessor.setSession(session)

    LongTimeDataProcessor.setConf(sc.getConf,session)


    val flag = sc.getConf.get("spark.schedule.batch")

    val module = sc.getConf.get("spark.schedule.module")

    val strategy = new StrategyAnalyzer(session)
    val user = new UserAnalyzer(session)
    val ability = new AbilityAnalyzer(session)
    val behavior = new TradeBehavior(session,sc)
    val longTerm = new LongTermAnalyzer(session)
    val currentHold = new HoldAnalyzer(session)
    val entrust = new EntrustAnalyzer(session)
    val asset = new AssetAnalyzer(session, sc)
    val fund = new FundAnalyzer(session,sc)
    val finance = new FinanceAnalyzer(session)
    val accountAnalyzer = new AccountAnalyzer(session)
    val openQualificationAnalyzer = new OpenQualificationAnalyzer(session)
    val basicInfoAnalyzer = new BasicInfoAnalyzer(session)
    val longTimeAnalyzer = new LongTimeAnalyzer(session)

    val startDate = sc.getConf.get("spark.schedule.start")
    val header = sc.getConf.get("spark.schedule.header")
    var endDate = DateTime.now.toString("yyyyMMdd")


    if (!sc.getConf.contains("spark.schedule.keepNum")) {
      sc.getConf.set("spark.schedule.keepNum","5")
    }
    if (sc.getConf.contains("spark.schedule.end")) {
      endDate = sc.getConf.get("spark.schedule.end")
    }
    if (flag == "1") {
      DataProcessor.loadData(session, 0,startDate,endDate, header, true)
      if (module == "1") {
        behavior.analyse()
      } else if (module == "2") {
        accountAnalyzer.analyse()
      }else if(module == "3") {
        user.analyse()
      }else if(module == "4") {
        ability.analyse()
      }else if(module == "5") {
        strategy.analyse()
      }else if(module == "6"){
        sc.getConf.set("spark.schedule.batch","4")
        DataProcessor.loadData(session, 1,startDate,endDate,header,false)
        longTerm.analyse()
      } else if (module == "7") {
        asset.analyse()
      } else if (module == "8") {
        currentHold.analyse()
      } else if (module == "9") {
        fund.analyse()
      } else if (module == "10") {
        openQualificationAnalyzer.analyse()
      } else if (module == "11") {
        basicInfoAnalyzer.analyse()
      } else if (module == "12") {
        entrust.analyse()
      } else if (module == "13") {
        finance.analyse()
      } else if(module == "0"){

        val f = List(
          Future{
            sc.setLocalProperty("spark.scheduler.pool", "ability")
            ability.analyse()
          },
          Future{
            sc.setLocalProperty("spark.scheduler.pool", "user")
            user.analyse()
            sc.setLocalProperty("spark.scheduler.pool", "behavior")
            behavior.analyse()

            entrust.analyse()
            sc.setLocalProperty("spark.scheduler.pool", "strategy")
            strategy.analyse()

            basicInfoAnalyzer.analyse()
          }
        )
        f.map{ a => {
          Await.result(a, 1 days)
        }}

        val f3 = List(
          Future{
            sc.setLocalProperty("spark.scheduler.pool", "longterm")
            longTerm.analyse()
            sc.setLocalProperty("spark.scheduler.pool", "currentHold")
            currentHold.analyse()
            sc.setLocalProperty("spark.scheduler.pool", "accountAnalyzer")
            accountAnalyzer.analyse()},
          Future{
            sc.setLocalProperty("spark.scheduler.pool", "fund")
            fund.analyse()
            sc.setLocalProperty("spark.scheduler.pool", "finance")
            finance.analyse()}
        )
        f3.map{ a => {
          Await.result(a, 1 days)
        }}

        DataProcessor.releaseCache(DataProcessor.mergedDF)
        DataProcessor.releaseCache(DataProcessor.ashareTradeDF)


        asset.analyse()
        openQualificationAnalyzer.analyse()


        LongTimeDataProcessor.loadData(session, 0,"20100101",endDate)
        longTimeAnalyzer.analyse()
      }
    }

    sc.stop()
  }
}