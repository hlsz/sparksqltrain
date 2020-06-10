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

package com.data.spark.sparksql.bdb.bean

case class ProfitBeanNew(var userID:String,var date:String,var totalNetAsset:Double,var capital:Double,var share:Double,var net:Double,var rate:Double,var dailyReturn:Double){

  def getUserID : String = {
    userID
  }
  def getDate : String = {
    date
  }
  def getShare : Double = {
    share
  }
  def getCapital : Double = {
    capital
  }
  def getTotalNetAsset : Double = {
    totalNetAsset
  }

  def getRate : Double = {
    rate
  }
  def getNet : Double = {
    net
  }
  def getDailyReturn : Double = {
    dailyReturn
  }

  def setUserID(userID:String) : Unit = {
    this.userID = userID
  }
  def setDate(date:String) : Unit = {
    this.date = date
  }
  def setShare(share:Double) : Unit = {
    this.share = share
  }
  def setCapital(capital:Double) : Unit = {
    this.capital = capital
  }
  def setTotalNetAsset(totalNetAsset:Double) : Unit = {
    this.totalNetAsset = totalNetAsset
  }
  def setNet(net:Double) : Unit = {
    this.net = net
  }
  def setRate(rate:Double) : Unit = {
    this.rate = rate
  }
  def setDailyReturn(dailyReturn:Double) : Unit = {
    this.dailyReturn = dailyReturn
  }
}