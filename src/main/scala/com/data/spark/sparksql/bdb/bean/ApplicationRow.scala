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

case class ApplicationRow(var userID:String,var stockID:String,var totalNum:Double,var num:Double,
                          var price:Double, var date:String, var cost:Double){

  def getUserID : String = {
    userID
  }
  def getStockID : String = {
    stockID
  }
  def getTotalNum : Double = {
    totalNum
  }
  def getNum : Double = {
    num
  }
  def getPrice : Double = {
    price
  }
  def getDate : String = {
    date
  }
  def getCost : Double = {
    cost
  }

  def setUserID(userID:String) : Unit = {
    this.userID = userID
  }
  def setStockID(stockID:String) : Unit = {
    this.stockID = stockID
  }
  def setTotalNum(totalNum:Double) : Unit = {
    this.totalNum = totalNum
  }
  def setNum(num:Double) : Unit = {
    this.num = num
  }
  def setPrice(price:Double) : Unit = {
    this.price = price
  }
  def setDate(date:String) : Unit = {
    this.date = date
  }
  def setCost(cost:Double) : Unit = {
    this.cost = cost
  }

}
