package com.data.utils

import java.sql.{Connection, DriverManager}
import java.util.{LinkedList, ResourceBundle}

import org.apache.spark.internal.Logging

/**
  * 数据库连接池工具类
  */
object DBConnectionPool extends Logging{
  private val reader = ResourceBundle.getBundle("properties/version_manager_jdbc")
  private val max_connection = reader.getString("max_connection") //连接池总数
  private val connection_num = reader.getString("connection_num") //产生连接数
  private var current_num = 0 //当前连接池已产生的连接数
  private val pools = new LinkedList[Connection]() //连接池
  private val driver = reader.getString("driver")
  private val url = reader.getString("url")
  private val username = reader.getString("username")
  private val password = reader.getString("password")
  /**
    * 加载驱动
    */
  private def before() {
    if (current_num > max_connection.toInt && pools.isEmpty()) {
      println("busyness")
      Thread.sleep(2000)
      before()
    } else {
      Class.forName(driver)
    }
  }
  /**
    * 获得连接
    */
  private def initConn(): Connection = {
    val conn = DriverManager.getConnection(url, username, password)
    //    logError(url)
    conn
  }


  /**
    * 初始化连接池
    */
  private def initConnectionPool(): LinkedList[Connection] = {
    AnyRef.synchronized({
      if (pools.isEmpty()) {
        before()
        for (i <- 1 to connection_num.toInt) {
          pools.push(initConn())
          current_num += 1
        }
      }
      pools
    })
  }
  /**
    * 获得连接
    */
  def getConn():Connection={
    initConnectionPool()
    pools.poll()
  }
  /**
    * 释放连接
    */
  def releaseCon(con:Connection){
    pools.push(con)
  }


}

