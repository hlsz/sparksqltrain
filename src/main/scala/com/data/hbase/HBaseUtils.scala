package com.data.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable}
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}

object HBaseUtils {

  /**
    * 设置HBaseConfiguration
    * @param quorum
    * @param port
    * @param tableName
    * @return
    */
  def getHBaseConfiguration(quorum:String, port:String, tableName:String) : Configuration ={
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum",quorum)
    conf.set("hbase.zookeeper.property.clientPort",port)
    conf
  }

  /**
    * 返回或新建HBaseAdmin
    * @param conf
    * @param tableName
    * @return
    */
  def getHBaseAdmin(conf:Configuration, tableName:String): HBaseAdmin ={
    val admin = new HBaseAdmin(conf)
    if(!admin.isTableAvailable(tableName)) {
      val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
      admin.createTable(tableDesc)
    }
    admin
  }
  /**
    * 返回HTable
    * @param conf
    * @param tableName
    * @return
    */
  def getTable(conf:Configuration, tableName:String): HTable ={
    new HTable(conf,tableName)
  }



}
