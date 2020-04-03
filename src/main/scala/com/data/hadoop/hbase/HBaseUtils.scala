package com.data.hadoop.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Get, HBaseAdmin, HTable, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import scala.collection.JavaConverters._

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
  /**
    * 根据rowkey集合查询hbase数据
    * @param tableName
    * @param keys
    */
  def multipleGet(tableName: String, keys: List[String]) = {
    val hbaseConf = getHBaseConfiguration("localhost","2181",tableName)
    var res : Array[Result] = null
    val hTable = new HTable(hbaseConf,tableName)
    val gets = keys.map(k => new Get(Bytes.toBytes(k))).asJava
    res = hTable.get(gets)
    hTable.close()

    res
  }


}
