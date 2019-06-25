package com.data.utils

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Administrator
  *         2018/10/16-14:35
  *
  */
object SaveFile {
  var hdfsPath: String = ""
  var proPath: String = ""
  var DATE: String = ""

  val sparkConf: SparkConf = new SparkConf().setAppName(getClass.getSimpleName)
  val sc: SparkContext = new SparkContext(sparkConf)
  val sqlContext: SQLContext = new HiveContext(sc)

  def main(args: Array[String]): Unit = {
    hdfsPath = args(0)
    proPath = args(1)
    //不过滤读取
    val dim_sys_city_dict: DataFrame = readMysqlTable(sqlContext, "TestMysqlTble1", proPath)
    saveAsFileAbsPath(dim_sys_city_dict, hdfsPath + "TestSaveFile", "|", SaveMode.Overwrite)
  }

  /**
    * 获取 Mysql 表的数据
    *
    * @param sqlContext
    * @param tableName 读取Mysql表的名字
    * @param proPath   配置文件的路径
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlTable(sqlContext: SQLContext, tableName: String, proPath: String): DataFrame = {
    val properties: Properties = getProPerties(proPath)
    sqlContext
      .read
      .format("jdbc")
      .option("url", properties.getProperty("mysql.url"))
      .option("driver", properties.getProperty("mysql.driver"))
      .option("user", properties.getProperty("mysql.username"))
      .option("password", properties.getProperty("mysql.password"))
      .option("dbtable", tableName)
      .load()
  }

  /**
    * 将 DataFrame 保存为 hdfs 文件 同时指定保存绝对路径 与 分隔符
    *
    * @param dataFrame  需要保存的 DataFrame
    * @param absSaveDir 保存保存的路径 （据对路径）
    * @param splitRex   指定分割分隔符
    * @param saveMode   保存的模式：Append、Overwrite、ErrorIfExists、Ignore
    */
  def saveAsFileAbsPath(dataFrame: DataFrame, absSaveDir: String, splitRex: String, saveMode: SaveMode): Unit = {
    dataFrame.sqlContext.sparkContext.hadoopConfiguration.set("mapred.output.compress", "false")
    //为了方便观看结果去掉压缩格式
    val allClumnName: String = dataFrame.columns.mkString(",")
    val result: DataFrame = dataFrame.selectExpr(s"concat_ws('$splitRex',$allClumnName) as allclumn")
    result.write.mode(saveMode).text(absSaveDir)
  }

  /**
    * 获取配置文件
    *
    * @param proPath
    * @return
    */
  def getProPerties(proPath: String): Properties = {
    val properties: Properties = new Properties()
    properties.load(new FileInputStream(proPath))
    properties
  }
}
