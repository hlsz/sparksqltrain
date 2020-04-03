package com.data.common

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext

object FileOperator {

  var fileSystem:FileSystem = null
  var dumpPath :String = null
  def setConf(sc: SparkContext): Unit ={
    val hdfsConf = sc.hadoopConfiguration
    val fs = org.apache.hadoop.fs.FileSystem.get(hdfsConf)
    fileSystem = fs
    dumpPath = sc.getConf.get("spark.schedule.dumpPath")
  }

  def checkExist(name:String) :Boolean ={
    fileSystem.exists(new Path(name))
  }

  def deleteDir(name: String) :Unit ={
    fileSystem.delete(new Path(dumpPath+name), true)
  }

}
