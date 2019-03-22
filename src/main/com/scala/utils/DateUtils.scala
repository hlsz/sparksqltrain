package com.scala.utils

import java.util.{Date, Locale}

import org.apache.commons.lang3.time.FastDateFormat

object DateUtils {

  val YYYYMMDDHHMM_TIME_FORMAT = FastDateFormat.getInstance("dd/MM/yyyy:HH:mm:ss Z",Locale.ENGLISH)

  val TARGET_FORMAT = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")

  def parse(time:String){
    TARGET_FORMAT.format(new Date(getTime(time)))
  }

  /**
    * 获取输入日志Long类型的时间
    *
    */
  def getTime(time: String): Long ={
    try{
      YYYYMMDDHHMM_TIME_FORMAT.parse(time.substring(time.indexOf("[") + 1, time.lastIndexOf("]"))).getTime()
    }catch{
      case e:Exception =>{
          0L
      }
    }
  }

  def main(args:Array[String]): Unit ={
    print(parse("[10/Nov/2016:00:01:02 +0800]"))
  }

}
