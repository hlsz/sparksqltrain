package com.data.utils

import org.apache.spark.sql.DataFrame
import org.json4s.DefaultFormats
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, parse, render}


object SparkJson {


  //////////////////////////////////////////////////////////

  type strList= List[String]

  def strToJson(ids:strList):String = {
    // 构造ids 的Json 结构
    val id = ids.map(x=>{
      val vid = x.split("\\|")
      (vid(0),vid(1))
    }).groupBy(_._2).map(x=>(x._1,x._2.map(_._1)))
    val json = id.map{x =>(
      x._1-> x._2
      )}
    compact(render(json))
  }
  def strJsonToJson(ids:strList):String = {
    // 构造ids 的Json 结构
    val id = ids.map(x=>{
      val vid = jsonToList(x)
      (vid(0),vid(1))
    }).groupBy(_._2).map(x=>(x._1,x._2.map(_._1)))
    val json = id.map{x =>(
      x._1-> x._2
      )}
    compact(render(json))
  }
  def listToJson(l:strList):String = compact(render(l))
  def jsonToList(str:String):strList = {
    implicit val formats = DefaultFormats
    val json = parse(str)
    json.extract[strList]
  }
  def saveToES(df:DataFrame) = {
  }
  case class UserTag(sid:String,id:String,idType:String,tag:String)

}
