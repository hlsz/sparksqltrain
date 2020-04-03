package com.data.common

import java.text.SimpleDateFormat
import java.util.Date

import akka.actor.Actor
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.functions.udf
import org.joda.time.{DateTime, Days, Months}
import org.joda.time.format.DateTimeFormat


class CommProcess(session: SQLContext, sc:SparkContext){

  val logger = Logger.getLogger(getClass.getName)

  val spark = SparkSession.builder().master("local").getOrCreate()

  val calCumReturn = udf{
    (rates: Seq[Double]) => {
      var annReturn: Double = 1d
      for(i <- rates) {
        annReturn = annReturn * i
      }
      Map("annReturn" -> annReturn)
    }
  }

  val calMaxDrawdown = udf {
    (dailyReturns: Seq[Double]) => {
      var maxDrawdown = 0.0
      var annReturn = 0.0
      //回撤时间占比
      var drawDownDayRatio = 0.0
      //平均回撤值
      var annDrawDownValue : Double = 0.0
      if (dailyReturns.size > 0) {
        var i = 0
        var drawdown = 0.0
        val cumReturns = dailyReturns.map(_ + 1.0).scanLeft(1.0)(_ * _)
        var globalMin = cumReturns(0)
        var pastMaxValue = cumReturns(0)
        var drawDaysNum: Integer = 0
        for(i <- 1 to dailyReturns.size){
          drawdown = (pastMaxValue - cumReturns(i)) / pastMaxValue
          if( drawdown > maxDrawdown) {
            maxDrawdown = drawdown
            globalMin = cumReturns(i)
          }
          if (drawdown < 0) {
            pastMaxValue = cumReturns(i)
          } else if (drawdown > 0) {
            drawDaysNum = drawDaysNum + 1
            annDrawDownValue = annDrawDownValue + drawdown
          }
        }
        annReturn = cumReturns.last
        drawDownDayRatio = drawDaysNum * 1.0 / dailyReturns.size
        annDrawDownValue = annDrawDownValue / dailyReturns.size
      }
      Map("maxDrawdown" -> maxDrawdown, "annReturn" -> annReturn, "drawDownDayRatio" -> drawDownDayRatio,
      "annDrawDownValue" -> annDrawDownValue)
    }
  }

  val mapFundType = udf {
    (fundType: String) => {
      if (fundType != null) {
        fundType match  {
          case "创新封闭式基金" => "封闭式基金"
          case "传统封闭式基金" => "封闭式基金"
          case "平衡混合型" => "混合式"
          case _ => null
        }
      } else {
        null
      }
    }
  }

  def mapStage: UserDefinedFunction = udf((t: Int) => {
    t match {
      case t if t == 0 => "stage1"
      case t if t == 1 => "stage2"
      case t if t == 2 => "stage3"
      case _ => "stage4"
    }
  })

  def getMonth: UserDefinedFunction = udf((s: String) => s.substring(0,6))
  def getMonth2: UserDefinedFunction = udf{ s:String => s.substring(0,6)}
  def getMonth3: UserDefinedFunction = udf {
    (s:String) => {
      s.substring(0,6)
    }
  }

  def formatDate(line: Date) = {
    val date = new SimpleDateFormat("yyyy-mm-dd H:mm:ss")
    val dateFormated = date.format(line)
    val dateFF = date.parse(dateFormated).getTime
    dateFF
  }



  //隐式函数
  implicit def intToString(i: Int)= i.toString

  val firstDay :Date = new Date()


  def stockAgeFunc(date1: Date, date2:Date) = {

    val pattern = "yyyy-MM-dd"
//    val start = new SimpleDateFormat(pattern).parse(startStr)
//    val end = new SimpleDateFormat(pattern).parse(endStr)


    val betweens = date1.getTime - date2.getTime
    val second = betweens / 1000
    val hour = second / 3600
    val day = hour / 24
    val year = day / 365
  }

  def analyse():Unit= {

  }

  val getDaysBetweenFunc = udf {
    dateStr: String =>
      try{
        Days.daysBetween(DateTimeFormat.forPattern("yyyyMMdd").parseDateTime(dateStr), DateTime.now).getDays
      }catch {
        case e:Exception =>
          Days.daysBetween(DateTimeFormat.forPattern("yyyyMMdd").parseDateTime("29990101"), DateTime.now).getDays

      }
  }


  val getMonthsBetweenFunc = udf {
    dateStr: String =>
      try{
        (Months
          .monthsBetween(DateTimeFormat.forPattern("yyyyMMdd").parseDateTime(dateStr), DateTime.now)
          .getMonths() / 12.0).formatted("%.2f")
      }catch {
        case e:Exception =>
          (Months
            .monthsBetween(DateTimeFormat.forPattern("yyyyMMdd").parseDateTime("29990101"), DateTime.now)
            .getMonths() / 12.0).formatted("%.2f")

      }
  }

  val ageFunc = udf{
    dateStr : String =>
      try{
        (Months
          .monthsBetween(DateTimeFormat.forPattern("yyyyMMdd").parseDateTime(dateStr), DateTime.now)
          .getMonths() / 12.0).formatted("%.2f")
      }catch {
        case e: Exception =>
          (Months
            .monthsBetween(DateTimeFormat.forPattern("yyyyMMdd").parseDateTime("29990101"), DateTime.now)
            .getMonths() / 12.0).formatted("%.2f")
      }
  }

  val getSex = udf {sex :String =>
    if(sex != null) {
      sex match {
        case "0" => "男"
        case "1" => "女"
        case _  => null
      }
    }else {
      null
    }
  }

//  Object Email {
//    def apply (user: String, domain:String) = user + "@" + domain
//
//    def unapply(str: String): Option[(String, String)] = {
//      val parts = str split "@"
//      if(parts.length == 2) Some(parts(0), parts(2)) else None
//    }
//  }
  case class Person(name:String, isMale:Boolean,children:Person*)

  val lara = Person("lara",false)
  val bob = Person("bob",true)
  val julie = Person("julie", false, lara, bob)
  val persons = List(lara, bob, julie)

  println(
    persons filter (p => !p.isMale) flatMap (p =>
      (p.children map (c => (p.name, c.name))))
  )

  println(
    for( p<- persons; if !p.isMale; c<- p.children)
      yield (p.name, c.name)
  )

  val cov = session.udf.register("cov", new Covariance)




}



case class ProccessStringMsg(string: String)
case class StringProcessMsg(words: Integer)
class StringCounterActor extends Actor {
  override def receive = {
    case ProccessStringMsg(string) => {
      val wordsInLine = string.split(" ").length
      sender ! StringProcessMsg(wordsInLine)
    }
    case _ => println("Error: message not recognized")
  }
}
