package com.data.common

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.joda.time.DateTime

import scala.concurrent._
import scala.concurrent.duration._

object Schedule {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("calc")
    val sc = new SparkContext(conf)

    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive","true")
    val session = new HiveContext(sc)
      FileOperator.setConf(sc)
    DataProcess.setConf(sc.getConf)
    DataProcess.setSession(session)

    val flag = sc.getConf.get("spark.schedule.batch")
    val module = sc.getConf.get("spark.schedule.module")

    val commProces = new CommProcess(session,sc)


    val startDate = sc.getConf.get("spark.schedule.start")
    val header = sc.getConf.get("spark.schedule.header")
    var endDate = DateTime.now.toString("yyyyMMdd")


    if(!sc.getConf.contains("spark.schedule.keepNum")){
      sc.getConf.set("spark.schedule.keepNum","4")
    }
    if(sc.getConf.contains("spark.schedule.end")){
      endDate = sc.getConf.get("spark.schedule.end")
    }

    DataProcess.loadData(session,0,startDate,endDate, header, true)

    import scala.concurrent.ExecutionContext.Implicits.global

    val f = List(
      Future{
        sc.setLocalProperty("spark.schedule.pool","ability")
        commProces.analyse()
      },
      Future{
        sc.setLocalProperty("spark.scheduler.pool","user")
        commProces.analyse()
      }
    )
    f.map(a => {
      Await.result(a, 1 days)
    })

  sc.stop()



  }

}
