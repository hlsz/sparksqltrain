package com.scala.test

import java.io.StringWriter

import au.com.bytecode.opencsv.CSVWriter
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._

case class Data(ip_start:String,ip_end:String,mvgeoid:String,cnt:String )


object WordCount {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordcount").setMaster("local")
    val sc = new SparkSession.Builder().config(conf).getOrCreate()
    val inputRDD = sc.sparkContext.parallelize(List(Data("ip_start","ip_end","mvgeoid","cnt")))
    inputRDD.map(data => List(data.ip_start, data.ip_end, data.mvgeoid, data.cnt).toArray)
      .mapPartitions{ data =>
        val stringWriter = new StringWriter();
        val csvWriter = new CSVWriter(stringWriter);
        csvWriter.writeAll(data.toList)
        Iterator(stringWriter.toString)
      }.saveAsTextFile("e:\\scala_out\\wdout")

    //val  input = sc.sparkContext.textFile("file///D:\\code\\java\\sparksqltrain\\src\\main\\Resources\\ipDatabase.csv")
//    val  input = sc.sparkContext.wholeTextFiles("file///D:\\code\\java\\sparksqltrain\\src\\main\\Resources\\ipDatabase.csv")
//    val results = input.flatMap { case (_, txt) =>
//      val reader = new CSVReader(new StringReader(txt));
//      reader.readAll.map(x => Data(x(0), x(1), x(2), x(3)))
//      reader.readAll()
//    }
//    results.collect().foreach(x => {
//      x.foreach(println);
//      println("=====")
//    })
//    for(res <- results){
//        println(res)
//    }
//    val results = input.map { line:String =>
//      val reader = new CSVReader(new StringReader(line))
//      reader.readNext()
//    }
//    for(result <- results){
//      for(re <- result){
//        println(re)
//      }
//    }


  }


}
