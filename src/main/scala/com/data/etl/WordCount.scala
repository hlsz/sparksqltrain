package com.data.etl

import java.io.{StringReader, StringWriter}

import au.com.bytecode.opencsv.{CSVReader, CSVWriter}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, _}

import scala.collection.JavaConversions._


case class Person(firstName:String, lastName:String, address:List[Address]){
  override def toString = s"Person(firstName=$firstName, lastName=$lastName, address=$address"
}
case class Address(line1:String, city:String, state:String, zip:String) {
  override def toString = s"Address(line1=$line1, city=$city, state=$state, zip=$zip)"
}

// 读取JSON
object WordCount {

  def main(args: Array[String]): Unit = {
    val inputJsonFile = "file:///home/wordcount.json"
    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val input5 = sc.textFile(inputJsonFile)
    val dataObjsRDD = input5.map{ myrecord =>
      implicit val formats = DefaultFormats

      val jsonObj = parse(myrecord)
      jsonObj.extract[Person]
    }
    dataObjsRDD.saveAsTextFile("file:///home/word.json")

  }
}
//读取CSV文件
object WordCount2 {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val input = sc.textFile("/sample_map.csv")
    val result6 = input.map{ line =>
      val reader = new CSVReader(new StringReader(line));
      reader.readNext();
    }
    for(result <- result6){
      for(re <- result){
        println(re)
      }
    }
  }
}

case class Data(index: String, title:String, content:String)

object WordCount3 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val input = sc.wholeTextFiles("d:/sample_map.csv")

    val result = input.flatMap{ case (_, txt) =>
    val reader = new CSVReader(new StringReader(txt));
//      reader.readAll().map( x=> Data(x(0), x(1), x(2)))
      reader.readAll()
    }

    result.collect().foreach( x=> {
      x.foreach(println); println("=======")
    })
  }
}

// 保存CSV
object WordCount4 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val inputRDD = sc.parallelize(List(Data("index", "title", "content")))
    inputRDD.map(data => List(data.index, data.title, data.content).toArray)
      .mapPartitions { data =>
        val stringWriter = new StringWriter();
        val csvWriter = new CSVWriter(stringWriter);
        csvWriter.writeAll(data.toList)
        Iterator(stringWriter.toString)
      }.saveAsTextFile("/home/common/coding/coding/Scala/word-count/sample_map_out")
  }
}

object SparkRDD {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Wordc").setMaster("local")
    val sc = new SparkContext(conf)

    //写sequenceFiles
    val rdd = sc.parallelize(List(("pandas", 3), ("kay", 6),("Snail", 2)))
    rdd.saveAsSequenceFile("output")

    //读取sequenceFile
    val output = sc.sequenceFile("output", classOf[Text], classOf[IntWritable]).
      map{ case (x, y) => (x.toString, y.get())}
    output.foreach(println)
  }
}

object SparkRDD2 {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)

    //使用老式 API 读取 KeyValueTextInputFormat()，以JSON文件为例子
    //注意引用的包是org.apache.hadoop.mapred.KeyValueTextInputFormat
    //    val input = sc.hadoopFile[Text, Text, KeyValueTextInputFormat]("input/test.json").map {
    //      case (x, y) => (x.toString, y.toString)
    //    }
    //    input.foreach(println)

    // 读取文件，使用新的API，注意引用的包是org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat
    val job = new Job()
    val data = sc.newAPIHadoopFile("input/test.json",
      classOf[KeyValueTextInputFormat],
      classOf[Text],
      classOf[Text],
      job.getConfiguration)
    data.foreach(println)

    //保存文件，注意引用的包是org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
    data.saveAsNewAPIHadoopFile(
      "input/test1.json",
      classOf[Text],
      classOf[Text],
      classOf[TextOutputFormat[Text, Text]],
      job.getConfiguration)

  }
}

  object WordCount5 {
    def main(args: Array[String]): Unit = {

      val conf = new SparkConf().setAppName("Wc5").setMaster("local")
      val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
      val sc = spark.sparkContext

      sc.setLogLevel("DEBUG")
      val txtfile = sc.textFile("D:\\code\\java\\sparksqltrain\\src\\main\\scala\\com\\data\\etl\\readme.md").flatMap(_.split(" "))
        .map(word => (word, 1)).reduceByKey(_+_, 1)
        .map(pair => (pair._2, pair._1))
        .sortByKey(false)
        .map(pair => (pair._2, pair._1)).cache()
        //.collect()
      txtfile.foreach(x => println(x))

    }
  }
