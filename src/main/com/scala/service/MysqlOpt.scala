package com.scala.service

import java.util.Properties

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

case class resultset(name:String,
                     info:String,
                     summary:String)
object MysqlOpt {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val url = "jdbc:mysql://localhost:3306/baidubaike?useUnicode=true&characterEncoding=UTF-8"
    val table = "baike_pages"

    val reader = sqlContext.read.format("jdbc")
    reader.option("url",url)
    reader.option("dbtable",table)
    reader.option("driver","com.mysql.jdbc.Driver")
    reader.option("user","root")
    reader.option("password","admin")
    val df =reader.load()
    df.show()

    val list = List(
      resultset("名字1","标题1","简介1"),
      resultset("名字2","标题2","简介2"),
      resultset("名字3","标题3","简介3"),
      resultset("名字4","标题4","简介4")
    )

    val jdbcDF = sqlContext.createDataFrame(list)
    jdbcDF.collect().take(20).foreach(println)

    val prop = new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","admin")
    jdbcDF.write.mode(SaveMode.Append).jdbc(url,"baike_page",prop)



  }



}
