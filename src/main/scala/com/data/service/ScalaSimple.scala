package com.data.service

import java.io.File

import org.apache.spark.{SparkConf, SparkContext}

object ScalaSimple {

  def main(args: Array[String]): Unit = {

//      val properties = new Properties()
//
//      properties.load(this.getClass.getResourceAsStream("/client.properties"))
//
//      val configuration = ClientUtils.initConfiguration()
//
//      ClientUtils.initKerberosENV(configuration, false, properties)
//
//      val fileSystem = FileSystem.get(configuration)
//
//    //使用Oozie运行Spark任务时，需要手动设置系统参数
//    System.setProperty("hive.metastore.uris", "thrift://vt-hadoop2:9083")

    val conf = new SparkConf()
      .setAppName("SparkJob")
      .set("spark.shuffle.compress","false")
//      .setMaster("yarn-client")
//      .set("yarn.resourcemanager.address","http://vt-hadoop1:8032")
//      .set("yarn.resourcemanager.scheduler.address","http://vt-hadoop1:8030")
//      .set("spark.yarn.preserve.staging.files","false")
//      .set("spark.yarn.dist.files","yarn-site.xml")
//      .set("yarn.resourcemanager.hostname","vt-hadoop1")

    val spark = new SparkContext(conf)
    val OUTPUT_HDFS = "output_hdfs"
    val outputPath : File = new File(OUTPUT_HDFS)
    if(outputPath.exists())
      deleteDir(outputPath)

    val input = spark.textFile("install.log")
    val words = input.flatMap(line => line.split(" "))
    val mapWords = words.map((_,1))
    val counts = mapWords.reduceByKey{case (x, y) => x + y}
    counts.take(10).foreach(println)

    counts.saveAsTextFile(OUTPUT_HDFS)
    val collection = counts.collect()
    println(s"Result: $collection")








  }

  def deleteDir(dir: File) :Unit = {
    val files = dir.listFiles()
    files.foreach(f => {
      if(f.isDirectory) {
        deleteDir(f)
      }else{
        f.delete()
        println(s"Delete File: ${f.getAbsolutePath}")
      }
    })
    dir.delete()
    println(s"Delete Dir: ${dir.getAbsolutePath}")
  }


}
