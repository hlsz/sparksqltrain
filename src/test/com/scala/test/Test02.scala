package com.scala.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object Test02 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("test02")
      .setMaster("local[1]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    val df = spark.read.json("file:///D:\\code\\java\\sparksqltrain\\src\\resources\\people.json")
    df.show()
    df.printSchema()
    import spark.implicits._
    df.select("name").show()
    df.select($"name",$"age"+1).show()
    df.filter($"age" > 21).show()
    df.groupBy("age").count().show()

//    df.write.bucketBy(42,"name")
//      .sortBy("age")
//      .saveAsTable("people_buck")

//    df.write.partitionBy("favorite_color")
//        .format("parquet")
//        .save("namesPartByColor.parquet")

//    df.write.partitionBy("favorite_color")
//        .bucketBy(42, "name")
//        .saveAsTable("user_part_buck")

//    df.cube("city","year").agg(sum("amount") as 'amount').show()
//    df.rollup("city","year").agg(sum("amount") as "amount").show()
    //pivot 只能跟在groupby之后
//    df.groupBy("year").pivot("city", Seq("war","Boston","Toronto")).agg(sum("amount") as "amount").show()

    //读写mysql数据
//    df.repartition(1).write.mode("append")
//        .option("user","root")
//        .option("password","admin")
//        .jdbc("jdbc:mysql://localhost:3306/test","alluxio",new Properties())

//    val fromMysql = spark.read
//      .option("user","root")
//      .option("password","admin")
//      .jdbc("jdbmc:mysql://localhost:3306/test","alluxio",new Properties())

    val sc = spark.sparkContext
    val lineCounter = sc.longAccumulator("lineCounter")

    val clientRdd =  sc.textFile("/users/hdfs/hs08_client_for_ai")
    val MinFieldsLength = 53
    val VTitleIndex = 11

    val inputPath = "/user/spark/input/att"
    val outputPath = "/user/spark/output"


    val resultRdd = clientRdd.map(_.split("\t"))
      .filter(
        fields => {
          lineCounter.add(1)
          if(fields.length < MinFieldsLength) false else true
        }
      )
      .map(fields => fields(VTitleIndex).length)
      .persist(StorageLevel.MEMORY_ONLY)

    resultRdd.saveAsTextFile(outputPath)
    val maxTitleLength = resultRdd.reduce((a,b) => if(a>b) a else b)

    println(s"Line count: ${lineCounter.value}")
    println(s"Max title length: ${maxTitleLength}")


    spark.stop()
  }



}
