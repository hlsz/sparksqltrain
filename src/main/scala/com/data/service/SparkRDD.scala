package scala.service

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}

object SparkRDD {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Workc").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd = sc.parallelize(List(("Panda", 3),("Ray",6),("Sarial",3)))
    rdd.saveAsTextFile("E:\\scala_out\\output")

    val output = sc.sequenceFile("E:\\scala_out\\output",classOf[Text], classOf[IntWritable])
      .map{case (x, y) => (x.toString, y.get())}
    output.foreach(println)

  }

}
