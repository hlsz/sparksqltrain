package com.data.spark.stream

import java.util

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaReceiverWordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("kafkaStream")
    val streamContext = new StreamingContext(conf, Seconds(10))

    val topicThreadMap: util.Map[String, Integer] = new util.HashMap[String,Integer]

    topicThreadMap.put("WordCount",1)

    val zkQuorun = "hadoop-100:2181,hadoop-101:2181,hadoop-102：2181"
    val group = "DefaultConsumerGroup"
    val topics = "wordCount"
    val numThreads = 1

    val topicMap = topics.split(",").map((_,numThreads))
//    val lines = KafkaUtils.createDirectStream(streamContext, zkQuorun, group, topicMap).map(_._2)
//
//    val words = lines.flatMap(line => line.split(" "))
//    val wordsNumber = words.map(word => (word, 1))
//    val result = wordsNumber.reduceByKey(_+_)
//    result.print()
//
//
//    streamContext.start()
//    streamContext.awaitTermination()









  }

}
