package com.data.spark.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

class WordCount {
  def stream(): Unit = {
    val conf = new SparkConf().setMaster("local")
      .setAppName("stream")
    val streamingContext = new StreamingContext(conf, Seconds(10))

    val lines = streamingContext.socketTextStream("localhost", 9999)
    val words = lines.flatMap(line => line.split(" "))
    val wordNumbers = words.map((word => (word, 1)))

    val result  = wordNumbers.reduceByKey(_ + _)
    result.print()

    streamingContext.start()

    streamingContext.awaitTermination()
  }


}

object WordCount {
  def main(args: Array[String]): Unit = {
    new WordCount().stream()
  }
}
