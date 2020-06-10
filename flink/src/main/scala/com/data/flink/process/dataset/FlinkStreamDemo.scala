package com.data.flink.process.dataset

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object FlinkStreamDemo {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val dataStream: DataStream[String] = env.socketTextStream("127.0.0.1",9000)

    dataStream.flatMap{ line => line.toLowerCase().split(",")}
      .filter(_.nonEmpty)
      .map {word => (word, 1)}
      .keyBy(0)
      .timeWindow(Time.seconds(3))
      .sum(1)
      .print()

    env.execute("streaming wordcount")

  }

}
