package com.data.flink.process.dataset

import org.apache.flink.api.scala._


object FlinkDemo {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
//    fromCollection(env)
    val dataSet = env.readTextFile("/Users/anna/code/word.txt")
    dataSet.flatMap {
      x => x.toLowerCase().split(",")
    }
      .filter(_.nonEmpty)
      .map{(_,1)}
      .sum(1)
      .print()

  }

  def fromCollection(env: ExecutionEnvironment): Unit = {
    import org.apache.flink.api.scala._
    val data = 1 to 10
    env.fromCollection(data).print()
  }

}
