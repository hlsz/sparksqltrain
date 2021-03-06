package com.data.flink.process.datastream

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object SocketWindowWordCount {

  case class WordWithCount(word:String, count:Long)

  def main(args: Array[String]): Unit = {

    val port : Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e:Exception =>  {
        System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'")
        return
      }
    }

    //获取运行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //连接此socket获取输入数据
    val text = env.socketTextStream("node21", port,'\n')
    //需要加上这一行隐式转换
    import org.apache.flink.api.scala._
    //解析数据， 分组，窗口化,并且聚合求SUM
    val windowCounts = text
      .flatMap(w => w.split("\\s"))
      .map{ w => WordWithCount(w, 1)}
      .keyBy("word")
      .timeWindow(Time.seconds(5),Time.seconds(1))
      .sum("count")

    windowCounts.print().setParallelism(1)
    env.execute("Socket Window WordCount")



  }

}
