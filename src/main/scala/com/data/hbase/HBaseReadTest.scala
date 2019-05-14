package com.data.hbase

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

object HBaseReadTest{

  def main (args: Array[String] ): Unit = {

    val sparkConf = new SparkConf().setAppName("hbase").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)

    val tableName = "course_clickCount"
    val quorum = "localhost"
    val port = "2181"

    //配置相关信息
    val conf = HBaseUtils.getHBaseConfiguration(quorum,port,tableName)
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    //HBase数据转成RDD
    val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]).cache()

    //RDD数据操作
    val data = hBaseRDD.map( x=> {
      val result = x._2
      val key = Bytes.toString(result.getRow)
      val value = Bytes.toString(result.getValue("info".getBytes, "click_count".getBytes))
      (key, value)
    })
    data.foreach(println)
    sc.stop()
  }

}
