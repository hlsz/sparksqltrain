package com.data.hadoop.hbase

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
  *  通过HTable中的Put向HBase写数据
  */
object HBaseWriteTest {

  def main(args: Array[String]): Unit = {

    val sparConf  = new SparkConf().setAppName("HBaseWriteTest").setMaster("local")
    val sc = new SparkContext(sparConf)

    val tableName = "count_click"
    val quorum = "localhost"
    val port = "2181"

    val conf = HBaseUtils.getHBaseConfiguration(quorum, port, tableName)
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val indataRDD = sc.makeRDD(Array("0002,10", "003,10",  "004,50"))
    indataRDD.foreachPartition( x=> {
      val conf =  HBaseUtils.getHBaseConfiguration(quorum, port, tableName)
      conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
      val htable = HBaseUtils.getTable(conf, tableName)

      x.foreach( y=> {
        val arr = y.split(",")
        val key = arr(0)
        val value = arr(1)
        val put = new Put(Bytes.toBytes(key))
        put.add(Bytes.toBytes("info"), Bytes.toBytes("click_count"),
          Bytes.toBytes(value))
      })
    })

    sc.stop()
  }
}

/**
  * 通过TableOutputFormat向HBase写数据
  */
object HBaseWriteTest2 {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("hbasewrite")
      .setMaster("local")
    val sc = new SparkContext(sparkConf)

    val tableName = "course_clickcount"
    val quorum = "localhost"
    val port = "2181"

    //配置相关信息
    val conf = HBaseUtils.getHBaseConfiguration(quorum , port, tableName)
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val jobConf = new JobConf()
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    //写入hbase
    val indataRdd = sc.makeRDD(Array("20180723_02,10","20180723_03,10","20180818_03,50"))

    val rdd = indataRdd.map(_.split(","))
      .map( arr => {
        val put = new Put(Bytes.toBytes(arr(0)))
        put.add(Bytes.toBytes("info"), Bytes.toBytes("click_count"), Bytes.toBytes(arr(1)))
        (new ImmutableBytesWritable, put)
      }).saveAsHadoopDataset(jobConf)

    sc.stop()
  }
}

//通过bulkload向HBase写数据
object HBaseWriteTest03 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("hbasewrite")
      .setMaster("local")
    val sc = new SparkContext(sparkConf)

    val tableName = "course_clickcount"
    val quorum = "localhost"
    val port = "2181"

    val conf = HBaseUtils.getHBaseConfiguration(quorum, port, tableName)
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val table = HBaseUtils.getTable(conf, tableName)
    val job = Job.getInstance(conf)
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputKeyClass(classOf[KeyValue])

    HFileOutputFormat2.configureIncrementalLoadMap(job,table)

    val indataRDD = sc.makeRDD(Array("20180723_02,13","20180723_03,13","20180818_03,13"))
    val rdd = indataRDD.map(x => {
      val arr = x.split(",")
      val kv = new KeyValue(Bytes.toBytes(arr(0)), "info".getBytes, "click_count".getBytes, arr(1).getBytes)
      (new ImmutableBytesWritable(Bytes.toBytes(arr(0))), kv)
    })
    //保存hfile to hdfs
    rdd.saveAsNewAPIHadoopFile("hdfs://localhost/tmp/hbase", classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], conf)

    //bulk 写hfile to HBase
    val bulkLoader = new LoadIncrementalHFiles(conf)
    bulkLoader.doBulkLoad(new Path("hdfs://localhost/tmp/hbase"), table)
  }

}

