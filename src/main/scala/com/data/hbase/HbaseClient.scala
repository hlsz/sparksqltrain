package com.data.hbase

object HbaseClient {
  /*把RDD中的列写进hbase
   sc:SparkContext
   tableName: 表名
   servers： 集群服务器别名，比如Master
   indataRDD:包含列数据的rdd
   columnFamily:列族
   newColumn:新列的命名
   2017-09-04
   */
//  def saveNewColumnToHbase(sc:SparkContext, tableName:String, servers:String, indataRDD:RDD[Row], columnFamily:String, newColumn : String) : Unit = {
//    sc.hadoopConfiguration.set("hbase.zookeeper.quorum",servers)
//    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
//    sc.hadoopConfiguration.set("zookeeper.znode.parent","/hbase")
//    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tableName)
//    val job = new Job(sc.hadoopConfiguration)
//    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
//    job.setOutputValueClass(classOf[Result])
//    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
//
//
//    val rdd = indataRDD.map{
//      arr=>{
//        //
//      }}
//    rdd.cache()
//    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())
//  }

}
