package com.data.process

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession



case class Info(id:Long,name:String)


object BizProcessMain {

  private val conf = new SparkConf()
    .setAppName("BizProcessMain")
    //rdd压缩  只有序列化后的RDD才能使用压缩机制
    .set("spark.rdd.compress", "true")

  //设置并行度
    .set("spark.default.parallelism", "24")
    //使用Kryo序列化库
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrationRequired", "true")
    .registerKryoClasses(Array(classOf[Info], classOf[scala.collection.mutable.WrappedArray.ofRef[_]]))
    //优化shuffle 读写
    .set("spark.shuffle.file.buffer","128k")

    .set("spark.reducer.maxSizeInFlight","96M")
    //优化kryo缓存存放class大小
    .set("spark.kryoserializer.buffer.mb","155")
    //合并map端输出文件
    .set("spark.shuffle.consolidateFiles", "true")
    //设置executor堆外内存
//    .set("spark.yarn.executor.memoryOverhead","2048M")
//    .setMaster("yarn-client")

  val spark = SparkSession
    .builder()
    .config(conf)
    //解决DecimalType存储精度问题， parquet格式 spark和hive不统一
    .config("spark.sql.parquet.writeLegacyFormat", true)
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse/bigdata.db")
    //数据倾斜
    .config("spark.sql.shuffle.partitions", 30)
    .enableHiveSupport()
    .getOrCreate()


  // 设置参数
  // hive > set  hive.exec.dynamic.partition.mode = nonstrict;
  // hive > set  hive.exec.dynamic.partition = true;
  spark.sql("set  hive.exec.dynamic.partition.mode = nonstrict")
  spark.sql("set  hive.exec.dynamic.partition = true")


  def main(args: Array[String]): Unit = {

    spark.sql("use bigdata")

    spark.sql(
      s"""
         | create table if not exists dm_branch_client_cnt
         | (
         | branch_no string
         | ,client_cnt int
         | ) ROW FORMAT DELIMITED FIELDS TERMINATED BY  ${raw"'\t'"}
         | LINES TERMINATED BY ${raw"'\n'"}
         | stored as textfile
     """.stripMargin)
    val branchClientCntDF = spark.sql(
      s"""
         | select
         | case when branch_no is null or branch_no='' then -1 else branch_no end branch_no
         | ,client_cnt
         | from (
         |      select   branch_no, count(*) client_cnt
         |      from  hs08_client_for_ai
         |      group by branch_no
         |     )
       """.stripMargin.replace("\r\n"," "))
    branchClientCntDF.createOrReplaceTempView("branchClientCntTmp")

    spark.sql("insert overwrite table  dm_branch_client_cnt select * from branchClientCntTmp ")

    var calcuDate = args(0).toInt
    var maxIntervalVal = args(1).toInt

    if (args.length == 2)
      {
        new DmCusttotalassetdmProc().custtotalassetdmProc(calcuDate, maxIntervalVal)
        new  DmDeliverProc().deliverProc(calcuDate, maxIntervalVal)
        new DmBankTransferProc().bankTransferProc(calcuDate, maxIntervalVal)

      } else if (args.length == 3){
        var minintervalVal =   args(2).toInt
        new DmCusttotalassetdmProc().custtotalassetdmProc(calcuDate, maxIntervalVal, minintervalVal)
        new  DmDeliverProc().deliverProc(calcuDate, maxIntervalVal, minintervalVal)
        new DmBankTransferProc().bankTransferProc(calcuDate, maxIntervalVal, minintervalVal)

      } else{
        println("参数错误! 请输入正确的参数. BizProcessMain 20190401 3 1 ")
        return
    }

    // com.data.process.BizProcessMain 20190401 3
    //spark-submit --principal 'me@DOMAIN.HAD' \ --keytab '/home/me/me.keytab' \












  }

}
