package com.data.process

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession



case class Info(id:Long,name:String)


object BizProcessMain {

  private val conf = new SparkConf()
    .setAppName("BizProcessMain")
    //rdd压缩  只有序列化后的RDD才能使用压缩机制
//    .set("spark.rdd.compress", "true")

  //设置并行度
//    .set("spark.default.parallelism", "24")
//    //使用Kryo序列化库
//    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    .set("spark.kryo.registrationRequired", "true")
//    .registerKryoClasses(Array(classOf[Info], classOf[scala.collection.mutable.WrappedArray.ofRef[_]]))
    //优化shuffle 读写
//    .set("spark.shuffle.file.buffer","128k")
//
//    .set("spark.reducer.maxSizeInFlight","96M")
//    //优化kryo缓存存放class大小
//    .set("spark.kryoserializer.buffer.mb","155")
//    //合并map端输出文件
//    .set("spark.shuffle.consolidateFiles", "true")
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

    if (args.length < 2){ System.exit(0); println("参数错误! 请输入正确的参数. BizProcessMain 20190401 3 1 ")}

    var calcuDate = args(0).toInt
    var maxIntervalVal = args(1).toInt

    if (args.length < 3)
      {
        new DmCusttotalassetdmProc().custtotalassetdmProc(calcuDate, maxIntervalVal)
        new  DmDeliverProc().deliverProc(calcuDate, maxIntervalVal)
        new DmBankTransferProc().bankTransferProc(calcuDate, maxIntervalVal)

      } else {
        var minintervalVal =   args(2).toInt
        new DmCusttotalassetdmProc().custtotalassetdmProc(calcuDate, maxIntervalVal, minintervalVal)
        new  DmDeliverProc().deliverProc(calcuDate, maxIntervalVal, minintervalVal)
        new DmBankTransferProc().bankTransferProc(calcuDate, maxIntervalVal, minintervalVal)

      }

    //计算客户年龄及排名、均值、中值
    new CalcuteCustInfo().calcuteCustInfo(calcuDate)

    // com.data.process.BizProcessMain 20190401 3
    //spark-submit --principal 'me@DOMAIN.HAD' \ --keytab '/home/me/me.keytab' \












  }

}
