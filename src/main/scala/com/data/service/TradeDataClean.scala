package com.data.service

import java.util.Date

import com.data.utils.DateUtils
import org.apache.spark.sql.SparkSession

object TradeDataClean {

  //  def main(args: Array[String]): Unit = {
  //    val conf = new SparkConf().setAppName("TradeDataClean").setMaster("local")
  //    val sc = new SparkContext(conf)
  //    val input = sc.textFile("hdfs://bigdata1:9000/bplan/data-center/alitradelist.log.2018-06-21")
  //
  //    input.collect().foreach(
  //      x => {
  //        println(x);
  //        val json = JSON.parseObject(x)
  //        println("====value====")
  //        println(json)
  //        println(json.getString("agentId"))
  //      }
  //    )
  //
  //    sc.stop()
  //  }

  /**
    * 如果有参数，直接返回参数中的值，如果没有默认是前一天的时间
    * @param args        :系统运行参数
    * @param pattern     :时间格式
    * @return
    */
  //  def gainDayByArgsOrSysCreate(args: Array[String],pattern: String):String = {
  //    //如果有参数，直接返回参数中的值，如果没有默认是前一天的时间
  //    if(args.length > 1) {
  //      args(1)
  //    } else {
  //      val previousDay = DateUtils.addOrMinusDay(new Date(), -1);
  //      DateUtils.dateFormat(previousDay, "yyyy-MM-dd");
  //    }
  //  }

  /**
    * args(0)      :要处理的json文件路径
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("TradeDataClean")
      //.master("local[*]")
      .config("spark.sql.warehouse.dir","/user/hive/warehouse")
      //为解决：Use the CROSS JOIN syntax to allow cartesian products between these relations
      //.config("spark.sql.crossJoin.enabled",true)
      //.config("spark.sql.warehouse.dir","hdfs://bigdata1:9000/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate();

    //val previousDayStr = gainDayByArgsOrSysCreate(args,"yyyy-MM-dd")

    //val df = spark.read.json("/bplan/data-center/tradeInfo/"+ previousDayStr +"/tradeInfo.json")
    val df = spark.read.json(args(0))
    //val df = spark.read.json("hdfs://bigdata1:9000/xxx/xxx/xxxx")
    spark.sql("use data_center")
    df.createOrReplaceTempView("tb_trade_info_temp");

    val previousDay = DateUtils.addOrMinusDay(new Date(), -1)
    //val tmepRdd = rs.rdd.saveAsTextFile("hdfs://bigdata1:9000/bplan/data-center/temp.txt")
    val pt_createDate = DateUtils.dateFormat(previousDay, "yyyyMMdd");
    spark.sql("INSERT INTO TABLE tb_trade_info partition(pt_createDate=" + pt_createDate + ") " +
      "SELECT " +
      "    ttit.agentId as agentId, " +
      "    from_unixtime(ttit.payTimeUnix,'yyyyMMdd') as createDate " +
      "FROM " +
      "    tb_sys_industry si,  " +
      "    tb_shop ts," +
      "    tb_trade_info_temp ttit " +
      "WHERE " +
      "    si.category_id = ts.industryId  " +
      "    and ts.shopId = ttit.shopId" +
      "    and ts.storeType != 10");

    spark.stop()
  }
}
