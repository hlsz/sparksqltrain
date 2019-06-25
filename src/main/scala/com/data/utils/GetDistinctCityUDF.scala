package com.data.utils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

object GetDistinctCityUDF extends UserDefinedAggregateFunction{
  /**
    * 输入的数据类型
    * */
  override def inputSchema: StructType = StructType(
    StructField("status",StringType,true) :: Nil
  )
  /**
    * 缓存字段类型
    * */
  override def bufferSchema: StructType = {
    StructType(
      Array(
        StructField("buffer_city_info",StringType,true)
      )
    )
  }
  /**
    * 输出结果类型
    * */
  override def dataType: DataType = StringType
  /**
    * 输入类型和输出类型是否一致
    * */
  override def deterministic: Boolean = true
  /**
    * 对辅助字段进行初始化
    * */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0,"")
  }
  /**
    *修改辅助字段的值
    * */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //获取最后一次的值
    var last_str = buffer.getString(0)
    //获取当前的值
    val current_str = input.getString(0)
    //判断最后一次的值是否包含当前的值
    if(!last_str.contains(current_str)){
      //判断是否是第一个值，是的话走if赋值，不是的话走else追加
      if(last_str.equals("")){
        last_str = current_str
      }else{
        last_str += "," + current_str
      }
    }
    buffer.update(0,last_str)

  }
  /**
    *对分区结果进行合并
    * buffer1是机器hadoop1上的结果
    * buffer2是机器Hadoop2上的结果
    * */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var buf1 = buffer1.getString(0)
    val buf2 = buffer2.getString(0)
    //将buf2里面存在的数据而buf1里面没有的数据追加到buf1
    //buf2的数据按照，进行切分
    for(s <- buf2.split(",")){
      if(!buf1.contains(s)){
        if(buf1.equals("")){
          buf1 = s
        }else{
          buf1 += s
        }
      }
    }
    buffer1.update(0,buf1)
  }
  /**
    * 最终的计算结果
    * */
  override def evaluate(buffer: Row): Any = {
    buffer.getString(0)
  }



  def main(args: Array[String]): Unit = {
    /**
      * 第一步 创建程序入口
      */
    val conf = new SparkConf().setAppName("AralHotProductSpark").setMaster("local")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    //注册成为临时函数
    hiveContext.udf.register("get_distinct_city",GetDistinctCityUDF)
    //注册成为临时函数
    hiveContext.udf.register("get_product_status",(str:String) =>{
      var status = 0
      for(s <- str.split(",")){
        if(s.contains("product_status")){
          status = s.split(":")(1).toInt
        }
      }
    })
  }

}
