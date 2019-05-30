package com.data.spark.ml

import org.apache.spark.{SparkConf, SparkContext}

object RDDMethod {

  val conf = new SparkConf().setAppName("testRDDMethod").setMaster("local")
    .set("spark.io.compression.codec", "snappy")
  val sc = new SparkContext(conf)

  val array_Int = Array(1,2,3,4,5,6,7)
  val array_String = Array("abc","b","c","d","e")
  val array_Cartesian1 = Array(2,3,4,5,6,7,8)
  val array_Cartesian2 = Array(8,7,6,5,4,3,2)
  val array_Count = Array((1,"cool"),(2,"good"), (1,"bad"),(1,"fine"))
  val array_setKey = Array("one", "two", "three", "four", "five")
  val array_String2 = Array("g", "h", "i", "j", "k", "l", "m")

  val rdd = new RDDMethod()

  def main(args: Array[String]): Unit = {
    aggregate_Max1(array_Int)
    println("*******************************************************************")
    aggregate_Max2(array_Int)
    println("*******************************************************************")
    aggregate_String(array_String)
    println("*******************************************************************")
    use_Cache(array_String)
    println("*******************************************************************")
    use_Foreach(array_String)
    println("*******************************************************************")
    use_Cartesian(array_Cartesian1, array_Cartesian2)
    println("*******************************************************************")
    use_Coalesce(array_Int)
    println("*******************************************************************")
//    use_Repartition(array_Int)
//    println("*******************************************************************")
//    use_CountByValue(array_Int)
//    println("*******************************************************************")
//    use_CountByKey(array_Count)
//    println("*******************************************************************")
//    use_Distinct(array_String)
//    println("*******************************************************************")
//    use_Filter(array_Int)
//    println("*******************************************************************")
//    use_FlatMap(array_Int)
//    println("*******************************************************************")
//    use_Map(array_Int)
//    println("*******************************************************************")
//    use_groupBy(array_Int)
//    println("*******************************************************************")
//    use_KeyBy(array_SetKey)
//    println("*******************************************************************")
//    use_reduce(array_String)
//    println("*******************************************************************")
//    use_SortBy(array_Count)
//    println("*******************************************************************")
//    use_zip(array_Int, array_String, array_String2)

    sc.stop()
  }

  def aggregate_Max1(array:Array[Int]): Unit ={
    val arr = sc.parallelize(array)
    val result  = arr.aggregate(0)(math.max(_,_), _+_)
    println(result)
  }

  def aggregate_Max2(array: Array[Int]): Unit ={
    println("aggregate_Max2")
    val arr = sc.parallelize(array, 2)
    val result = arr.aggregate(0)(math.max(_,_), _+_)
  }

  def aggregate_String(array:Array[String]): Unit ={
    println("aggregate_String")
    val result = sc.parallelize(array).aggregate("")((value,word) => value + word, _+_)
    println(result)
  }

  def use_Cache(array: Array[String]): Unit ={
    val arr = sc.parallelize(array)
    println(arr)
    println(arr.cache())

  }

  def use_Foreach(array:Array[String]): Unit = {
    sc.parallelize(array).foreach(println)
  }

  //创建两个数组并进行笛卡尔积
  def use_Cartesian(array1:Array[Int], array2:Array[Int]): Unit = {
    sc.parallelize(array1).cartesian(sc.parallelize(array2)).foreach(println)
  }

  def use_Coalesce(array:Array[Int]): Unit ={
    val arr1 = sc.parallelize(array)
    val arr2 = arr1.coalesce(2,true)
    val result = arr1.aggregate(0)(math.max(_,_), _+_)
    val result2 = arr2.aggregate(0)(math.max(_,_), _+_)
    println(result)
    println(result2)
  }

  def use_repartition(array:Array[Int]): Unit = {
    val arr_num = sc.parallelize(array).repartition(3).partitions.length
    println(arr_num)
  }

  //此方法是计算数据集是某个数据出现的次数，并将其以map的形式返回
  def use_CountByValue(array:Array[Int]): Unit ={
    sc.parallelize(array).countByValue().foreach(println)
  }

  def use_CountByKey(array:Array[(Int, String)]): Unit ={
    sc.parallelize(array).countByKey().foreach(println)
  }

  def use_Distinct(array:Array[String]): Unit ={
    sc.parallelize(array).distinct().foreach(println)
  }

  def use_Filter(array:Array[Int]): Unit ={
    sc.parallelize(array).filter(_ > 3).foreach(println)
  }

  def use_FlatMap(array:Array[Int]): Unit ={
    sc.parallelize(array).flatMap(x => List(x + 1)).collect().foreach(print)
  }

  def use_Map(array:Array[Int]): Unit ={
    sc.parallelize(array).map(x=> List(x+1)).collect().foreach(println)
  }

  def use_GroupBy(array:Array[Int]): Unit ={
    val arr = sc.parallelize(array)
    arr.groupBy(rdd.myFilter(_), 1).foreach(println)
    arr.groupBy(rdd.myFilter2(_), 2).foreach(println)
  }

  //keyby方法
  //此方法是为数据集中的每个个体数据增加一个key，从而可以与原来的个体数据形成键值对
  def use_KeyBy(array:Array[String]): Unit ={
    sc.parallelize(array).keyBy(word => word.size).foreach(println)
  }

  def use_reduce(array:Array[String]): Unit ={
    val arr = sc.parallelize(array)
    //字符串拼接
    arr.reduce(_+_).foreach(println(_))
    //打印最长字符串
    arr.reduce(funLength).foreach(println(_))

  }

  /**
    * 寻找最长字符串
    * @param str1
    * @param str2
    * @return
    */
  def funLength(str1:String, str2:String): String ={
    var str = str1
    if (str2.size > str.size){
      str = str2
    }
    return str
  }

  def use_SortBy(array:Array[(Int, String)]): Unit = {
    var str = sc.parallelize(array)
    //按照第一个数据进行排序
    str = str.sortBy(word => word._1, true)
    //按照第二个数据进行排序
    val str2 = str.sortBy(word => word._2, true)
    str.foreach(println(_))
    str2.foreach(println(_))
  }

  //zip 此方法将若干个rdd压缩成一个新的rdd,形成一系列的键值对存储形式的RDD

  def use_zip(array1:Array[Int], array2:Array[String], array3:Array[String]): Unit ={
    array1.zip(array2).zip(array3).foreach(println(_))
  }





}

class RDDMethod{
  def myFilter(num:Int): Unit = {
    num >= 3
  }

  def myFilter2(num:Int): Unit ={
    num < 3
  }
}