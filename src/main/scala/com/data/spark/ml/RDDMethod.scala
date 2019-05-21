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

}

class RDDMethod{
  def myFilter(num:Int): Unit = {
    num >= 3
  }

  def myFilter2(num:Int): Unit ={
    num < 3
  }
}