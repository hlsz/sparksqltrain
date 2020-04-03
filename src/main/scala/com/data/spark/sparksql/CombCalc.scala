package com.data.spark.sparksql

import org.apache.spark.sql.SparkSession

class CombCalc {


  def sum1(lst: List[Int]):Int={
    if (lst == null) 0 else lst.head + sum1(lst.tail)
  }
  def sum2(lst: List[Int]):Int = lst match {
    case Nil => 0
    case h :: t => h + sum2(t)
  }

//  def statis(weights:Array[Int]): Unit = {
//    val  portReturns = sum1(peMeans * weights);
//    val portVariance = sqrt
//  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate();

  }


}
