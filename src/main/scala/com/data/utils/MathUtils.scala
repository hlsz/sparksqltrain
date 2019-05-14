package com.data.utils

object MathUtils {

   /**
    * sum1、sum2 递归方式 求和
    *
    * @param lst
    * @return
    */

  def sum1(lst : List[Int]):Int= {
    if (lst == Nil) 0 else lst.head + sum1(lst.tail)
  }

  def sum2(lst : List[Int]): Int = lst match {
    case Nil => 0
    case h :: t => h + sum1(t)
  }

  def sum5(lst : List[Double]): Double = lst match {
    case Nil => 0
    case h::t => h + sum5(t)
  }

  /**
    * 迭代器遍历方式 求和
    * @param lst
    * @return
    */
  def sum3(lst : List[Int]): Int = {
    var result = 0
    for (l <- lst) result += l
    result
  }

}
