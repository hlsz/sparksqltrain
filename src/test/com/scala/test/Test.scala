package com.scala.test

object Test {
  def sum(xs: List[Int]): Int = {
    def doSum(acc: Int, list: List[Int]): Int = list match {
      case Nil => acc
      case head :: tail => doSum(head + acc, tail)
    }

    doSum(0, xs)
  }


  def main(args: Array[String]): Unit = {
    new Test().run()
  }
}

class Test {
  def run(): Unit = { // TODO Auto-generated method stub
    System.out.println("测试========》")
    val s1 = this.getClass.getResource("").getPath
    val s2 = this.getClass.getResource("/").getPath
    System.out.println(s1)
    System.out.println(s2)
  }
}
