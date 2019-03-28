package com.data.service

object Test {
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
