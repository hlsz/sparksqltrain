package com.data.demo

object For_Function_Advanced {

  def main(args: Array[String]): Unit = {

    for (i <- 1.to(2); j <- 1.to(2)) print((100 * i + j) + " ")
    println

    for(i <- 1.to(2); j <- 1.to(2) if i != j) print((100 * i + j) + " ")
    println

    def addA(x : Int) = x + 100
    println("the result is:" +addA(1))

    val add = (x:Int) => x + 100
    println("this result from a val is :" + add(2))

    /**
      * 递归调用
      * 在scala中必须指明函的返回值
      */
    def fac(n: Int) : Int = if(n <= 0) 1 else n * fac(n-1)
    println("the result from a fac is:" + fac(10))

    /**
      * 函数的默认值
      * @param content
      * @param left
      * @param right
      * @return
      */
    def combine(content  : String,
                left     : String = "{",
                right    : String = "}") = left + content + right
    println("result from a combine is " + combine("hello"))
    println("result from a combin is : "+ combine("hello", "(", ")"))

  }

}
