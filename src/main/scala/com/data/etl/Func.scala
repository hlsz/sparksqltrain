package com.data.etl

object Func {

  def main(args: Array[String]): Unit = {

    //函数赋值给变 时，必须在函数后面加上空格和下划线
    def sayHello(name:String) {println("hello, " + name)}
    val sayHelloFunc = sayHello _
    sayHelloFunc("leo")

    //scala定义匿名函数的语法规则 (参数名: 参数类型) => 函数体
    val sayHelloFunc1 = (name: String) => println("hello, " + name)
    sayHelloFunc1("leo")

    //接收其它函数作为参数的函数, 也被称作高阶函数
    def greeting(func:(String) => Unit, name:String) {func(name)}
    greeting(sayHelloFunc1, "leo@")

    //高阶函数的另外一个功能是将函数作为返回值
    def getGreetingFunc(msg: String) = (name: String) => println(msg + "," + name)
    val greetingFunc = getGreetingFunc("hello")
    greetingFunc("leo!")
    greetingFunc("saiwen!")

    //高阶函数可以自动推断出参数类型， 而不是需要写明类型
    //而且对于只有一个参数的函数， 还可以省去其小括号
    def greeting1(func: (String) => Unit, name: String) {func(name)}
    greeting1((name: String) => println("hello, " + name), "leo")
    greeting1((name) => println("hello, "+ name), "leo#")
    greeting1(name => println("hello," + name), "leo$")

    //map 对传入的每个元素进行映射， 返回一个处理后的无素
    println(Array(1,2,3,4,5).map(2 * _))
    //foreach 对传入的每个元素进行映射,但是没有返回值
    (1 to 9).map("*" * _).foreach(println _)
    //filter 对传入的每个元素都进行条件判断，如果对元素返回true, 则保留该元素，否则过滤该元素
    println((1 to 20).filter( _ % 2 ==0))

    //educeLeft 从左侧元素开始，进行reduce操作，即先对元素1和元素2
    //进行处理， 然后将结果与元素3处理， 再将结果与元素4处理，依次类推，
    //相当于1 * 2 * 3 * 4 * 5 * 6
    println((1 to 9).reduceLeft(_ * _))
    //sortWith 对元素进行两两对比， 进行排序
    println(Array(3,2,5,4,10,1).sortWith(_ < _))

    val list = List(1,2,3,4)
    for(i <- list) {println(i)}
    def decorator(list: List[Int], prefix:String): Unit = {
      if(list != Nil){
        println(prefix + list.head)
        decorator(list.tail, prefix)
      }
    }
    decorator(list, "list-")
    val LinkList = scala.collection.mutable.LinkedList(1,2,3,4,5,6)
    var currentList = LinkList
    while(currentList != Nil){
      currentList.elem = currentList.elem * 2
      currentList = currentList.next
      println(currentList)
    }














  }

}
