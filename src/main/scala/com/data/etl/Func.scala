package com.data.etl

import org.apache.spark.SparkConf

object Func {

  val conf = new SparkConf().setMaster("local")

//  def main(args: Array[String]): Unit = {
//
//    //函数赋值给变 时，必须在函数后面加上空格和下划线
//    def sayHello(name:String) {println("hello, " + name)}
//    val sayHelloFunc = sayHello _
//    sayHelloFunc("leo")
//
//    //scala定义匿名函数的语法规则 (参数名: 参数类型) => 函数体
//    val sayHelloFunc1 = (name: String) => println("hello, " + name)
//    sayHelloFunc1("leo")
//
//    //接收其它函数作为参数的函数, 也被称作高阶函数
//    def greeting(func:(String) => Unit, name:String) {func(name)}
//    greeting(sayHelloFunc1, "leo@")
//
//    //高阶函数的另外一个功能是将函数作为返回值
//    def getGreetingFunc(msg: String) = (name: String) => println(msg + "," + name)
//    val greetingFunc = getGreetingFunc("hello")
//    greetingFunc("leo!")
//    greetingFunc("saiwen!")
//
//    //高阶函数可以自动推断出参数类型， 而不是需要写明类型
//    //而且对于只有一个参数的函数， 还可以省去其小括号
//    def greeting1(func: (String) => Unit, name: String) {func(name)}
//    greeting1((name: String) => println("hello, " + name), "leo")
//    greeting1((name) => println("hello, "+ name), "leo#")
//    greeting1(name => println("hello," + name), "leo$")
//
//    //map 对传入的每个元素进行映射， 返回一个处理后的无素
//    println(Array(1,2,3,4,5).map(2 * _))
//    //foreach 对传入的每个元素进行映射,但是没有返回值
//    (1 to 9).map("*" * _).foreach(println _)
//    //filter 对传入的每个元素都进行条件判断，如果对元素返回true, 则保留该元素，否则过滤该元素
//    println((1 to 20).filter( _ % 2 ==0))
//
//    //educeLeft 从左侧元素开始，进行reduce操作，即先对元素1和元素2
//    //进行处理， 然后将结果与元素3处理， 再将结果与元素4处理，依次类推，
//    //相当于1 * 2 * 3 * 4 * 5 * 6
//    println((1 to 9).reduceLeft(_ * _))
//    //sortWith 对元素进行两两对比， 进行排序
//    println(Array(3,2,5,4,10,1).sortWith(_ < _))
//
//    val list = List(1,2,3,4)
//    for(i <- list) {println(i)}
//    def decorator(list: List[Int], prefix:String): Unit = {
//      if(list != Nil){
//        println(prefix + list.head)
//        decorator(list.tail, prefix)
//      }
//    }
//    decorator(list, "list-")
//    val LinkList = scala.collection.mutable.LinkedList(1,2,3,4,5,6)
//    var currentList = LinkList
//    while(currentList != Nil){
//      currentList.elem = currentList.elem * 2
//      currentList = currentList.next
//      println(currentList)
//    }
//
//
//  }

  //scala
  // 初始化代码；从HDFS商的一个Hadoop SequenceFile中读取用户信息
  // userData中的元素会根据它们被读取时的来源，即HDFS块所在的节点来分布
  // Spark此时无法获知某个特定的UserID对应的记录位于哪个节点上
//  val sc = new SparkContext(conf)
  //  val userData = sc.sequenceFile[UserID, UserInfo]("hdfs://...").persist()

//  对userdata 表使用partitionBy()将该表转化为哈希分区
  // 构造100个分区
//  val userData = sc.sequenceFile[UserID, UserInfo]("hdfs://...").partitionBy(new HashPartitioner(100)) .persist()
  
//  // 周期性调用函数来处理过去五分钟产生的事件日志
//  // 假设这是一个包含(UserID, LinkInfo)对的SequenceFile
//  def processNewLogs(logFileName: String) {
//    val events = sc.sequenceFile[UserID, LinkInfo](logFileName)
//    val joined = userData.join(events)// RDD of (UserID, (UserInfo, LinkInfo)) pairs
//    val offTopicVisits = joined.filter {
//      case (userId, (userInfo, linkInfo)) => !userInfo.topics.contains(linkInfo.topic)
//    }.count()
//    println("Number of visits to non-subscribed topics: " + offTopicVisits)
//  }


//  val labelCol = "label"
//  def balanceDataset(dataset: DataFrame):DataFrame ={
//    val numNegatives = dataset.filter(dataset(labelCol) === 0).count()
//    val datasetSize = dataset.count()
//    val balancingRatio = (datasetSize - numNegatives).toDouble /  datasetSize
//
//    val calculateWeights = udf {
//      d: Double =>
//        if (d == 0.0) {
//          1 * balancingRatio
//        } else {
//        (1 * (1.0 - balancingRatio))
//      }
//    }
//
//    val weightedDataset = dataset.withColumn("classWeightCol",
//      calculateWeights(dataset(labelCol)))
//    weightedDataset
//  }

//  val df_weighted = balanceDataset(df)
//  val lr = new LogisticRegression()
//    .setLabelCol(labelCol)
//    .setWeightCol("classWeightCol")

}
