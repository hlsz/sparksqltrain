package com.data.spark.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object UserSimilar {

  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  val conf = new SparkConf().setMaster("local")
    .setAppName("UserSimilar")

  val sc = new SparkContext(conf)

  val users = sc.parallelize(Array("张三", "李四", "王五", "赵六", "阿七"))
  val films = sc.parallelize(Array("逆战", "人间", "鬼屋", "西游记", "雪豹"))

  var source = Map[String, Map[String, Int]]()

  //存放电影分的map
  val filmSource = Map[String, Int]()

  def getSource(): Map[String, Map[String,Int]] = {

    //设置电影评分
    val user1FilmSource = Map("逆战" -> 2, "人间" -> 3, "鬼屋" -> 1, "西游记" -> 0, "雪豹" -> 1)
    val user2FilmSource = Map("逆战" -> 1, "人间" -> 2, "鬼屋" -> 2, "西游记" -> 1, "雪豹" -> 4)
    val user3FilmSource = Map("逆战" -> 2, "人间" -> 1, "鬼屋" -> 0, "西游记" -> 1, "雪豹" -> 4)
    val user4FilmSource = Map("逆战" -> 3, "人间" -> 2, "鬼屋" -> 0, "西游记" -> 5, "雪豹" -> 3)
    val user5FilmSource = Map("逆战" -> 5, "人间" -> 3, "鬼屋" -> 1, "西游记" -> 1, "雪豹" -> 2)

    //对人名进行储存
    source += ("张三" -> user1FilmSource)
    source += ("李四" -> user2FilmSource)
    source += ("王五" -> user3FilmSource)
    source += ("赵六" -> user4FilmSource)
    source += ("阿七" -> user5FilmSource)

    source

  }

  def getCollaborateSource(user1:String, user2:String): Double = {

    val user1FilmSource = source.get(user1).get.values.toVector
    val user2FilmSource = source.get(user2).get.values.toVector
    //对公式部分分子进行计算
    val member = user1FilmSource.zip(user2FilmSource).map(d => d._1 * d._2).reduce(_+_).toDouble
    //求出分母第一个变量值
    val temp1 = math.sqrt(user1FilmSource.map(num => {math.pow(num, 2)}).reduce(_+_))
    //求出分母第二个变量值
    val temp2 = math.sqrt(user2FilmSource.map(num => {math.pow(num, 2)}).reduce(_+_))
    //求出分母
    val denominator = temp1 * temp2
    //进行计算
    member / denominator
  }

  def main(args: Array[String]): Unit = {
    //初始化分数
    getSource()

    val name1 = "张三"
    val name2 = "李四"
    val name3 = "王五"
    val name4 = "赵六"
    val name5 = "阿七"

    users.foreach(user => {
      println(name1 +" 相对于 " + user + "的相似性分数是" + getCollaborateSource(name2, user))
    })
    println("--------------------------------------------------------------------------")

    users.foreach(user => {
      println(name2 + " 相对于 " + user + " 的相似性分数是 " + getCollaborateSource(name2, user) )
    })

    println("--------------------------------------------------------------------------")

    users.foreach(user => {
      println(name3 + " 相对于 " + user + " 的相似性分数是 " + getCollaborateSource(name3, user) )
    })

    println("--------------------------------------------------------------------------")

    users.foreach(user => {
      println(name4 + " 相对于 " + user + " 的相似性分数是 " + getCollaborateSource(name4, user) )
    })

    println("--------------------------------------------------------------------------")

    users.foreach(user => {
      println(name5 + " 相对于 " + user + " 的相似性分数是 " + getCollaborateSource(name5, user) )
    })
  }
}
