package com.scala.test

import com.data.utils.DateUtils
import org.apache.spark.{SparkConf, SparkContext}
import org.junit._

@Test
class AppTest {

    @Test
//    def testOK() = assertTrue(true)
  def test(): Unit =
  {
     val sttr =     DateUtils.intToDateStr(20180903)
    println(sttr)
  }

//    @Test
//    def testKO() = assertTrue(false)

  @Test
  def test2() {
    val conf  = new SparkConf().setMaster("local").setAppName("rdd")
    val sc = new SparkContext(conf)

    val rdd1 = sc.makeRDD(Array(("A",0), ("B", 2), ("B",1),("B",2),("B",3)))
    rdd1.foreach(println(_))


    for (elem <- rdd1.countByKey()){
      print(elem)
    }



  }


}


