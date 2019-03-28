package scala.service

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object HiveContextTest {

  def  main(args :Array[String]): Unit  = {

    val sparkConf = new SparkConf()
    sparkConf.setAppName("HiveContext").setMaster("local[3]")
    val sc = new SparkContext(sparkConf)
    //这个只在spark1.X中使用，2.X已经不用该方法
    val hiveContext = new HiveContext(sc)
    hiveContext.table("emp").show()

    sc.stop()

  }

}
