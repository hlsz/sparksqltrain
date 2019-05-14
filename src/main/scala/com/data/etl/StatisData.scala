package com.data.etl

import java.sql.DriverManager

import com.data.utils.MathUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

class StatisData {

  val conf = new  SparkConf().setMaster("local").setAppName("StatisData")
  val spark = SparkSession.builder().config(conf).getOrCreate()
  val sc = spark.sparkContext

  def getConnection() = {
    Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()
    DriverManager.getConnection("jdbc:oracle:thin:@192.168.1.116:1521:orcl","dcetl","bigdata#6868")
  }

//  val rdd = new JdbcRDD(sc,getConnection,
//  "SELECT * FROM table WHERE ? <= ID AND ID <= ?",Long.MinValue, Long.MaxValue, 2 ,(r: ResultSet) => { r.getInt("id")+"\t"+r.getString("name")}
//  )

  val rdd = new JdbcRDD(
    sc,
    () => {
      Class.forName("oracle.jdbc.driver.OracleDriver").newInstance()
      DriverManager.getConnection("jdbc:oracle:thin:@//192.168.1.110:1521/bigdata","dcetl","bigdata#6868")
    },
    "select * from tab01 where id = ? and rownum  < ?",
    1,10,1,
    r => (r.getString(1), r.getString(2), r.getString(3)))
  rdd.collect().foreach(println(_))


//  val jdbcDF = spark.sqlContext.read.format("jdbc").options(
  ////    Map("url" ->"jdbc:oracle:thin:ddcetl/bigdata#6868@//192.168.1.110:1521/bigdata",
  ////      "dbtable" -> "( select * from T_LG_GNLK ) a",
  ////      "driver" -> "oracle.jdbc.driver.OracleDriver",
  ////      "numPartitions" -> "5",
  ////      "partitionColumn"->"OBJECTID",
  ////      "lowerBound"->"0",
  ////      "upperBound"->"80000000")).load()
  ////  val size= jdbcDF.rdd.partitions.size


  val rdd2: RDD[Int] = ???

  val sorted = rdd2.sortBy(identity).zipWithIndex().map {
    case (v, idx) => (idx, v)
  }

  val count = sorted.count()

  val median: Double = if (count % 2 == 0) {
    val l = count / 2 - 1
    val r = l + 1
    (sorted.lookup(l).head + sorted.lookup(r).head).toDouble / 2
  } else sorted.lookup(count / 2).head.toDouble


  val schema:StructType = StructType(mutable.ArraySeq(
    StructField("init_date",IntegerType,false),
    StructField("fund_code",StringType,false),
    StructField("nav",DoubleType,false),
    StructField("next_nav",DoubleType,false),
    StructField("ratio_val",DoubleType,false),
    StructField("avg_income",DoubleType,false)
  ))

  val resultSchema:StructType = StructType(mutable.ArraySeq(
    StructField("fund_code",StringType,false),
    StructField("mean",DoubleType,false)
  ))


  def randomNew(n:Int)={
    var resultList:List[Double]=Nil
    while(resultList.length<n) {
      val randomNum=(new Random).nextDouble()
      if(!resultList.contains(randomNum)){
        resultList=resultList:::List(randomNum)
      }
    }
    resultList
  }

  def randomNew2(n:Int)={
    var arr= 0 to 20 toArray
    var outList:List[Int]=Nil
    var border=arr.length//随机数范围
    for(i <- 0 until  n){//生成n个数
      val index=(new Random).nextInt(border)
      println(index)
      outList=outList:::List(arr(index))
      arr(index)=arr.last//将最后一个元素换到刚取走的位置
      arr=arr.dropRight(1)//去除最后一个元素
      border-=1
    }
    outList
  }

  //获取产品组合
  def getCombination(prodInfo:List[String]): List[List[String]] ={
    val combins =   prodInfo.combinations(3).toList
    combins
  }

  def statistics(weight:List[Int]): Unit ={
    // combination
    val portReturns = MathUtils.sum1(weight)

  }

  def riskProd(): Unit ={

    spark.sql("drop table if exists tmp_ofprice")
    val ofpriceDF = spark.sql(
      s"""
         | select  a1.init_date, a1.fund_code, a1.nav
         | from dcraw.hs08_his_ofprice@czdcdb2 a1
         | and a1.init_date >= 20180101
         | and a1.init_date < 20190101
         | order by init_date,fund_code
       """.stripMargin.replace("\r\n"," ")).persist(MEMORY_AND_DISK)
//    ofpriceDF.createOrReplaceTempView("ofpriceTmp")
//    spark.sql("create table if not exists tmp_ofprice as select * from ofpriceTmp ")

    import spark.implicits._

    val prodCodeArray:DataFrame = ofpriceDF.select("fund_code").distinct()


    val rdd = ofpriceDF.rdd

    val  ofprice = rdd.map{
      line =>
        val reslist = new ListBuffer[(Int, String, Double, Double, Double, Double)]()
        val nextNav = 0.0
        val ratioVal = line(2).asInstanceOf[Double] / line(3).asInstanceOf[Double]
        val avgIncome = math.log(ratioVal)
        reslist += ((line(0).asInstanceOf[Int], //init_date
          line(1).asInstanceOf[String], //prod_code
          line(2).asInstanceOf[Double], //nav
          nextNav,
          ratioVal,
          avgIncome
          ))     //每产品近一年的日均收益
        reslist.sortBy(_._2).take(2)
    }
    val ofpriceRdd = ofprice.collect()
//    val result = sc.parallelize(ofpriceRdd.map { x => Row(x._1, x._2, x._3, x._4, x._5, x._6) })


    val ofpriceMeanResult = ofprice.toDF().select("fund_code","nav").groupBy("fund_code")
      .agg( "nav" -> "mean").withColumnRenamed("mean(nav)","mean_nav") // mean(nav)
      .limit(68).select("fund_code","mean_nav"  )


    // inner, outer, left_outer, right_outer, leftsemi
    // df.join(df2, df("id") === df2("id"), "inner")

    val noa = 3
    var weights = randomNew(3)

    sc.stop()

  }

}
