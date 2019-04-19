package com.data.process

import java.sql.DriverManager

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Matrix, Vector, Vectors}
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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

    val rdd = ofpriceDF.rdd

    val  ofprice = rdd.map{
      line =>
        val reslis = new ListBuffer[(Int, String, Double, Double, Double, Double)]()
        val nextNav = 0.0
        val ratioVal = 0.0
        val avgIncome = 0.0
        var nav = line("nav").asInstanceOf[Double]
        for (i <- 1 util line.length ) {
          var  value = line(2).asInstanceOf[Double] / line(3).asInstanceOf[Double]
          reslis += ((line(0).asInstanceOf[Int],
            line(1).asInstanceOf[String],
            line(2).asInstanceOf[Double],
            line(2).asInstanceOf[Double],
            value,
            math.log(value)))     //每产品近一年的日均收益
        }
        reslis.sortBy(_._3).take(2)
    }.collect().flatten

    val result = sc.parallelize(ofprice.map { x => Row(x._1, x._2, x._3, x._4, x._5, x._6) })

    //rdd[Array] 转换为 rdd[Vector]
    val resultVector = {
      result.map { case Row(v:Vector) => v }
    }
    //RDD转换成RowMatrix
    val mat:RowMatrix = new RowMatrix(resultVector)
    //统计
    val statisSummary: MultivariateStatisticalSummary = mat.computeColumnSummaryStatistics()
    //均值
    val mean = statisSummary.mean
    //方差
    statisSummary.variance
    //协方差
    val covariance: Matrix = mat.computeCovariance()


    val  annualizedRate = mean

    val resultSch = spark.createDataFrame(result, schema)
    resultSch.createOrReplaceTempView("resultSchTmp")




    //每产品年化收益



    val textFile = sc.textFile("hdfs://user/hive/warehouse/bigdata.db/prodprice")



    sc.stop()

  }

}

object CovarianceExample {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("CovarianceExample").setMaster("local[8]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    //输入数据
    val data = Array(
      Vectors.dense(4.0, 2.0, 3.0),
      Vectors.dense(5.0, 6.0, 1.0),
      Vectors.dense(2.0, 4.0, 7.0),
      Vectors.dense(3.0, 6.0, 5.0)
    )

    // Array[Vector]转换成DataFrame
    val df = sqlContext.createDataFrame(data.map(Tuple1.apply)).toDF("features")

    // DataFrame转换成RDD
    val rddData=df.select("features").map { case Row(v: Vector) => v}

    // RDD转换成RowMatrix
    val mat: RowMatrix = new RowMatrix(rddData)

    // 统计
    val stasticSummary: MultivariateStatisticalSummary =mat.computeColumnSummaryStatistics()

    // 均值
    println(stasticSummary.mean)
    // 结果：3.5,4.5,4.0

    // 方差
    println(stasticSummary.variance)
    // 结果：1.6666666666666667,3.6666666666666665,6.666666666666667


    // 协方差
    val covariance: Matrix = mat.computeCovariance()
    println(covariance)
    // 结果：
    //  cov(dim1,dim1) cov(dim1,dim2) cov(dim1,dim3)
    //  cov(dim2,dim1) cov(dim2,dim2) cov(dim2,dim3)
    //  cov(dim3,dim1) cov(dim3,dim2) cov(dim3,dim3)
    //  1.6666666666666679   0.3333333333333357   -3.3333333333333304
    //  0.3333333333333357   3.666666666666668    -0.6666666666666679
    //  -3.3333333333333304  -0.6666666666666679  6.666666666666668
    // 结果分析：以cov(dim1,dim2)为例
    //  dim1均值：3.5  dim2均值：4.5
    //  val cov(dim2,dim3)=((4.0-3.5)*(2.0-4.5)+(5.0-3.5)*(6.0-4.5)+(2.0-3.5)*(4.0-4.5)+(3.0-3.5)*(6.0-4.5))/(4-1)
    //  cov(dim2,dim3)=0.3333333333333333
  }

}
