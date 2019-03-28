package scala.service

import java.util.Properties

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkSqlToMysql {

  def main(args: Array[String]): Unit = {

    val spark : SparkSession =
      SparkSession
      .builder()
      .appName("SparkSqlToMysql")
      .master("local")
      .getOrCreate()

    val sc : SparkContext =
      spark.sparkContext
    val fileRDD : RDD[String] =
      sc.textFile("people.txt")
    //切分
    val lineRDD : RDD[Array[String]] =
      fileRDD.map(_.split(","))

    //关联 通过StructType 指定schema将rdd转换成DataFrame
    val rowRDD : RDD[Row] = lineRDD.map(x => Row(x(0).toInt,x(1),x(2).toInt))
    val schema = (new StructType).add("id",IntegerType,true).add("name",StringType, true)
      .add("age",IntegerType, true)

    val peopleDF:DataFrame = spark.createDataFrame(rowRDD, schema)

    val resultDF : DataFrame =spark.sql("select * from people order by age desc")

    val prop: Properties = new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","admin")

    resultDF.write.mode("error").jdbc("jdbc:mysql://local:3306/spark","person",prop)

    spark.stop()
  }

}
