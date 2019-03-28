package scala.service

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object DataFrameRDD {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameRDD")
      .master("local[2]")
      .getOrCreate()

    reflection(spark)

    program(spark)

    spark.stop()
  }

  def reflection(spark:SparkSession): Unit ={

    //得到RDD 每一行一个记录 每一行中的多个字段用逗号分隔
    val rdd = spark.sparkContext.textFile("file:///D:\\code\\java\\sparksqltrain\\src\\Resources\\people.txt")

    //将rdd转成一个case class 这里是People对象
    val rddPeople = rdd.map(_.split(",")).map(line => People(line(0).toInt, line(1), line(2).toInt))

    //将RDD转换成DataFrame 用反射的方式 需要导入隐式转换
    import spark.implicits._
    val peopleDF = rddPeople.toDF()

    peopleDF.show()

    //基于DataFrame的API编程
    peopleDF.filter(peopleDF.col("age") > 30).show()

    peopleDF.filter(peopleDF("age") > 30).show()

    //基于SQL, API编程 把DATAFRRAME注册成一张临时表 表名people
    peopleDF.createOrReplaceTempView("people")
    //注册成全局临时表
    //peopleDF.createOrReplaceGlobalTempView("people")
    spark.sql("select * from people where age > 30").show()

  }

  def program(spark: SparkSession): Unit = {
    val rdd =spark.sparkContext.textFile("file:///D:\\code\\java\\sparksqltrain\\src\\Resources\\people.txt")

    //1.用Rows创建RDD
    val peopleRdd = rdd.map(_.split(",")).map(line => Row(line(0).toInt,line(1), line(2).toInt))
    //2定义schema, 使用structtype来指定
    val structType = StructType(Array(StructField("id", IntegerType, true),
      StructField("name",StringType,true),
      StructField("age",IntegerType,true)))
    //3 把这个schema作用到RDD的Rows上，通过sparkSession。createDataFrame方法
    val peopleDF = spark.createDataFrame(peopleRdd, structType)

    peopleDF.printSchema()
    peopleDF.show()
  }

  //用反射的方式把RDD转DATAFRAME需要用到 case class
  case class People(id:Int, name:String, age:Int)

}
