package com.data.service

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkProcessJson {


  case class People(name:String, age:Long)
  case class Employee(name:String, salary:Long)


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

    val conf = new SparkConf()
      .setAppName("SparkJsonJob")
      .setMaster("yarn-client")
      .set("spark.shuffle.compress", "false")


    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .enableHiveSupport()
        .getOrCreate()

    import spark.implicits._
    import org.apache.spark.sql.functions._

    //    val employeePath = this.getClass.getClassLoader.getResource("employee.json").getPath

    val employeeDF =  spark.read.json("employees.json")
    val peopleDF =  spark.read.json("people.json")
    val employeeDS = employeeDF.as[Employee]
    val peopleDS = peopleDF.as[People]

    //字段重名名
    val employeeDS2 =  employeeDS.withColumnRenamed("name","empName")

    peopleDS
      //重复列必须带表名  employeeDS("name") === peopleDS("name")
      //1.select(peopleDS("name")) 选择指定列名
      //2. join.drop(employeeDS("name")) 删除指定列名
      //3. join(df2, Seq("name")) 通过Seq对象实现
      .join(employeeDS, Seq("name"))
      .groupBy(peopleDS("name"))
      .agg(avg("salary").alias("avgSalary"),
        max("salary").alias("maxSalary"),
        min("salary").alias("minSalary"),
        countDistinct("name").alias("cnt"))
      //      .select(peopleDS("name"),$"avgSalary",$"maxSalary",$"cnt")
      .show()



    val userDF = spark.read.json("employees.json")
    userDF.printSchema()
    //生成json数据
    userDF.limit(5).write.mode("overwrite").json("emp.json")

    userDF.show()

    userDF.limit(2).toJSON.foreach(x => println(x))

    userDF.printSchema()

    val userDF2 = userDF.toDF("a","b")

    userDF.show(2)

    userDF.select("name","salary").show(2)

    userDF.selectExpr("name","ceil(salary/1000) as newSalary").show(2)

    userDF.groupBy("name")
      .agg(max("salary"),min("salary"),avg("salary"))
      .show()

    spark.close()
  }

}
