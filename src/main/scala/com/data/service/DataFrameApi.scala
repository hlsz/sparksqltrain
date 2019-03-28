package scala.service

import org.apache.spark.sql.SparkSession

object DataFrameApi {

  def main(args: Array[String]): Unit = {

    //Spark SQL的入口点是Spark Session
    val spark = SparkSession.builder()
      .appName("DataFrameApi")
      .master("local[4]")
      .getOrCreate()
    //将json文件加载成一个dataFrame
    val filePath = "file:///D:\\code\\java\\sparksqltrain\\src\\Resources\\people.json"
    val peopleDF = spark.read.format("json").load(filePath)

    //打印schema信息到控制台
    peopleDF.printSchema()

    //展示DataFrame中的记录，默认前20条
    peopleDF.show()

    peopleDF.select("name").show()

    peopleDF.select(peopleDF.col("name"), (peopleDF.col("age")+10).as("age")).show()

    //select * fromm table where age > 30
    peopleDF.filter(peopleDF.col("age") > 30).show()

    //select age,count(*) from table group by age
    peopleDF.groupBy("age").count().show()

    //select * from table where name = "sid" or name = "list"
    peopleDF.filter("name='sid' or name = 'list'" ).show()
    //select * from table where substr(name,0,1) = 's'
    peopleDF.filter("substr(name,0,1) ='s'").show()
    //select * from t order by name,age desc
    peopleDF.sort(peopleDF("name").asc, peopleDF("age").desc).show()

    val peopleDF2 = spark.read.format("json").load(filePath)
    //select * from t1 inner join t2 on t1.age = t2.age
    peopleDF.join(peopleDF2,peopleDF.col("age") === peopleDF2.col("age"),"inner").show()

    spark.stop()


  }

}
