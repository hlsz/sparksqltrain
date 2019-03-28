package scala.service

import org.apache.spark.sql.SparkSession

object ParquetApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ParquetApp")
      .master("local[2]")
      .getOrCreate()

    //不指定format会默认按照parquet处理
    //读json
    val  filePath = "src/Resources/people.json"
    var peopleDFReadJson = spark.read.format("json").load(filePath)
    peopleDFReadJson.printSchema()
    peopleDFReadJson.show()

    //写成parquet
    peopleDFReadJson.write.parquet("file:///D:\\code\\java\\sparksqltrain\\src\\Resources\\people")

    //读parquet看是否正确
    val peopleDFReadParquet = spark.read.format("parquet").load("file:///D:\\code\\java\\sparksqltrain\\src\\Resources\\people")
    peopleDFReadParquet.printSchema()
    peopleDFReadParquet.show()
  }

}
