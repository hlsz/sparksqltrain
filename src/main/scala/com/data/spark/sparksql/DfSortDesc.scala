package com.data.spark.sparksql

import org.apache.spark.sql.SparkSession

object DfSortDesc {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DfSortDesc").master("local").getOrCreate()

    val data = Array((7, 2, 3), (1, 8, 6), (4, 5, 9))
    val df = spark.createDataFrame(data).toDF("col1", "col2", "col3")
    //打印 df
    df.show()
    // 默认的升序
    df.orderBy("col2").show()
    //降序方法一
    df.orderBy(-df("col2")).show
    //降序方法二同上
    df.orderBy(df("col2").desc).show

    import org.apache.spark.sql.functions._
    //降序方法三
    df.orderBy(desc("col2")).show
    //测试方法三
    spark.createDataFrame(data).toDF("col1", "col2", "col3").orderBy(desc("col2")).show

    //降序方法四
    df.orderBy(-col("col2")).show
    //降序方法五
    df.orderBy(col("col2").desc).show
    //sort函数和orderBy用法和结果是一样的
    df.sort(desc("col2")).show
    spark.stop()

  }

}
