package com.data.spark.ml

import com.data.spark.ml.RowMatrixDedmo.MatrixEntry
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.sql.SparkSession

object MatrixSimTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .appName("sim")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val df = spark.createDataFrame(Seq(
      (0, 0, 1.0),
      (1, 0, 1.0),
      (2, 0, 1.0),
      (3, 0, 1.0),
      (0, 1, 2.0),
      (1, 1, 2.0),
      (2, 1, 1.0),
      (3, 1, 1.0),
      (0, 2, 3.0),
      (1, 2, 3.0),
      (2, 2, 3.0),
      (0, 3, 1.0),
      (1, 3, 1.0),
      (3, 3, 4.0)
    ))
  }

}
