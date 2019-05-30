package com.data.spark.ml

import org.apache.spark.mllib.linalg.{Matrices, Matrix, SingularValueDecomposition, Vectors}
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, RowMatrix}
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary
import org.apache.spark.{SparkConf, SparkContext}

object RowMatrixDedmo extends  App {

  val conf = new SparkConf().setAppName("RowMatrix").setMaster("local")
  val sc = new SparkContext(conf)

  val rdd1 = sc.parallelize(
    Array(
      Array(1.0,2.0,3.0,4.0),
      Array(2.0,3.0,4.0,5.0),
      Array(3.0,4.0,5.0,6.0)
    )
  ).map(f => Vectors.dense(f))

  val rowMatrix = new RowMatrix(rdd1)
  case class MatrixEntry(i:Long, j:Long, value:Double)
  var coordinateMatrix:CoordinateMatrix = rowMatrix.columnSimilarities()

  // 返回矩阵行数， 列数
  println(coordinateMatrix.numCols())
  println(coordinateMatrix.numRows())


  println(coordinateMatrix.entries.collect())

  //轩成后块矩阵
  coordinateMatrix.toBlockMatrix()
  // 转换成索引行矩阵
  coordinateMatrix.toIndexedRowMatrix()
  //转换成RowMatrix
  coordinateMatrix.toRowMatrix()

  //统计列信息
  var mss:MultivariateStatisticalSummary = rowMatrix.computeColumnSummaryStatistics()
  // 每列的均值
  mss.mean
  // 每列的最大值
  mss.max
  // 每列的最小值
  mss.min

  mss.numNonzeros
  mss.normL1
  mss.normL2
  //矩阵列的方差
  mss.variance
  //计算协方差
  val covariance:Matrix =rowMatrix.computeCovariance()

  //计算拉姆矩阵
  var gramianMatrix:Matrix=rowMatrix.computeGramianMatrix()
  //对矩阵进行主成分分析， 参数指定返回的列数，即主成分个数
  //PCA算法是一种经典的降维算法
  var principalComponents = rowMatrix.computePrincipalComponents(2)

  /**
    * 对矩阵进行奇异值分解，设矩阵为A(m x n). 奇异值分解将计算三个矩阵，分别是U,S,V
    * * 它们满足 A ~= U * S * V', S包含了设定的k个奇异值，U，V为相应的奇异值向量
    */

  var svd:SingularValueDecomposition[RowMatrix, Matrix] =
    rowMatrix.computeSVD(3, true)

  // 矩阵相乘积操作
  var multiplyMatrix:RowMatrix=rowMatrix.multiply(Matrices.dense(4,1,Array(1,2,3,4)))









}
