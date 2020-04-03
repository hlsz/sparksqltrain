package com.data.common

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

class Covariance extends UserDefinedAggregateFunction{

  override def inputSchema: StructType = {
    new StructType().add("x", DoubleType).add("y", DoubleType)
  }

  override def bufferSchema: StructType = {
    new StructType()
      .add("deltaX", DoubleType)
      .add("deltaY", DoubleType)
      .add("count", LongType)
      .add("xAvg", DoubleType)
      .add("yAvg", DoubleType)
      .add("Ck", DoubleType)
      .add("MkX", DoubleType)
      .add("MkY", DoubleType)
      .add("totalCount", LongType)
  }

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, 0d)
    buffer.update(1, 0d)
    buffer.update(2, 0L)
    buffer.update(3, 0d)
    buffer.update(4, 0d)
    buffer.update(5, 0d)
    buffer.update(6, 0d)
    buffer.update(7, 0d)
    buffer.update(8, 0L)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0, input.getAs[Double](0) - buffer.getAs[Double](3))
    buffer.update(1, input.getAs[Double](1) - buffer.getAs[Double](4))
    buffer.update(2, input.getAs[Long](2) + 1)
    buffer.update(3, input.getAs[Double](3) + buffer.getAs[Double](0) / buffer.getAs[Double](2)  )
    buffer.update(4, input.getAs[Double](4) + buffer.getAs[Double](1) / buffer.getAs[Double](2) )
    buffer.update(5, input.getAs[Double](5) + buffer.getAs[Double](0)*(input.getAs[Double](1) - buffer.getAs[Double](4)))
    buffer.update(6, input.getAs[Double](6) + buffer.getAs[Double](0)*(input.getAs[Double](0) - buffer.getAs[Double](3)))
    buffer.update(7, input.getAs[Double](7) + buffer.getAs[Double](1)*(input.getAs[Double](1) - buffer.getAs[Double](4)))
    buffer.update(8, input.getAs[Long](2) )
  }
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0, buffer1.getAs[Double](3) - buffer2.getAs[Double](3))
    buffer1.update(1, buffer1.getAs[Double](4) - buffer2.getAs[Double](4))
    buffer1.update(8, buffer1.getAs[Long](2) + buffer2.getAs[Double](2))
    buffer1.update(5, buffer2.getAs[Double](4) + buffer2.getAs[Double](5) +
      buffer1.getAs[Double](0) * buffer1.getAs[Double](1) * buffer1.getAs[Long](2) / buffer1.getAs[Long](8) *  buffer1.getAs[Long](2) )
    buffer1.update(3, buffer1.getAs[Double](3) + buffer1.getAs[Double](1) / buffer1.getAs[Double](2) )
    buffer1.update(4, buffer1.getAs[Double](4) + buffer1.getAs[Double](0)*(buffer1.getAs[Double](1) - buffer1.getAs[Double](4)))
    buffer1.update(6, buffer1.getAs[Double](6) + buffer1.getAs[Double](0)*(buffer1.getAs[Double](0) - buffer1.getAs[Double](3)))
    buffer1.update(7, buffer1.getAs[Double](7) + buffer1.getAs[Double](1)*(buffer1.getAs[Double](1) - buffer1.getAs[Double](4)))
    buffer1.update(2, buffer1.getAs[Long](8) )
  }

  override def evaluate(buffer: Row): Any = {
    val cov = buffer.getAs[Double](5) / (buffer.getAs[Long](2) -1)
    cov.toDouble
  }

}
