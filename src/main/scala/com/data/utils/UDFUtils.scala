package com.data.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
case class Employee(name: String, salary: Long)
case class Average(var sum: Long, var count: Long)


object UDFUtils {

  object MyAverageUDAF extends UserDefinedAggregateFunction {
    //Data types of input arguments of this aggregate function
    def inputSchema:StructType = StructType(StructField("inputColumn", LongType) :: Nil)
    // Data types of values in the aggregation buffers
    def bufferSchema:StructType = {
      StructType(StructField("sum", LongType):: StructField("count", LongType) :: Nil)
    }
    //The data type of the returned value
    def dataType:DataType = DoubleType
    //Whether this function always returns the same output on the identical input
    def deterministic: Boolean = true
    //Initializes the given aggregation buffer. The buffer itself is a `Row` that inaddition to
    // standard methods like retrieving avalue at an index (e.g., get(), getBoolean()), provides
    // the opportunity to update itsvalues. Note that arrays and maps inside the buffer are still
    // immutable.
    def initialize(buffer:MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = 0L
    }
    //Updates the given aggregation buffer `buffer` with new input data from `input`
    def update(buffer:MutableAggregationBuffer, input: Row): Unit ={
      if(!input.isNullAt(0)) {
        buffer(0) = buffer.getLong(0)+ input.getLong(0)
        buffer(1) = buffer.getLong(1)+ 1
      }
    }
    // Mergestwo aggregation buffers and stores the updated buffer values back to `buffer1`
    def merge(buffer1:MutableAggregationBuffer, buffer2: Row): Unit ={
      buffer1(0) = buffer1.getLong(0)+ buffer2.getLong(0)
      buffer1(1) = buffer1.getLong(1)+ buffer2.getLong(1)
    }
    //Calculates the final result
    def evaluate(buffer:Row): Double =buffer.getLong(0).toDouble /buffer.getLong(1)
  }


  object MyAverageAggregator extends Aggregator[Employee, Average, Double] {

    // A zero value for this aggregation. Should satisfy the property that any b + zero = b
    def zero: Average = Average(0L, 0L)
    // Combine two values to produce a new value. For performance, the function may modify `buffer`
    // and return it instead of constructing a new object

    def reduce(buffer: Average, employee: Employee): Average = {
      buffer.sum += employee.salary
      buffer.count += 1
      buffer
    }
    // Merge two intermediate values
    def merge(b1: Average, b2: Average): Average = {
      b1.sum += b2.sum
      b1.count += b2.count
      b1

    }
    // Transform the output of the reduction
    def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count

    // Specifies the Encoder for the intermediate value type


    def bufferEncoder: Encoder[Average] = Encoders.product
    // Specifies the Encoder for the final output value type
    def outputEncoder: Encoder[Double] = Encoders.scalaDouble

  }

  val conf = new SparkConf().setMaster("local").setAppName("UDF")
  val spark = SparkSession.builder().config(conf).getOrCreate()

  def main(args: Array[String]): Unit = {
    spark.udf.register("myAverage",MyAverageUDAF)


  }

}


