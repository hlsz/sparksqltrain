/**
  * 通联数据机密
  * --------------------------------------------------------------------
  * 通联数据股份公司版权所有 © 2013-2020
  *
  * 注意：本文所载所有信息均属于通联数据股份公司资产。本文所包含的知识和技术概念均属于
  * 通联数据产权，并可能由中国、美国和其他国家专利或申请中的专利所覆盖，并受商业秘密或
  * 版权法保护。
  * 除非事先获得通联数据股份公司书面许可，严禁传播文中信息或复制本材料。
  *
  * DataYes CONFIDENTIAL
  * --------------------------------------------------------------------
  * Copyright © 2013-2020 DataYes, All Rights Reserved.
  *
  * NOTICE: All information contained herein is the property of DataYes
  * Incorporated. The intellectual and technical concepts contained herein are
  * proprietary to DataYes Incorporated, and may be covered by China, U.S. and
  * Other Countries Patents, patents in process, and are protected by trade
  * secret or copyright law.
  * Dissemination of this information or reproduction of this material is
  * strictly forbidden unless prior written permission is obtained from DataYes.
  */

package com.data.spark.sparksql.bdb.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

/**
  * Created by liang on 17-7-19.
  */
class Covariance extends UserDefinedAggregateFunction{
  override def inputSchema: StructType = {
    new StructType().add("x", DoubleType).add("y",DoubleType)
  }

  override def bufferSchema: StructType = {
    new StructType()
      .add("deltaX", DoubleType).add("deltaY", DoubleType)
      .add("count", LongType)
      .add("xAvg", DoubleType).add("yAvg", DoubleType)
      .add("Ck", DoubleType)
      .add("MkX", DoubleType).add("MkY", DoubleType)
      .add("totalCount", LongType)

  }


  override def dataType: DataType = DoubleType


  override def deterministic: Boolean = true


  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0, 0d)
    buffer.update(1, 0d)
    buffer.update(2, 0l)
    buffer.update(3, 0d)
    buffer.update(4, 0d)
    buffer.update(5, 0d)
    buffer.update(6, 0d)
    buffer.update(7, 0d)
    buffer.update(8, 0l)
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer.update(0, input.getAs[Double](0) - buffer.getAs[Double](3))
    buffer.update(1, input.getAs[Double](1) - buffer.getAs[Double](4))
    buffer.update(2, buffer.getAs[Long](2) + 1)
    buffer.update(3, buffer.getAs[Double](3) + buffer.getAs[Double](0)/buffer.getAs[Long](2))
    buffer.update(4, buffer.getAs[Double](4) + buffer.getAs[Double](1)/buffer.getAs[Long](2))
    buffer.update(5, buffer.getAs[Double](5) + buffer.getAs[Double](0)*(input.getAs[Double](1)-buffer.getAs[Double](4)))
    buffer.update(6, buffer.getAs[Double](6) + buffer.getAs[Double](0)*(input.getAs[Double](0)-buffer.getAs[Double](3)))
    buffer.update(7, buffer.getAs[Double](7) + buffer.getAs[Double](1)*(input.getAs[Double](1)-buffer.getAs[Double](4)))
    buffer.update(8, buffer.getAs[Long](2))
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0, buffer1.getAs[Double](3) - buffer2.getAs[Double](3))
    buffer1.update(1, buffer1.getAs[Double](4) - buffer2.getAs[Double](4))
    buffer1.update(8, buffer1.getAs[Long](2) + buffer2.getAs[Long](2))
    buffer1.update(5, buffer1.getAs[Double](5) + buffer2.getAs[Double](5) +
      buffer1.getAs[Double](0)*buffer1.getAs[Double](1)*buffer1.getAs[Long](2)/buffer1.getAs[Long](8)*buffer2.getAs[Long](2))
    buffer1.update(3,(buffer1.getAs[Double](3)*buffer1.getAs[Long](2) + buffer2.getAs[Double](3)*buffer2.getAs[Long](2))/buffer1.getAs[Long](8))
    buffer1.update(4,(buffer1.getAs[Double](4)*buffer1.getAs[Long](2) + buffer2.getAs[Double](4)*buffer2.getAs[Long](2))/buffer1.getAs[Long](8))
    buffer1.update(6, buffer2.getAs[Double](6) + buffer1.getAs[Double](0)*buffer1.getAs[Double](0)*buffer1.getAs[Long](2)/buffer1.getAs[Long](8)*buffer2.getAs[Long](2))
    buffer1.update(7, buffer2.getAs[Double](7) + buffer1.getAs[Double](1)*buffer1.getAs[Double](1)*buffer1.getAs[Long](2)/buffer1.getAs[Long](8)*buffer2.getAs[Long](2))
    buffer1.update(2,buffer1.getAs[Long](8))
  }


  override def evaluate(buffer: Row): Any = {
    val cov = buffer.getAs[Double](5) / (buffer.getAs[Long](2) - 1)
    cov.toDouble
  }

}
