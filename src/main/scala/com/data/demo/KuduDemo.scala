package com.data.demo

import org.apache.kudu.client.{KuduPredicate, SessionConfiguration}
import org.apache.kudu.spark.kudu.{KuduContext, KuduWriteOptions}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object KuduDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("yarn-client")
    val spark = SparkSession.builder()
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()

    val df = spark.read.format("kudu")
      .options(Map("kudu.master" -> "master:7051", "kudu.table" -> "impala:default.test_table"))
      .load()

    df.createOrReplaceTempView("tmp_table")
    spark.sql("select * from tmp_table limit 10").show()

    val sc = spark.sparkContext

    //批量写kudu
    val kuduMaster = "master:7051"
    var table = "impala:default.test_table"

    val kuduContext = new KuduContext(kuduMaster, sc)
    //kuduContext.upsertRows(df, table, new KuduWriteOptions(false, true))

    //单个读/ 条件写 kudu2
    var kudutable = kuduContext.syncClient.openTable(table)
    val predicate = KuduPredicate.newComparisonPredicate(kudutable.getSchema().getColumn("id"),
      KuduPredicate.ComparisonOp.EQUAL, "testid" )
    val scanner = kuduContext.syncClient.newScannerBuilder(kudutable).addPredicate(predicate).build()

    scanner.hasMoreRows
    val rows = scanner.nextRows()
    rows.hasNext
    val row = rows.next()
    println(row.getString(0))

    //单个写
    val kuduClient =kuduContext.syncClient
    val kuduTable = kuduClient.openTable(table)
    val kuduSession = kuduClient.newSession()

    //AUTO_FLUSH_BACKGROUD AUTO_FLUSH_SYNC
    kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC)
    kuduSession.setMutationBufferSpace(1000)

    val insert = kuduTable.newInsert()
    val row2 = insert.getRow()
    row2.addString(0, "hello")
    kuduSession.apply(insert)
//    kuduSession.flush()

  }

}
