package com.data.etl

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

object Oracle2Hive {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Oracle2Hive")
      .master("local")
      .config("spark.sql.parquet.writeLegacyFormat", true)
      .enableHiveSupport()
      .getOrCreate()

    val p = new Properties()
    p.put("driver","oracle.jdbc.driver.oracleDriver")
    p.put("url","jdbc:oracle:thin:@172.0.0.1:1521:bigdata")
    p.put("user","dcetl")
    p.put("password","bigdata")

    import scala.collection.JavaConversions._
    val database_conf: scala.collection.mutable.Map[String, String] = p

    val owner = "BIGDATA"
    val sql_in_owner = s"('${owner}')"

    database_conf.put("dbtable","TEST")

    spark.sql(s"user ${owner}")

    database_conf.put("dbtable", s"(select table_name from all_tables where owner in ${sql_in_owner})")

    val allTableNames = getDataFrame(spark, database_conf)

    database_conf.put("dbtable",s"select * from all_col_comments where owner  in ${sql_in_owner}")
    val allColComments = getDataFrame(spark, database_conf).repartition(160).cache()

    allTableNames.select("table_name").collect().foreach(row => {
      val table_name = row.getAs[String]("table_name")
      database_conf.put("dbtable", table_name)

      val df = getDataFrame(spark, database_conf)

      val colName_comments_map = allColComments.where(s"TABLE_NAME='${table_name}'")
        .select("COLUMN_NAME","COMMENTS")
        .na.fill("", Array("COMMENTS"))
        .rdd.map(row =>  (row.getAs[String]("COLUMN_NAME"), row.getAs[String]("COMMENTS")))
        .collect()
        .toMap

      val colName = ArrayBuffer[String]()
      val schema = df.schema.map(s => {
        if (s.dataType.equals(DecimalType(8,0))){
          colName += s.name
          new StructField(s.name, IntegerType, s.nullable, s.metadata)
            .withComment(colName_comments_map(s.name))
        } else {
          s.withComment(colName_comments_map(s.name))
        }
      })

      import org.apache.spark.sql.functions._
      var df_int = df
      colName.foreach(name => {
        df_int = df_int.withColumn(name, col(name).cast(IntegerType))

      })

      val new_df = spark.createDataFrame(df_int.rdd, StructType(schema))
      new_df.write.mode("overwrite").saveAsTable(table_name)
    })

    spark.stop()


  }

  def getDataFrame(spark: SparkSession, database_conf: scala.collection.Map[String, String]) ={
    spark.read.format("jdbc").options(database_conf).load()
  }

}
