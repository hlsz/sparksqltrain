package scala.service

import javax.ws.rs.core.Application
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Hello world!
 *
 */
object App extends Application {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("SparkJob")
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .config(conf)
      .getOrCreate()

    import spark.implicits._


    //不限定小集的大小
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
    //每个分区的平均大小不超过spark.sql.autoBroadcastJoinThreshold设置的值
    spark.conf.set("spark.sql.join.preferSortMergeJoin",true)

    println(spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))

    val df1 : DataFrame = Seq(
      ("0", "a"),
      ("1","b"),
      ("2","c")
    ).toDF("id","name")

    val df2 : DataFrame = Seq(
      ("0","d"),
      ("1","e"),
      ("2","f")
    ).toDF("aid","aname")

    //重新分区
    df2.repartition()
    //df1.cache().count()

    val result = df1.join(df2,$"id" === $"aid")

    result.explain()

    result.show()


//     spark.sql("use default")
//     spark.sql("show tables").collect().foreach(println(_))
//     spark.sql("select * from default.prediction limit 100").collect().foreach(println(_))
//     spark.sql("create table if not exists prediction(id int, name string) row format " +
//       " delimited fields terminated by ',' collection items terminated by '-' map keys terminated by  ':'")
//    .collect().foreach(println)
//     spark.sql("insert into prediction  values(1,'big') ").collect().foreach(println)
  //for implicit  conversions like converting RDDS to Dataframes

    val peoplePath = this.getClass.getResource("/people.json").getPath
    println(peoplePath)

    val df = spark.read.json(peoplePath)

    val userPath = this.getClass.getResource("/users.parquet").getPath

    val userParquetDF = spark.read.format("parquet").load(userPath)
    userParquetDF.printSchema()

    val userParquetDF2 = spark.read.parquet(userPath)
    userParquetDF2.printSchema()

    // spark.read.text返回 DataFrame
    val peopleTxt = this.getClass.getResource("/people.txt").getPath
    val rdd =spark.read.text(peopleTxt)
    rdd.show()





    df.show()


  }

 }
