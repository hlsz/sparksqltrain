package com.data.etl

import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{StringType, _}
import org.apache.spark.sql.{Encoder, Row, SparkSession}

import scala.collection.mutable

object Test {



  def main(args: Array[String]): Unit = {
    print("======================")
    val conf = new SparkConf().setMaster("local").setAppName("demo")
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
    val  a_line = spark.sparkContext.textFile("D://1.txt")
    val  a_tg_product = a_line.map( x=> x.split(","))
    val  al_cust = a_tg_product.map( r=> Row(r(0).toString, r(1).toDouble,r(2).toDouble
    ))

    val  tg_line = spark.sparkContext.textFile("D://2.txt")
    val  tg_product = tg_line.map( x=> x.split(","))
    val  tg_cust = tg_product.map( r=> Row(r(0).toString, r(1).toDouble,r(2).toDouble

    ))
    val sechem :StructType = StructType(mutable.ArraySeq(
      StructField("c_custno",StringType,nullable = false),
      StructField("lastdate_dvalue",DoubleType,nullable = false),
      StructField("age",DoubleType, nullable= false)
    ))
    val  tb = spark.createDataFrame(tg_cust,sechem)
    tb.createOrReplaceTempView("BUYTG_STANDARD")
    spark.sql("select c_custno,lastdate_dvalue,age from BUYTG_STANDARD").show()
    val  tb2 = spark.createDataFrame(al_cust,sechem)
    tb2.createOrReplaceTempView("al_cust")
    val sechem2 :StructType = StructType(mutable.ArraySeq(
      StructField("c_custno",StringType,nullable = false),
      StructField("sim_cust",StringType,nullable = false),
      StructField("degree",DoubleType, nullable= false)
    ))
    val buytg_rows = spark.sql("""select C_CUSTNO,LASTDATE_DVALUE,AGE
      from  BUYTG_STANDARD""")

    val allcust_rows = spark.sql("""select substr(C_CUSTNO,3)C_CUSTNO,LASTDATE_DVALUE,AGE
      from  al_cust""")





//    allcust_rows.rdd.toMap.map(x=>println(x))
def mapSort(map:Map[String,Double]):Unit = {
   val sort_map =map.toSeq.sortBy(_._2).reverse
  println(sort_map)
  for(i <- 1 to sort_map.size){
   if(sort_map(0)._2*0.7 > sort_map(1)._2){
     println(1)
   }else {println(2)}
  }
    println(map.toSeq.sortBy(_._2).reverse)
//  return 0
}
    implicit val mapEncoder: Encoder[Map[String, Double]] = org.apache.spark.sql.Encoders.kryo[Map[String,Double]]
    val tt = allcust_rows.map(_.getValuesMap[Double](List("C_CUSTNO","LASTDATE_DVALUE","AGE")))
//      foreach(x=>println(x))
//     tt.foreach(x=>println(x))
    println("ddddddddd")

      tt.foreach(x=>((x("C_CUSTNO"),mapSort(x.-("C_CUSTNO")))))



//      .map(x=>(x("C_CUSTNO")->x.dropRight(1))).collect().map(x=>println(x))
//    allcust_rows.map(t=>t.getAs[String]("C_CUSTNO"))
    val bd = spark.sparkContext.broadcast(buytg_rows.collect())
//    bd.value.map(y=>println(y(1)))
//    bd.value.map(y=>print(y(0)))
    val data = allcust_rows.rdd.map{
      x=>
//      val arlis = new ListBuffer[(String,String,Double)]()
      bd.value.map{
      y=>var s=0.0
        for (i <-1 to x.length-1){
          s=s+math.pow(x(i).asInstanceOf[Double]-y(i).asInstanceOf[Double],2)
      }
        ((x(0).asInstanceOf[String],y(0).asInstanceOf[String],
          (100/1+math.sqrt(s)).formatted("%.2f").toDouble))

    }.sortBy(_._3).reverse.take(2)
    }.collect().flatten
      val df =spark.sparkContext.parallelize(data)
//     df.saveAsTextFile("D://out2")
//      val sim_tb = spark.createDataFrame(df,sechem2)
//
//      sim_tb.persist(StorageLevel.MEMORY_AND_DISK_SER).
//    sim_tb.write().mode(SaveMode.Overwrite).save("hdfs://hadoop1:9000/output/namesAndFavColors_scala");
//      sim_tb.createOrReplaceTempView("result")
//       spark.sql("select * from result").show()show
//    allcust_rows.rdd.map(x=>print(x(0))).collect()

//    val sim = allcust_rows.rdd.map(line=>buytg_rows.rdd.map{ y=>
//      var s =0.0
//
//      for (i <- 1 to 3){
////        println("AL :"+line.getDouble(i)+"/"+y.getDouble(i))
//      }
//      //     ((line(0),y(0),(1/(1+math.sqrt(s)))*100).format("%.2f"))
//    }).collect()
  }


}
