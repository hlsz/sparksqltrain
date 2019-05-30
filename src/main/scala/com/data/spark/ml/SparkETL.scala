package com.data.spark.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{VectorAssembler, VectorIndexer}
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

case class People(affairs:Double, gender:String, age:Double, yearsmarried:Double,
                  children:String, religiousness:Double, education:Double,
                  occupation:Double, rating:Double)
object SparkETL {

  def main(args: Array[String]): Unit = {

      val spark = SparkSession.builder()
          .appName("spark gbdt")
          .master("local")
          .enableHiveSupport()
          .getOrCreate()
      val sc = spark.sparkContext

      import spark.implicits._

      val cols = Array("affairs", "gender", "age", "yearsmarried", "children", "religiousness", "education", "occupation", "rating")
      val textFile = sc.textFile("file:///Users/anna/IdeaProjects/sparksqltrain/src/resources/modeldata.txt")

      //在未知列的情况下输出结构和数据
      // split("\\s+")
      // 从第一行数据获取列数, 并设置列名
      val colsLength = textFile.first.split(",").length
      val colNames = new Array[String](colsLength)
      for(i <- 0 until colsLength) {
          colNames(i) = "col"+ (i + 1)
      }


//      val rowRDD2 = textFile.map(_.split(",")).map(
//          p => {
//          val ppp = p.map(x => x.replace("\"", "").trim)
//          Row(ppp:_*)
//          })
      val rowRDD = textFile.map(_.split(",")).map(p =>
                Row(p(0).toDouble,
                    p(1).replace("\"",""),
                    p(2).toDouble,
                    p(3).toDouble,
                    p(4).replace("\"",""),
                    p(5).toDouble,
                    p(6).toDouble,
                    p(7).toDouble,
                    p(8).toDouble))
//      val rowRDD3 = textFile.map(_.split(","))
//        .map(p => People(p(0).toDouble, p(1), p(2).toDouble,
//          p(3).toDouble, p(4), p(5).toDouble, p(6).toDouble, p(7).toDouble, p(8).toDouble))
//      rowRDD3.toDF()
      val rowRDD4 = textFile.map(_.split(",").map(_.toDouble)).map(Row(_:_*))

//      val detailRow:RDD[Row]  = textFile.map(line => {
//          val splits = line.split(",")
//          val affairs = splits(0).toDouble
//          val gender = splits(1).replace("\"","").trim
//          val age = splits(2).toDouble
//          val yearsmarried = splits(3).toDouble
//          val children = splits(4).replace("\"","").trim
//          val religiousness = splits(5).toDouble
//          val education = splits(6).toDouble
//          val occupation = splits(7).toDouble
//          val rating = splits(8).toDouble
//          Row(affairs, gender, age, yearsmarried, children, religiousness, education, occupation, rating)
//      })


      // val schema2 = StructType(cols.map(fieldName => StructField(fieldName, DoubleType(),true)))
      val schema = StructType(List(
          StructField("affairs", DoubleType, true),
          StructField("gender", StringType, true),
          StructField("age", DoubleType, true),
          StructField("yearsmarried", DoubleType, true),
          StructField("children", StringType, true),
          StructField("religiousness", DoubleType, true),
          StructField("education", DoubleType, true),
          StructField("occupation", DoubleType, true),
          StructField("rating", DoubleType, true)
        ))
//    val dataList: List[(Double, String, Double, Double, String, Double, Double, Double, Double)]
      val data = spark.createDataFrame(rowRDD, schema)

      // 自定义列查询, 转变列的类型
      import org.apache.spark.sql.functions._
      var colsname ="affairs,gender,age,yearsmarried,children"
      data.select(colsname.split(",").map(name => col(name)):_*).show()
      data.select(colsname.split(",").map(name => col(name).cast(DoubleType)):_*).show()

      // 转换所有列为double
//      val fieldNames = data.columns
//      var df = data
//      for(colName <- fieldNames){
//          df = df.withColumn(colName, col(colName).cast(DoubleType))
//      }
//      df.show()

      data.printSchema()
      data.show(false)



      // 保存数据
//      data.coalesce(1).write.format("parquet")
//          .mode(SaveMode.Append)
//          .partitionBy("day")
//          .save("/user/anna/code/people")


    // GBDT建模
      data.createOrReplaceTempView("data")

    val labelWhere = "affairs as label"
    val genderWhere = "case when gender='female' then 0 else cast(1 as double) end as gender"
    val childrenWhere = "case when children='no' then 0 else cast(1 as double) end as children"

    val dataLabelDF = spark.sql(s"select $labelWhere, $genderWhere,age,yearsmarried," +
      s"$childrenWhere,religiousness,education,occupation,rating from data")

    val featuresArray = Array("gender", "age", "yearsmarried", "children", "religiousness", "education", "occupation", "rating")

    //字段转换成特征向量
    val assembler = new VectorAssembler().setInputCols(featuresArray).setOutputCol("features")
    val vecDF:DataFrame = assembler.transform(dataLabelDF)

    vecDF.show(10, truncate = false)

    //将数据分为训练和测试集(30%)
    val Array(trainingDF, testDF) = vecDF.randomSplit(Array(0.7, 0.3))

    //自动识别分类的特征，并对它们进行索引
    //具有大于5个不同的值的特征被视为连续
    val featureIndexer = new VectorIndexer().setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(5)

    //训练GBT模型
    val gbt = new GBTRegressor()
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures")
      .setImpurity("variance")
      .setLossType("squared")
      .setMaxIter(2)
      .setMinInstancesPerNode(1)

    //Chain indexer and GBT in a Pipeline
    val pipeline = new Pipeline().setStages(Array(featureIndexer, gbt))

    //Train model, this alse runs the indexer
    val model = pipeline.fit(trainingDF)

    //做出预测
    val predictions = model.transform(testDF)

    //预测样本展示
    predictions.select("prediction", "label", "features")
      .show(20, false)

    //选择（预测标签，实际标签）, 并计算测试
    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    val rmse = evaluator.evaluate(predictions)
    println("Root meam squared error (RMSE) on test data = " + rmse)
    val gbtModel = model.stages(1).asInstanceOf[GBTRegressionModel]
    println("Learned regression GBT model:\n"+ gbtModel.toDebugString)


    spark.stop()

  }

}
