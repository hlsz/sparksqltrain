package com.data.spark.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{PCA, StandardScaler, VectorAssembler}
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.sql.SparkSession


object modelPrediction {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir","/user/anna/hadoop")

    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)

    val spark = SparkSession.builder()
      .appName("LinearSVC")
      .master("local")
      .getOrCreate()
    val path = "/Users/anna/IdeaProjects/sparksqltrain/src/resources/mldata/sample_libsvm_data.txt"
    val data = spark.read.format("libsvm")
      .load(path)

    println(data.count())

    //归一化
    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithMean(true)
      .setWithStd(true)
      .fit(data)

    val scaleddata = scaler.transform(data)
      .select("label","scaledFeatures")
      .toDF("label","features")

    //创建PCA 模型， 生成transformer
    // 主成分分析 降维
    val pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcafeatures")
      .setK(5)
      .fit(scaleddata)

    //transform 数据，生成主成分特征
    val pcaResult = pca.transform(scaleddata)
      .select("label","pcaFeatures")
      .toDF("label","features")

    pcaResult.show(false)

    //
    val assembler = new VectorAssembler()
      .setInputCols(Array("label", "features"))
      .setOutputCol("assemble")

    val output = assembler.transform(pcaResult)

    val ass = output.select(output("assemble").cast("string"))

    ass.write.mode("overwrite").csv("output.csv")

    val Array(trainingData, testData) = pcaResult.randomSplit(Array(0.7, 0.3), seed = 20)

    val lsvc = new LinearSVC()
      .setMaxIter(10)
      .setRegParam(0.1)


    val pipeline = new Pipeline()
      .setStages(Array(scaler, pca, lsvc))

    val model = pipeline.fit(trainingData)

    val predictions = model.transform(testData)
      .select("prediction", "label")

    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val accuracy = evaluator.evaluate(predictions)
    println(s"Accuracy = ${accuracy}")

    spark.stop()

  }

}
