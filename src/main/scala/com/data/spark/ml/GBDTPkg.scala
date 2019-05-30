package com.data.spark.ml

import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.feature.StandardScalerModel
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GBDTPkg {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    if(args.length < 4)
      {
        System.err.println("Usage: DecisionTrees <InputPath> <modelPath> <maxDepth> <master> <appName>")
        System.err.println("eg: /IdeaProjects/sparksqltrain/ml/train /IdeaProjects/sparksqltrain/ml/model 10 local DecisionTrees")
        System.exit(1)
      }

    val appName = if (args.length > 4) args(4) else "DecisionTrees"
    val conf = new SparkConf().setAppName(appName).setMaster(args(3))
    val sc = new SparkContext(conf)

    val traindata:RDD[LabeledPoint] = MLUtils.loadLabeledPoints(sc, args(0))
    val features = traindata.map(_.features)
    val scalar: StandardScalerModel = new StandardScaler(
      withMean = true,
      withStd = true).fit(features) // fit 计算训练数据的均值与方差

    val train:RDD[LabeledPoint] = traindata.map(sample => {
      val label = sample.label
      val feature = scalar.transform(sample.features) // 在fit后，进行转换训练数据为标准的正态分布
      new LabeledPoint(label, feature)
    })

    val splitRdd: Array[RDD[LabeledPoint]] = train.randomSplit(Array(0.1, 0.9))
    val testData: RDD[LabeledPoint] = splitRdd(0)
    val realTrainData: RDD[LabeledPoint] = splitRdd(1)

    val bootingStrategy: BoostingStrategy = BoostingStrategy.defaultParams("Classification")
    bootingStrategy.setNumIterations(3)
    bootingStrategy.treeStrategy.setNumClasses(2)
    bootingStrategy.treeStrategy.setMaxDepth(args(2).toInt)
    bootingStrategy.setLearningRate(0.8)

    val model = GradientBoostedTrees.train(realTrainData, bootingStrategy)

    val labelAndPreds = testData.map(point => {
      val prediction = model.predict(point.features)
      (point.label, prediction)
    })

    val acc = labelAndPreds.filter(r => r._1 == r._2).count().toDouble / testData.count()

    println("Test Error = " + acc)

    model.save(sc, args(1))

  }

}
