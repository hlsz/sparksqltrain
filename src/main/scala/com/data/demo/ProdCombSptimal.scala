package com.data.demo

import java.util

import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.linalg.{SparseVector => OldSparseVector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.{BoostingStrategy, FeatureType}
import org.apache.spark.mllib.tree.model.{GradientBoostedTreesModel, Node => OldNode}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object prod_comb_optimal {
  val spark = SparkSession.builder()
    .appName("prod_comb_optimal")
    .master("local")
    .enableHiveSupport()
    .getOrCreate()

  val sc = spark.sparkContext

  def dataProcess() {
    val path = "/Users/anna/IdeaProjects/sparksqltrain/src/resources/mldata/"
    val manualSchema = StructType(
      Array(
        StructField("age", IntegerType, true),
        StructField("workclass", StringType, true),
        StructField("fnlwgt", IntegerType, true),
        StructField("education", StringType, true),
        StructField("education-num", IntegerType, true),
        StructField("marital-status", StringType, true),
        StructField("occupation", StringType, true),
        StructField("relationship", StringType, true),
        StructField("race", StringType, true),
        StructField("sex", StringType, true),
        StructField("capital-gain", IntegerType, true),
        StructField("capital-loss", IntegerType, true),
        StructField("hours-per-week", IntegerType, true),
        StructField("native-country", StringType, true),
        StructField("label", StringType, true)
      )    )

    val df = spark.read
      .option("header", false)
      .option("delimiter", ",")
      .option("nullValue", "?")
      .schema(manualSchema)
      .format("csv")
      .load(path+"adult.data")

    // 去掉代表序列号的col
    var df1 = df.drop("fnlwgt")
      .na.drop()

    val allFeature = df1.columns.dropRight(1)
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val colIdx = new util.HashMap[String, Int](allFeature.length)

    var idx = 0
    while (idx < allFeature.length){
      colIdx.put(allFeature(idx), idx)
      idx += 1
    }

    val numCols = Array("age", "education-num", "capital-gain", "capital-loss", "hours-per-week")
    val catCols = df1.columns.dropRight(1).diff(numCols)
    val numLen = numCols.length
    val catLen = catCols.length

    def labeludf(elem: String):Int = {
      if(elem == "<=50K") 0
      else 1
    }

    val labelIndexer = udf(labeludf(_:String):Int)

    //也可以使用when 函数
    // val labelIndexer = when($"lable" ==="<=50K",0).otherwise(1)

    df1 = df1.withColumn("indexed_label", labelIndexer(col("label"))).drop("label")

    val inderMap: util.HashMap[String, util.HashMap[String, Int]] = new util.HashMap(catCols.length)
    var i = numCols.length
    for (column <- catCols) {
      val uniqueElem = df1.select(column)
        .groupBy(column)
        .agg(count(column))
        .select(column)
        .map(_.getAs[String](0))
        .collect()

      val len = uniqueElem.length
      var index = 0
      val freqMap = new util.HashMap[String, Int](len)

      while(index < len){
        freqMap.put(uniqueElem(index), i)
        index += 1
        i += 1
      }
      inderMap.put(column, freqMap)
    }

    val bcMap = spark.sparkContext.broadcast(inderMap)

    val d = 1

    // 合并为LabeledPoint
    val df2 = df1.rdd.map {
      row =>
        val indics = new Array[Int](numLen + catLen)
        val value = new Array[Double](numLen + catLen)
        var i = 0
        for (col <- numCols) {
          indics(i) = i
          value(i) = row.getAs[Int](colIdx.get(col)).toDouble
          i += 1
        }

        for (col <- catCols) {
          indics(i) = bcMap.value.get(col).get(row.getAs[String](colIdx.get(col)))
          value(i) = 1
          i += 1
        }
        new LabeledPoint(row.getAs[Int](numLen + catLen), new OldSparseVector(d, indics, value))
    }

    val ds = df2.toDF("label", "feature")
    ds.write.format("csv").save(path + "processed")

    val df3 = spark.read
      .load(path+"processed")
      .rdd
      .map(row => LabeledPoint(row.getAs[Double](0), row.getAs[OldSparseVector](1)))


    val boostingStrategy = BoostingStrategy.defaultParams("classification")
    boostingStrategy.numIterations = 10
    boostingStrategy.treeStrategy.numClasses = 2
    boostingStrategy.treeStrategy.maxDepth = 3
    boostingStrategy.learningRate = 0.3
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

    val model = GradientBoostedTrees.train(df3, boostingStrategy)

    model.save(spark.sparkContext, path+ "GBDTmodel")

  }

  def main(args: Array[String]): Unit = {
    dataProcess()
  }

}

object GBTLRTraining {

  def predictModify(node:OldNode, features: OldSparseVector) :Int ={
    val split = node.split
    if (node.isLeaf) {
      node.id - 1
    } else {
      if (split.get.featureType == FeatureType.Continuous) {
        if (features(split.get.feature) <= split.get.threshold){
          predictModify(node.leftNode.get, features)
        } else {
          predictModify(node.rightNode.get, features)
        }
      } else {
        if (split.get.categories.contains(features(split.get.feature))) {
          predictModify(node.leftNode.get, features)
        } else {
          predictModify(node.rightNode.get, features)
        }
      }
    }
  }

  // 获取每颗树的出口叶子节点Id数组
  def getGBTFeatures(gbtModel: GradientBoostedTreesModel, oldFeatures:OldSparseVector): Array[Int] ={
    val GBTMaxIter = gbtModel.trees.length
    val leafIdArray = new Array[Int](GBTMaxIter)
    for (i <- 0 until GBTMaxIter) {
      val treePredict = predictModify(gbtModel.trees(i).topNode, oldFeatures)
      leafIdArray(i) = treePredict
    }
    leafIdArray
  }

  def mlocal(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("test")
      // 本地配置
//      .config("spark.sql.shuffle.partitions", 12)
//      .config("spark.default.parallelism", 12)
//      .config("spark.memory.fraction", 0.75)
    //      .config("spark.memory.ofHeap.enabled", true)
    //      .config("spark.memory.ofHeapa.size", "2G")
//      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //      .config("spark.executor.memory", "2G")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import org.apache.spark.sql.functions._

    val path = "/Users/anna/IdeaProjects/sparksqltrain/src/resources/mldata/"
    val df = spark.read.format("csv").load(path)

    val model = GradientBoostedTreesModel.load(spark.sparkContext, path+"GBDTmodel")

    val bcmodel = spark.sparkContext.broadcast(model)

    var treeNodeNum = 0
    var treeDepth = 0
    //获取最大的树的数据
    for(elem <- model.trees) {
      if (treeNodeNum < elem.numNodes) {
        treeNodeNum = elem.numNodes
        treeDepth = elem.depth
      }
    }

    val leafNum = math.pow(2, treeDepth).toInt
    val nonLeafNum = treeNodeNum - leafNum
    val totalColNum = leafNum * model.trees.length

    // 利用之前训练好的GBT模型进行特征提取，并把原特征OldSparseVector转化为ml的SparseVector，让后续的LR使用
    val addFeatureUDF = udf { features: OldSparseVector =>
      val gbtFeatures = getGBTFeatures(bcmodel.value, features)
      var i = 0
      while (i< gbtFeatures.length){
        val leafIdx = gbtFeatures(i) - nonLeafNum
        // 有些树可能没有生长完全，leafIdx没有达到最大的树的最后一层，这里将这些情况默认为最大的树的最后一层的第一个叶子节点。
        gbtFeatures(i) = ( if (leafIdx < 0 ) 0 else leafIdx) + i * leafNum
        i += 1
      }
      val idx = gbtFeatures
      val values = Array.fill[Double](idx.length)(1.0)
      Vectors.sparse(totalColNum, idx, values)
    }

    val dsWithCombinedFeatures = df
      .withColumn("lr_features", addFeatureUDF(col("feature")))
//    dsWithCombinedFeatures.show(false)

    val lr = new LogisticRegression()
      .setMaxIter(500)
      .setFeaturesCol("lr_feature")
      .setLabelCol("label")

    val lrmodel = lr.fit(dsWithCombinedFeatures)

    val res = lrmodel.transform(dsWithCombinedFeatures)

    res.show(false)

    val evaluator1 = new MulticlassClassificationEvaluator().setMetricName("accuracy")
      .setLabelCol("label")
      .setPredictionCol("prediction")
    println("ACC:"+evaluator1.evaluate(res))

    val evaluator2 = new BinaryClassificationEvaluator().setMetricName("areaUnderROC")
      .setLabelCol("label")
      .setRawPredictionCol("prediction")
    println("AUC:"+evaluator2.evaluate(res))

  }

}