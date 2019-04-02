package scala.com.data.spark.ml

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}

object KmeansTest {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val data = sc.textFile(args(0))
    val parseData = data.map(s => Vectors.dense(s.split(' ').map(_.trim.toDouble))).cache()

    val numClusters = 3
    val numIterations = 20
    val runs = 10
    val clusters = KMeans.train(parseData, numClusters, numIterations, runs)
    val WSSSE = clusters.computeCost(parseData)
    println("WithinSet Sum of Squared Errors = " + WSSSE)

    val a21 = clusters.predict(Vectors.dense(57.0,30.0))
    val a22 = clusters.predict(Vectors.dense(0.0, 0.0))

    println("ClusterCenters:")
    for(center <- clusters.clusterCenters) {
      println(" "+ center)
    }

    println(parseData.map(v => v.toString() + "belong to cluster: "+ clusters.predict(v)).collect().mkString("\n"))
    println("预测第21个用户的归类为-->"+a21)
    println("预测第22个用户的归类为-->"+a22)



  }

}
