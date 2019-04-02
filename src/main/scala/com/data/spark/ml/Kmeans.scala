import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors


object Kmeans {

  def main(args: Array[String]): Unit = {


    //本地运行不报hadoop错误
    System.setProperty("hadoop.home.dir", "E:\\data\\winutils")


    val conf = new SparkConf().setAppName("kmeans").setMaster("local")
    val sc = new SparkContext(conf)


    //rfm
    var input = "E:\\数据处理分析\\用户RFM模型\\kmean_input_20171114_request.csv";
    var output = "E:\\数据处理分析\\用户RFM模型\\kmean_output_20171114_request.csv"

    //emui探索分类
    input = "E:\\数据处理分析\\emui兴趣探索\\kmeans_input.csv"
    output = "E:\\数据处理分析\\emui兴趣探索\\kmeans_output.csv"

    val data = sc.textFile(input)

    //val parsedData = data.map(s => Vectors.dense(s.split(',').map(_.trim.toDouble)))
    val parsedData = data.map(s => Vectors.dense(s.split(',').map(x => (x.replaceAll("nan", "0"))).map(_.trim.toDouble)))

    //如何选择K
    /* val ks = Array(10,11,12,13,14,15,16,17,18,19,
                    20,21,22,23,24,25,26,27,28,29,
                    30,31,32,33,34,35,36,37,38,39,
                    40,41,42,43,44,45,46,47,48,49,
                    50,51,52,53,54,55,56,57,58,59);
     ks.foreach(k=>{
       val model = KMeans.train(parsedData, k, 20)
       val sse = model.computeCost(parsedData)
       println("sum of squared distances of points to their nearest center when k=" + k + " -> "+ sse)
     })*/

    //设置簇的个数为3
    val numClusters = 50
    //迭代20次
    val numIterations = 20

    //设置初始K选取方式为k-means++
    val initMode = "k-means||"
    val clusters = new KMeans().
      setInitializationMode(initMode).
      setK(numClusters).
      setMaxIterations(numIterations).
      run(parsedData)




    //打印出测试数据属于哪个簇
    //println(parsedData.map(v => v.toString() + " belong to cluster :" + clusters.predict(v)).collect().mkString("\n"))
    val writer = new PrintWriter(new File(output))
    writer.println(parsedData.map(v => v.toString() + "," + clusters.predict(v)).collect().mkString("\n"))
    writer.close()


    // Evaluateclustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("WithinSet Sum of Squared Errors = " + WSSSE)


    //打印出中心点
    println("Clustercenters:")
    for (center <- clusters.clusterCenters) {
      println(" " + center)
    }

  }

}