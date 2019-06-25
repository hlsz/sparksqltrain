package com.data.utils;

import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class TfidfUtil {
    /**
     * visit below website to get more detail about tfidf
     * @see  <a href="http://dblab.xmu.edu.cn/blog/1261-2/">Spark入门：特征抽取： TF-IDF</a>
     * @param dataset
     * @return
     */
    public static Dataset<Row> tfidf(Dataset<Row> dataset) {
        Tokenizer tokenizer = new Tokenizer().setInputCol("goodsSegment").setOutputCol("words");
        Dataset<Row> wordsData = tokenizer.transform(dataset);
        HashingTF hashingTF = new HashingTF()
                .setInputCol("words")
                .setOutputCol("rawFeatures");
        Dataset<Row> featurizedData = hashingTF.transform(wordsData);
        IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
        IDFModel idfModel = idf.fit(featurizedData);
        return idfModel.transform(featurizedData);
    }
}
