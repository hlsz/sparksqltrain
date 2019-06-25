package com.data.service;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


public class WordCount {
    public static void main(String[] args) {
        System.out.println(args[0]);
        System.out.println(args[1]);
        System.out.println(args[2]);
        //1.本地模式，创建spark配置及上下文
        //	SparkConf conf = new SparkConf().setAppName("wordCount").setMaster("spark://192.168.174.132:7077");
        SparkConf conf = new SparkConf().setAppName("wordCount").setMaster(args[0]);

        JavaSparkContext sc = new JavaSparkContext(conf);

        //2.读取本地文件,并创建RDD
        //	JavaRDD<String> linesRDD = sc.textFile("e:\\words.txt");
        //	JavaRDD<String> linesRDD = sc.textFile("hdfs://192.168.174.132:9000/wc_input");
        JavaRDD<String> linesRDD = sc.textFile(args[1]);
        //3.每个单词由空格隔开,将每行的linesRDD拆分为每个单词的RDD
        //	JavaRDD<String> wordsRDD = linesRDD.flatMap(s  -> Arrays.asList(s.split("\\s")));
        //相当于 ==>
        JavaRDD<String> wordsRDD = linesRDD.flatMap(new FlatMapFunction<String, String>(){
            private static final long serialVersionUID = 1L;
            @SuppressWarnings("unchecked")
            @Override
            public Iterator<String> call(String line) throws Exception {
                return Arrays.asList(line.split(" ")).listIterator();
            }
        });
        //4.将每个单词转为key-value的RDD，并给每个单词计数为1
        //JavaPairRDD<String,Integer> wordsPairRDD = wordsRDD.mapToPair(s -> new Tuple2<String,Integer>(s, 1));
        //相当于 ==>
        JavaPairRDD<String,Integer> wordsPairRDD = wordsRDD.mapToPair(new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String,Integer>(word,1);
            }
        });

        //5.计算每个单词出现的次数
        //JavaPairRDD<String,Integer> wordsCountRDD = wordsPairRDD.reduceByKey((a,b) -> a+b);
        //相当于 ==>
        JavaPairRDD<String,Integer> wordsCountRDD = wordsPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        //6.因为只能对key进行排序，所以需要将wordsCountRDD进行key-value倒置，返回新的RDD
        // JavaPairRDD<Integer,String> wordsCountRDD2 = wordsCountRDD.mapToPair(s -> new Tuple2<Integer,String>(s._2, s._1));
        //相当于 ==>
        JavaPairRDD<Integer,String> wordsCountRDD2 = wordsCountRDD.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {
            private static final long serialVersionUID = 1L;
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                return new Tuple2<Integer,String>(t._2,t._1);
            }
        });

        //7.对wordsCountRDD2进行排序,降序desc
        JavaPairRDD<Integer,String> wordsCountRDD3 = wordsCountRDD2.sortByKey(false);

        //8.只取前10个
        List<Tuple2<Integer, String>>  result = wordsCountRDD3.take(10);

        //9.打印
        //	 result.forEach(t -> System.out.println(t._2 + "   " + t._1));
             for(Tuple2<Integer, String> t : result){
            System.out.println(t._2 + "   " + t._1);
        }

        //10.将结果保存到文件中
             wordsCountRDD3.saveAsTextFile(args[2]);
             sc.close();
        }
}
