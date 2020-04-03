package com.data.spark.Handler;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import scala.Tuple2;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;

public class WordCount {

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("hdfsCountStream").setMaster("local[2]");
        JavaStreamingContext javaStreamingContext = new JavaStreamingContext(conf, Durations.seconds(10));


        JavaDStream<String> lines = javaStreamingContext.textFileStream("hdfs://user/hdfs/test");

        JavaDStream<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        JavaPairDStream<String, Integer> wordsNumber =
                words.mapToPair(new PairFunction<String, String, Integer>() {
                                    @Override
                                    public Tuple2<String, Integer> call(String s) throws Exception {
                                        return new Tuple2<>(s, 1);
                                    }
                                }
                );

        JavaPairDStream<String, Integer> result = wordsNumber.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        result.print();
        javaStreamingContext.start();
        javaStreamingContext.awaitTermination();

        javaStreamingContext.close();
        javaStreamingContext.stop(true, true);

    }
}
