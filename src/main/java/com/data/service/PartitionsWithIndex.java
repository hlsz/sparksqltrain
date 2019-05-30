package com.data.service;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class PartitionsWithIndex {
    public static void main(String[] args) {

        SparkConf conf  = new SparkConf()
                .setAppName("index")
                .setMaster("local");

        JavaSparkContext  sc = new JavaSparkContext(conf);
        List<String> studentNames= Arrays.asList("张三", "李四", "王二", "麻子");

        JavaRDD<String> studentNameRDD = sc.parallelize(studentNames, 2);

        JavaRDD<String> studentWithClassRDD = studentNameRDD.mapPartitionsWithIndex(
                new Function2<Integer, Iterator<String>, Iterator<String>>(){
                    @Override
                    public Iterator<String> call(Integer integer, Iterator<String> stringIterator) throws Exception {
                        List<String> studentWithClassList = new ArrayList<String>();

                        while(stringIterator.hasNext()){
                            String studentName = stringIterator.next();
                            String studentWithClass =  studentName +"_" +(integer + 1);
                            studentWithClassList.add(studentWithClass);
                        }
                        return studentWithClassList.iterator();
                    }
                },true);
        studentWithClassRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println("s= "+ s);
            }
        });

    }
}
