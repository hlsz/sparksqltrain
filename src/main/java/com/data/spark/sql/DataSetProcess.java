package com.data.spark.sql;

import com.data.spark.Bean.Person;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.spark.sql.functions.col;

public class DataSetProcess {



    public static void main(String[] args) throws AnalysisException {

        SparkSession spark = SparkSession.builder()
                .appName("test")
                .getOrCreate();

        Dataset<Row> df = spark.read().json("example/src/main/main.json");
        df.show();

        df.printSchema();

        df.select("name").show();

        df.select(col("name"), col("age").plus(1)).show();

        df.filter(col("age").gt(21)).show();

        df.groupBy("age").count().show();

        df.createOrReplaceTempView("people");
        Dataset<Row> sqlDF = spark.sql("select * from people");
        sqlDF.show();

        df.createGlobalTempView("people");

        spark.sql("select * from global_temp.people").show();

        spark.newSession().sql("select * from global_temp.people").show();

        // Dataset
        // Dataset API和RDD类似，不过Dataset不使用Java序列化或者Kryo，
        // 而是使用专用的编码器（Encoder ）来序列化对象和跨网络传输通信

        Person person = new Person();
        person.setName("Anna");
        person.setAge(20);

        Encoder<Person> personEncoder = Encoders.bean(Person.class);
        Dataset<Person> javaBeanDS = spark.createDataset(
                Collections.singletonList(person),
                personEncoder
        );
        javaBeanDS.show();


        Encoder<Integer> integerEncoder= Encoders.INT();
        Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1,2,3,4), integerEncoder);

        Dataset<Integer> transformedDS = primitiveDS.map(
                new MapFunction<Integer, Integer>() {
                    @Override
                    public Integer call(Integer value) throws Exception {
                        return value + 1;
                    }
                },integerEncoder
        );
        transformedDS.collect();


        String path = "example/people.json";
        Dataset<Person> personDS = spark.read().json(path).as(personEncoder);
        personDS.show();

        // 利用反射推导schema
        JavaRDD<Person> personRDD = spark.read()
                .textFile("example/person.txt")
                .javaRDD()
                .map(new Function<String, Person>() {
                    @Override
                    public Person call(String line) throws Exception {
                        String[] parts = line.split(",");
                        Person p = new Person();
                        p.setName(parts[0]);
                        p.setAge(Integer.parseInt(parts[1].trim()));
                        return p;
                    }
                });

        Dataset<Row> peopleDF = spark.createDataFrame(personRDD, Person.class);

        peopleDF.createOrReplaceTempView("people");

        Dataset<Row> teenagersDF = spark.sql("select name from people where age between 13 and 19");

        Encoder<String> stringEncoder = Encoders.STRING();



    }
}
