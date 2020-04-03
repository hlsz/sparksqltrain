package com.data.spark.Bean;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

/**
 * Data Skew Solution
 *
 */
public class DataSkew {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("DataSkew");

        JavaSparkContext sc = new JavaSparkContext(conf);

        // create initial RDD
        JavaRDD<String> initRDD = sc.textFile(args[0]);

        // transform initRDD into pairRDD
        JavaPairRDD<String, Integer> pairRDD = initRDD.mapToPair(
                new PairFunction<String, String, Integer>() {

                    private static final long serialVersionUID = 2479906636617428526L;

                    @Override
                    public Tuple2<String, Integer> call(String line) throws Exception {

                        String[] arr = line.split(",");
                        String key = arr[0];
                        Integer value = Integer.valueOf(arr[1]);

                        return new Tuple2<String, Integer>(key, value);
                    }
                });

        // add random prefix from pairRDD
        JavaPairRDD<String, Integer> prePairRDD = pairRDD.mapToPair(
                new PairFunction<Tuple2<String,Integer>, String, Integer>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple) throws Exception {

                        Random random = new Random();
                        int prefix = random.nextInt(10);

                        String key = prefix+"_"+tuple._1;

                        return new Tuple2<String, Integer>(key, tuple._2);
                    }
                });

        // reduceByKey
        JavaPairRDD<String, Integer> tempPrePairRDD = prePairRDD.reduceByKey(
                new Function2<Integer, Integer, Integer>() {

                    private static final long serialVersionUID = 2021476568518204795L;

                    @Override
                    public Integer call(Integer value1, Integer value2) throws Exception {

                        return value1 + value2;
                    }
                });

        // split Key
        JavaPairRDD<String, Integer> initPairRDD = tempPrePairRDD.mapToPair(
                new PairFunction<Tuple2<String,Integer>, String, Integer>() {

                    private static final long serialVersionUID = -178978937197684290L;

                    @Override
                    public Tuple2<String, Integer> call(Tuple2<String, Integer> tuple) throws Exception {

                        String key = tuple._1.split("_")[1];

                        return new Tuple2<String, Integer>(key, tuple._2);
                    }
                });

        // reduceByKey
        JavaPairRDD<String, Integer> resultPairRDD = initPairRDD.reduceByKey(
                new Function2<Integer, Integer, Integer>() {

                    private static final long serialVersionUID = -815845668882788529L;

                    @Override
                    public Integer call(Integer value1, Integer value2) throws Exception {

                        return value1 + value2;
                    }
                });

        saveToMysql(resultPairRDD, args[1]);

        sc.close();

    }

    /**
     * save resultRDD to mysql
     *
     * @param resultPairRDD
     */
    public static void saveToMysql(JavaPairRDD<String, Integer> resultPairRDD, String tableName) {

        // create SparkSession object
        SparkSession spark = SparkSession.builder().getOrCreate();

        // create RowRDD
        JavaRDD<Row> rowRDD = resultPairRDD.map(
                new Function<Tuple2<String,Integer>, Row>() {

                    private static final long serialVersionUID = 7659308133806959864L;

                    @Override
                    public Row call(Tuple2<String, Integer> tuple) throws Exception {

                        return RowFactory.create(tuple._1, tuple._2);
                    }
                });

        // create Schema
        List<StructField> fields = new ArrayList<StructField>();
        StructField field = null;
        field = DataTypes.createStructField("key", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("value", DataTypes.IntegerType, true);
        fields.add(field);
        StructType schema = DataTypes.createStructType(fields);

        // create DataFrame
        Dataset<Row> resultDF = spark.createDataFrame(rowRDD, schema);

        // save to mysql
        Properties properties = new Properties();
        properties.put("driver", "com.mysql.jdbc.Driver");
        properties.put("user", "root");
        properties.put("password", "hadoop");

        resultDF.write().mode("overwrite").jdbc("jdbc:mysql://localhost:3306", tableName, properties);
//--driver-class-path /root/workspace/java/mysql-connector-java.jar \
//--jars /root/workspace/java/mysql-connector-java.jar
    }

}
