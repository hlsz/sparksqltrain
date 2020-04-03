package com.data.spark.Bean;

import java.util.ArrayList;
import java.util.List;

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
import scala.Tuple4;

public class AppAccessLog {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("AppAccessLog");

        JavaSparkContext sc = new JavaSparkContext(conf);
        SparkSession spark = SparkSession.builder().enableHiveSupport().getOrCreate();

        // create RDD
        JavaRDD<String> AppAccessLogRDD = sc.textFile("hdfs:///temp/data/access.log");

        // transform into PairRDD
        JavaPairRDD<String, AppAccessLogInfo> AppAccessLogPairRDD =
                mapToPairRDD(AppAccessLogRDD);

        // aggregate DeviceID
        JavaPairRDD<String, AppAccessLogInfo> AppAccessAggregatePairRDD =
                agggregateToPairRDD(AppAccessLogPairRDD);

        // transform into sortByKeyPairRDD
        JavaPairRDD<AppAccessLogSortInfo, String> AppAccessSortByKeyLogPairRDD =
                mapToSortByKeyPairRDD(AppAccessAggregatePairRDD);

        // transformation sortBykey
        JavaPairRDD<AppAccessLogSortInfo, String> resultRDD =
                AppAccessSortByKeyLogPairRDD.sortByKey(false);

        // get Top 10
        List<Tuple2<AppAccessLogSortInfo, String>> top10 = resultRDD.take(10);

        // print Top 10
        for(Tuple2<AppAccessLogSortInfo, String> t : top10) {
            System.out.println(t._2+" " + t._1.getUpTraffic() + " " + t._1.getDownTraffic() + " " + t._1.getTimpStamp());
        }

        // JDK 1.8
        // top10.forEach(t -> System.out.println(t._2+" " + t._1.getUpTraffic() + " " + t._1.getDownTraffic() + " " + t._1.getTimpStamp()));

        // create RowRDD
        JavaRDD<Row> rowRDD = mapToRowRDD(resultRDD);
        // create schema
        ArrayList<StructField> fields = getColumnName();
        StructType schema = DataTypes.createStructType(fields);
        // create DataFrame
        Dataset<Row> logDF = spark.createDataFrame(rowRDD, schema);

        // save to Hive
        logDF.write().mode("overwrite").saveAsTable("test.log");

        spark.close();
        sc.close();
    }

    //
    private static ArrayList<StructField> getColumnName() {
        ArrayList<StructField> fields = new ArrayList<StructField>();
        StructField field = null;
        field = DataTypes.createStructField("timeStame", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("DeviceID", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("upTraffic", DataTypes.StringType, true);
        fields.add(field);
        field = DataTypes.createStructField("downTraffic", DataTypes.StringType, true);
        fields.add(field);
        return fields;
    }

    // transform resultRDD into RowRDD
    private static JavaRDD<Row> mapToRowRDD(JavaPairRDD<AppAccessLogSortInfo, String> resultRDD) {

        JavaRDD<Tuple4<String, String, String, String>> tempRDD = resultRDD
                .map(new Function<Tuple2<AppAccessLogSortInfo, String>, Tuple4<String, String, String, String>>() {

                    private static final long serialVersionUID = 7952741378495112332L;

                    @Override
                    public Tuple4<String, String, String, String> call(Tuple2<AppAccessLogSortInfo, String> tuple)
                            throws Exception {

                        String DeviceID = tuple._2;
                        AppAccessLogSortInfo accessLogSortInfo = tuple._1;

                        return new Tuple4<String, String, String, String>(
                                String.valueOf(accessLogSortInfo.getTimpStamp()), DeviceID,
                                String.valueOf(accessLogSortInfo.getUpTraffic()),
                                String.valueOf(accessLogSortInfo.getDownTraffic()));
                    }
                });
        return tempRDD.map(new Function<Tuple4<String, String, String, String>, Row>() {

            private static final long serialVersionUID = -1227536252899303985L;

            @Override
            public Row call(Tuple4<String, String, String, String> tuple) throws Exception {

                return RowFactory.create(tuple._1(), tuple._2(), tuple._3(), tuple._4());
            }
        });

    }

    // aggregate DeviceID calculate total of upTraffic/downTraffic and select
    // minimum timeStamp
    private static JavaPairRDD<String, AppAccessLogInfo> agggregateToPairRDD(
            JavaPairRDD<String, AppAccessLogInfo> appAccessLogPairRDD) {

        // transformation reduceByKey
        return appAccessLogPairRDD.reduceByKey(new Function2<AppAccessLogInfo, AppAccessLogInfo, AppAccessLogInfo>() {

            private static final long serialVersionUID = -8552789221394152834L;

            @Override
            public AppAccessLogInfo call(AppAccessLogInfo v1, AppAccessLogInfo v2) throws Exception {

                Long timeStamp = v1.getTimeStamp() > v2.getTimeStamp() ? v2.getTimeStamp() : v1.getTimeStamp();
                Long upTraffic = v1.getUpTraffic() + v2.getUpTraffic();
                Long downTraffic = v1.getDownTraffic() + v2.getDownTraffic();

                AppAccessLogInfo accessLogInfo = new AppAccessLogInfo();
                accessLogInfo.setUpTraffic(upTraffic);
                accessLogInfo.setDownTraffic(downTraffic);
                accessLogInfo.setTimeStamp(timeStamp);

                return accessLogInfo;
            }
        });
    }

    // transform AppAccessLogPairRDD into AppAccessSortByKeyLogPairRDD
    private static JavaPairRDD<AppAccessLogSortInfo, String> mapToSortByKeyPairRDD(
            JavaPairRDD<String, AppAccessLogInfo> AppAccessAggregatePairRDD) {

        // transformation mapToPair
        return AppAccessAggregatePairRDD
                .mapToPair(new PairFunction<Tuple2<String, AppAccessLogInfo>, AppAccessLogSortInfo, String>() {

                    private static final long serialVersionUID = -4778843695438540948L;

                    @Override
                    public Tuple2<AppAccessLogSortInfo, String> call(Tuple2<String, AppAccessLogInfo> tuple)
                            throws Exception {

                        String DeviceID = tuple._1;
                        AppAccessLogInfo appAccessLogInfo = tuple._2;

                        AppAccessLogSortInfo accessLogSortInfo = new AppAccessLogSortInfo();
                        accessLogSortInfo.setTimpStamp(appAccessLogInfo.getTimeStamp());
                        accessLogSortInfo.setUpTraffic(appAccessLogInfo.getUpTraffic());
                        accessLogSortInfo.setDownTraffic(appAccessLogInfo.getDownTraffic());

                        return new Tuple2<AppAccessLogSortInfo, String>(accessLogSortInfo, DeviceID);
                    }
                });
    }

    // transform AppAccessLogRDD into AppAccessLogPairRDD
    private static JavaPairRDD<String, AppAccessLogInfo> mapToPairRDD(JavaRDD<String> AppAccessLogRDD) {

        // transformation mapToPair
        return AppAccessLogRDD.mapToPair(new PairFunction<String, String, AppAccessLogInfo>() {

            private static final long serialVersionUID = 5998646612001714125L;

            @Override
            public Tuple2<String, AppAccessLogInfo> call(String line) throws Exception {

                String[] lineSplitArray = line.split("\t");

                String DeviceID = lineSplitArray[1];
                Long timeStamp = Long.valueOf(lineSplitArray[0]);
                Long upTraffic = Long.valueOf(lineSplitArray[2]);
                Long downTraffic = Long.valueOf(lineSplitArray[3]);

                AppAccessLogInfo appAccessLogInfo = new AppAccessLogInfo();
                appAccessLogInfo.setTimeStamp(timeStamp);
                appAccessLogInfo.setUpTraffic(upTraffic);
                appAccessLogInfo.setDownTraffic(downTraffic);

                return new Tuple2<String, AppAccessLogInfo>(DeviceID, appAccessLogInfo);
            }
        });
    }
}

