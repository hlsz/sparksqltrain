package com.data.spark.ml;

import com.alibaba.fastjson.JSON;
import com.data.model.*;
import com.data.utils.DateUtils;
import com.data.utils.TfidfUtil;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.ml.linalg.BLAS;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.spark_project.jetty.util.StringUtil;
import scala.reflect.ClassTag$;

import java.util.List;

public class CategorySuggestion {
    private static SparkSession spark;

    private static final String YESTERDAY = DateUtils.getYesterday();

    private static boolean CALCULATE_ALL = false;

    private static long MT_SHOP_COUNT = 2000;

    public static final String TRAINNING_DATA_SQL = "select shop_id, coalesce(shop_name,'') as shop_name,coalesce(category,0) as category_id, " +
            "vector_size, coalesce(vector_indices,'[]') as vector_indices, coalesce(vector_values,'[]') as vector_values " +
            "from dw.poi_category_pre_data limit %s ";

    public static final String COMPETITOR_DATA_SQL = "select id,coalesce(name,'') as name,coalesce(food,'') as food from dw.unknow_category_restaurant " +
            "where dt='%s' and id is not null limit %s ";

    public static void main(String[] args){
        spark = initSaprk();
        try{
            MiniTrainningData[] miniTrainningDataArray = getTrainData();
            final Broadcast<MiniTrainningData[]> trainningData = spark.sparkContext().broadcast(miniTrainningDataArray, ClassTag$.MODULE$.<MiniTrainningData[]>apply(MiniTrainningData[].class));
            System.out.println("broadcast success and list is "+trainningData.value().length);

            Dataset<Row> rawMeituanDataset = getMeituanDataSet();
            Dataset<Row> meituanTfidDataset = TfidfUtil.tfidf(rawMeituanDataset);
            Dataset<SimilartyData> similartyDataList = pickupTheTopSimilarShop(meituanTfidDataset, trainningData);

            saveToHive(similartyDataList);
            System.out.println("poi suggest stopped");
            spark.stop();
        } catch (Exception e) {
            System.out.println("main method has error " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static SparkSession initSaprk(){
        long startTime = System.currentTimeMillis();
        return SparkSession
                .builder()
                .appName("poi-spark")
                .enableHiveSupport()
                .getOrCreate();
    }

    /**
     * to get the origin ele shop data including category and goods which is separated by '|'
     * and then divide the goods into words
     * @return Dataset<Row>
     */
    private static MiniTrainningData[] getTrainData(){
        String trainningSql = String.format(TRAINNING_DATA_SQL,20001);
        System.out.println("tranningData sql is "+trainningSql);
        spark.sql("use dw");
        Dataset<Row> rowRdd = spark.sql(trainningSql);
        List<MiniTrainningData> trainningDataList = rowRdd.javaRDD().map((row) -> {
            MiniTrainningData data = new MiniTrainningData();
            data.setEleShopId( row.getAs("shop_id"));
            data.setCategory( row.getAs("category_id"));
            Long vectorSize = row.getAs("vector_size");
            List<Integer> vectorIndices = JSON.parseArray(row.getAs("vector_indices"),Integer.class);
            List<Double> vectorValues = JSON.parseArray(row.getAs("vector_values"),Double.class);
            SparseVector vector = new SparseVector(vectorSize.intValue(),integerListToArray(vectorIndices),doubleListToArray(vectorValues));
            data.setFeatures(vector);
            return data;
        }).collect();
        MiniTrainningData[] miniTrainningDataArray = new MiniTrainningData[trainningDataList.size()];
        return trainningDataList.toArray(miniTrainningDataArray);
    }

    private static int[] integerListToArray(List<Integer> integerList){
        int[] intArray = new int[integerList.size()];
        for (int i = 0; i < integerList.size(); i++) {
            intArray[i] = integerList.get(i).intValue();
        }
        return intArray;
    }

    private static double[] doubleListToArray(List<Double> doubleList){
        double[] doubleArray = new double[doubleList.size()];
        for (int i = 0; i < doubleList.size(); i++) {
            doubleArray[i] = doubleList.get(i).intValue();
        }
        return doubleArray;
    }

    private static Dataset<Row> getMeituanDataSet() {
        String meituanSql = String.format(COMPETITOR_DATA_SQL, YESTERDAY, 10000);
        System.out.println("meituan sql is " + meituanSql);
        spark.sql("use dw");
        Dataset<Row> rowRdd = spark.sql(meituanSql);
        JavaRDD<MeiTuanData> meituanDataJavaRDD = rowRdd.javaRDD().map((row) -> {
            MeiTuanData data = new MeiTuanData();
            String goods = (String) row.getAs("food");
            String shopName = (String) row.getAs("name");
            data.setShopId((Long) row.getAs("id"));
            data.setShopName(shopName);
            if (StringUtil.isBlank(goods)) {
                return null;
            }
            StringBuilder wordsOfGoods = new StringBuilder();
            try {
                List<Word> words = WordSegmenter.seg(goods.replace("|", " "));
                for (Word word : words) {
                    wordsOfGoods.append(word.getText()).append(" ");
                }
            } catch (Exception e) {
                System.out.println("exception in segment " + data);
            }
            data.setGoodsSegment(wordsOfGoods.toString());
            return data;
        }).filter((data) -> data != null);
        System.out.println("meituan data count is " + meituanDataJavaRDD.count());
        return spark.createDataFrame(meituanDataJavaRDD, MeiTuanData.class);
    }

    private static Dataset<SimilartyData> pickupTheTopSimilarShop(Dataset<Row> meituanTfidDataset, Broadcast<MiniTrainningData[]> trainningData){
        return meituanTfidDataset.map(new MapFunction<Row, SimilartyData>() {
            @Override
            public SimilartyData call(Row row) throws Exception {
                SimilartyData similartyData = new SimilartyData();
                Long mtShopId = row.getAs("shopId");
                Vector meituanfeatures = row.getAs("features");
                similartyData.setMtShopId(mtShopId);
                MiniTrainningData[] trainDataArray = trainningData.value();
                if(ArrayUtils.isEmpty(trainDataArray)){
                    return similartyData;
                }
                double maxSimilarty = 0;
                long maxSimilarCategory = 0L;
                long maxSimilareleShopId = 0;
                for (MiniTrainningData trainData : trainDataArray) {
                    Vector trainningFeatures = trainData.getFeatures();
                    long categoryId = trainData.getCategory();
                    long eleShopId = trainData.getEleShopId();
                    double dot = BLAS.dot(meituanfeatures.toSparse(), trainningFeatures.toSparse());
                    double v1 = Vectors.norm(meituanfeatures.toSparse(), 2.0);
                    double v2 = Vectors.norm(trainningFeatures.toSparse(), 2.0);
                    double similarty = dot / (v1 * v2);
                    if(similarty>maxSimilarty){
                        maxSimilarty = similarty;
                        maxSimilarCategory = categoryId;
                        maxSimilareleShopId = eleShopId;
                    }
                }
                similartyData.setEleShopId(maxSimilareleShopId);
                similartyData.setSimilarty(maxSimilarty);
                similartyData.setCategoryId(maxSimilarCategory);
                return similartyData;
            }
        }, Encoders.bean(SimilartyData.class));
    }

    private static void saveToHive(Dataset<SimilartyData> similartyDataset){
        try {
            similartyDataset.createTempView("records");
            String sqlInsert = "insert overwrite table dw.poi_category_suggest  PARTITION (dt = '"+DateUtils.getYesterday()+"') \n" +
                    "select mtShopId,eleShopId,shopName,similarty,categoryId from records ";
            System.out.println(spark.sql(sqlInsert).count());
        } catch (Exception e) {
            System.out.println("create SimilartyData dataFrame failed");
            e.printStackTrace();
        }
        //Dataset<Row> resultSet = spark.createDataFrame(similartyDataset,SimilartyData.class);
        spark.sql("use platform_dw");
    }
}
