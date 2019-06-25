package com.data.spark.ml;

import com.data.model.TrainningData;
import com.data.model.TrainningFeature;
import com.data.model.Word;
import com.data.model.WordSegmenter;
import com.data.utils.DateUtils;
import com.data.utils.TfidfUtil;
import jodd.util.StringUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class CategorySuggestionTrainning {
    private static SparkSession spark;

    private static final String YESTERDAY = DateUtils.getYesterday();

    public static final String TRAINNING_DATA_SQL = "select id, coalesce(shop_name,'') as name,coalesce(category_id,0) as category_id, coalesce(food_name,'') as food " +
            "from dw.category_and_foodname where dt='%s' limit 100000";

    public static void main(String[] args){
        spark = initSaprk();
        try{
            Dataset<Row> rawTranningDataset = getTrainDataSet();
            Dataset<Row> trainningTfidfDataset = TfidfUtil.tfidf(rawTranningDataset);
            JavaRDD<TrainningFeature> trainningFeatureRdd = getTrainningFeatureRDD(trainningTfidfDataset);
            Dataset<Row> trainningFeaturedataset = spark.createDataFrame(trainningFeatureRdd,TrainningFeature.class);

            saveToHive(trainningFeaturedataset);
            System.out.println("poi suggest trainning stopped");
            spark.stop();
        } catch (Exception e) {
            System.out.println("main method has error " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * to get the origin ele shop data including category and goods which is separated by '|'
     * and then divide the goods into words
     * @return Dataset<Row>
     */
    private static Dataset<Row> getTrainDataSet(){
        String trainningSql = String.format(TRAINNING_DATA_SQL,YESTERDAY);
        System.out.println("tranningData sql is "+trainningSql);
        spark.sql("use dw");
        Dataset<Row> rowRdd = spark.sql(trainningSql);
        JavaRDD<TrainningData> trainningDataJavaRDD = rowRdd.javaRDD().map((row) -> {
            String goods = (String) row.getAs("food");
            String shopName = (String) row.getAs("name");
            if (StringUtil.isBlank(shopName) || StringUtil.isBlank(goods) || goods.length() < 50) {
                System.out.println("some field is null " + row.toString());
                return null;
            }
            TrainningData data = new TrainningData();
            data.setShopId((Long) row.getAs("id"));
            data.setShopName(shopName);
            data.setCategory((Long) row.getAs("category_id"));

            List<Word> words = WordSegmenter.seg(goods);
            StringBuilder wordsOfGoods = new StringBuilder();
            for (Word word : words) {
                wordsOfGoods.append(word.getText()).append(" ");
            }
            data.setGoodsSegment(wordsOfGoods.toString());
            return data;
        }).filter((data) -> data != null);
        return spark.createDataFrame(trainningDataJavaRDD, TrainningData.class);
    }

    private static JavaRDD<TrainningFeature> getTrainningFeatureRDD(Dataset<Row> trainningTfidfDataset){
        return trainningTfidfDataset.javaRDD().map(new Function<Row, TrainningFeature>(){
            @Override
            public TrainningFeature call(Row row) throws Exception {
                TrainningFeature data = new TrainningFeature();
                data.setCategory(row.getAs("category"));
                data.setShopId(row.getAs("shopId"));
                data.setShopName(row.getAs("shopName"));
                SparseVector vector = row.getAs("features");
                data.setVectorSize(vector.size());
                data.setVectorIndices(Arrays.toString(vector.indices()));
                data.setVectorValues(Arrays.toString(vector.values()));
                return data;
            }
        });
    }

    private static SparkSession initSaprk(){
        long startTime = System.currentTimeMillis();
        return SparkSession
                .builder()
                .appName("poi-spark-trainning")
                .enableHiveSupport()
                .getOrCreate();
    }

    private static void saveToHive(Dataset<Row> trainningTfidfDataset){
        try {
            trainningTfidfDataset.createTempView("trainData");
            String sqlInsert = "insert overwrite table dw.poi_category_pre_data " +
                    "select shopId,shopName,category,vectorSize,vectorIndices,vectorValues from trainData ";

            spark.sql("use dw");
            System.out.println(spark.sql(sqlInsert).count());
        } catch (Exception e) {
            System.out.println("save tranning data to hive failed");
            e.printStackTrace();
        }
    }
}
