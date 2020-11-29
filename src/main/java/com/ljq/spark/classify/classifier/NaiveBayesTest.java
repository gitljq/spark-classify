package com.ljq.spark.classify.classifier;

import com.alibaba.fastjson.JSON;
import com.ljq.spark.classify.dto.Result;
import com.ljq.spark.classify.utils.AnsjUtils;
import com.ljq.spark.classify.utils.FileUtils;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IDFModel;
import org.apache.spark.ml.linalg.SparseVector;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.*;

import java.io.File;
import java.text.DecimalFormat;
import java.util.*;

/**
 * 3
 */
public class NaiveBayesTest {

    private static HashingTF hashingTF;

    private static IDFModel idfModel;

    private static NaiveBayesModel model;

    private static Map<Integer,String> labels = new HashMap<>();

    static {
        boolean error = false;
        if (!new File(DataFactory.DATA_TEST_PATH).exists()) {
            new Exception(DataFactory.DATA_TEST_PATH + " is not exists").printStackTrace();
            error = true;
        }
        if (!new File(DataFactory.MODEL_PATH).exists()) {
            new Exception(DataFactory.MODEL_PATH + " is not exists").printStackTrace();
            error = true;
        }
        if (!new File(DataFactory.TF_PATH).exists()) {
            new Exception(DataFactory.TF_PATH + " is not exists").printStackTrace();
            error = true;
        }
        if (!new File(DataFactory.IDF_PATH).exists()) {
            new Exception(DataFactory.IDF_PATH + " is not exists").printStackTrace();
            error = true;
        }
        if (!new File(DataFactory.LABEL_PATH).exists()) {
            new Exception(DataFactory.LABEL_PATH + " is not exists").printStackTrace();
            error = true;
        }
        if (error) {
            System.exit(0);
        }

        String labelsData = FileUtils.readFile(DataFactory.LABEL_PATH);

        labels = JSON.parseObject(labelsData,Map.class);
    }

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().appName("NaiveBayes").master("local")
                .getOrCreate();

        //load tf
        hashingTF = HashingTF.load(DataFactory.TF_PATH);
        //load idf
        idfModel = IDFModel.load(DataFactory.IDF_PATH);
        //加载模型
        model = NaiveBayesModel.load(spark.sparkContext(), DataFactory.MODEL_PATH);

        //批量
        batchTestModel(spark, DataFactory.DATA_TEST_PATH);

        //单个
        testModel(spark,"伊拉克开始进攻美国");

    }

    public static void batchTestModel(SparkSession sparkSession, String testPath) {

        Dataset<Row> test = sparkSession.read().json(testPath);
        //word frequency count
        Dataset<Row> featurizedData = hashingTF.transform(test);
        //count tf-idf
        Dataset<Row> rescaledData = idfModel.transform(featurizedData);

        List<Row> rowList = rescaledData.select("category", "features").javaRDD().collect();

        List<Result> dataResults = new ArrayList<>();
        for (Row row : rowList) {
            Long category = row.getAs("category");
            SparseVector sparseVector = row.getAs("features");
            Vector features = Vectors.dense(sparseVector.toArray());
            double predict = model.predict(features);
            dataResults.add(new Result(category, (long) predict));
        }

        Integer successNum = 0;
        Integer errorNum = 0;

        for (Result result : dataResults) {
            if (result.isCorrect()) {
                successNum++;
            } else {
                errorNum++;
            }
        }

        DecimalFormat df = new DecimalFormat("######0.0000");
        Double result = (Double.valueOf(successNum) / Double.valueOf(dataResults.size())) * 100;

        System.out.println("批量测试");
        System.out.println("=======================================================");
        System.out.println(String.format("正确分类       :      %s\t   %s%%",successNum,df.format(result)));
        System.out.println(String.format("错误分类       :      %s\t    %s%%",errorNum,df.format(100-result)));
        System.out.println(String.format("测试总数       :      %s",dataResults.size()));
        System.out.println("=======================================================");

    }

    public static void testModel(SparkSession sparkSession, String content){
        List<Row> data = Arrays.asList(
                RowFactory.create(AnsjUtils.participle(content))
        );
        StructType schema = new StructType(new StructField[]{
                new StructField("text", new ArrayType(DataTypes.StringType, false), false, Metadata.empty())
        });

        Dataset<Row> testData = sparkSession.createDataFrame(data, schema);
        //word frequency count
        Dataset<Row> transform = hashingTF.transform(testData);
        //count tf-idf
        Dataset<Row> rescaledData = idfModel.transform(transform);

        Row row =rescaledData.select("features").first();
        SparseVector sparseVector = row.getAs("features");
        Vector features = Vectors.dense(sparseVector.toArray());
        Double predict = model.predict(features);

        System.out.println("single test");
        System.out.println("=======================================================");
        System.out.println(labels.get(predict.intValue()));
        System.out.println("===================================");
    }


}
