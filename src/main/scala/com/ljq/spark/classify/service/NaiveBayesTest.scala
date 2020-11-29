package com.ljq.spark.classify.service

import java.io.InputStream
import java.text.DecimalFormat
import java.util

import com.alibaba.fastjson.JSON
import com.ljq.spark.classify.dto.Result
import com.ljq.spark.classify.service.DataETL.HDFS_PATH
import com.ljq.spark.classify.utils.{AnsjUtils, FileUtil, HbaseUtil, HdfsUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.hive.metastore.hbase.HBaseUtils
import org.apache.spark.ml.feature.{HashingTF, IDFModel}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, RowFactory, SparkSession}

/**
 * @Description:
 * @Author: ljq_dmr
 * @Date: 2020/11/28 20:44
 */
object NaiveBayesTest {
  var hashingTF:HashingTF = null

  var idfModel :IDFModel= null

  var model :NaiveBayesModel= null

  var labels = new util.HashMap[Integer, String]

  val col:Array[String] = Array("id","text","category","predict")

  def init(fileSystem: FileSystem,hdfsUtil: HdfsUtil): Boolean = {
    if (!hdfsUtil.exists(fileSystem, DataETL.TF_PATH)) {
      return false
    }

    if (!hdfsUtil.exists(fileSystem,DataETL.IDF_PATH)) {
      return false
    }

    if (!hdfsUtil.exists(fileSystem,DataETL.MODEL_PATH)) {
      return false
    }

    if (!hdfsUtil.exists(fileSystem,DataETL.LABEL_PATH)) {
      return false
    }

    true
  }

  def batchTestModel(spark: SparkSession,hbaseUtil: HbaseUtil, path: String) = {
    val test: Dataset[Row] = spark.read.json(path)
    val featurizedData: Dataset[Row] = hashingTF.transform(test)
    val rescaledData: Dataset[Row] = idfModel.transform(featurizedData)

    val rowList: util.List[Row] = rescaledData.select("id","category", "features").javaRDD.collect

    val dataResults = new util.ArrayList[Result]
    import scala.collection.JavaConversions._
    for (row <- rowList) {
      val id: Long = row.getAs("id")
      val category: Long = row.getAs("category")
      val sparseVector: SparseVector = row.getAs("features")
      val features: Vector = Vectors.dense(sparseVector.toArray)
      val predict: Long = model.predict(features).toLong
      dataResults.add(new Result(category, predict))
      val textList: util.Map[String, String] = hbaseUtil.getRowData("news:classify", id+"", "f", Array("text"))
      hbaseUtil.putData("news:classify_test",id+"","f",col,Array(id+"",textList.get("text"),category+"",predict+""))
    }

    var successNum = 0
    var errorNum = 0

    import scala.collection.JavaConversions._
    for (result <- dataResults) {
      if (result.isCorrect) successNum += 1
      else errorNum += 1
    }

    println("批量测试")
    println("=======================================================")
    println("正确分类       :      "+successNum)
    println("错误分类       :      "+errorNum)
    println("测试总数       :      "+dataResults.size)
    println("=======================================================")
  }

  def testModel(spark: SparkSession, str: String): Unit = {
    val data: util.List[Row] = util.Arrays.asList(RowFactory.create(AnsjUtils.participle(str)))
    val schema = new StructType(Array[StructField](new StructField("text", new ArrayType(DataTypes.StringType, false), false, Metadata.empty)))

    val testData: Dataset[Row] = spark.createDataFrame(data, schema)
    val transform: Dataset[Row] = hashingTF.transform(testData)
    val rescaledData: Dataset[Row] = idfModel.transform(transform)

    val row: Row = rescaledData.select("features").first
    val sparseVector: SparseVector = row.getAs("features")
    val features: Vector = Vectors.dense(sparseVector.toArray)
    val predict: Double = model.predict(features)

    System.out.println("single test")
    System.out.println("=======================================================")
    System.out.println(labels.get(predict.intValue))
    System.out.println("===================================")
  }

  def main(args: Array[String]): Unit = {
    val configuration: Configuration = new Configuration()
    configuration.set("fs.defaultFS", HDFS_PATH)
    configuration.setBoolean("dfs.support.append", true)
    val fileSystem: FileSystem = FileSystem.get(configuration)
    val hdfsUtil = new HdfsUtil

    val res: Boolean = init(fileSystem, hdfsUtil)

    if (!res) {
      System.exit(-1)
    }

    val labelInputStream: InputStream = hdfsUtil.getFile(fileSystem, DataETL.LABEL_PATH)
    val fileUtil = new FileUtil
    val jsonStr = fileUtil.readLine(labelInputStream).get(0)
    labelInputStream.close()
    labels = JSON.parseObject(jsonStr, classOf[util.Map[_, _]])

    val spark: SparkSession = SparkSession.builder.appName("NaiveBayes").master("local").getOrCreate

    hashingTF= HashingTF.load(DataETL.HDFS_PATH.concat(DataETL.TF_PATH))
    idfModel= IDFModel.load(DataETL.HDFS_PATH.concat(DataETL.IDF_PATH))
    model= NaiveBayesModel.load(spark.sparkContext, DataETL.HDFS_PATH.concat(DataETL.MODEL_PATH))

    val hbaseUtil = new HbaseUtil

    //批量
    batchTestModel(spark,hbaseUtil,DataETL.HDFS_PATH.concat(DataETL.DATA_TEST_PATH))

    //单个
    testModel(spark, "伊拉克开始进攻美国")
  }
}
