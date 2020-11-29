package com.ljq.spark.classify.service

import com.ljq.spark.classify.utils.HdfsUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.ml.feature.{HashingTF, IDF, IDFModel}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
 * @Description:
 * @Author: ljq_dmr
 * @Date: 2020/11/28 20:44
 */
object NaiveBayesTrain {
  def main(args: Array[String]): Unit = {
    exec()
  }


  def initDir(fileSystem: FileSystem, hdfsUtil: HdfsUtil, path: String) = {
    if (hdfsUtil.exists(fileSystem,path)) {
      hdfsUtil.deleteFile(fileSystem,path)
    }
//    hdfsUtil.createFolder(fileSystem,path)
  }

  def exec(): Unit ={
    if (null == System.getenv("HADOOP_HOME")) new Exception("请安装并配置Hadoop环境").printStackTrace()

    val configuration: Configuration = new Configuration()
    configuration.set("fs.defaultFS", DataETL.HDFS_PATH)
    configuration.setBoolean("dfs.support.append", true)
    val fileSystem: FileSystem = FileSystem.get(configuration)
    val hdfsUtil = new HdfsUtil

    initDir(fileSystem,hdfsUtil,DataETL.MODEL_PATH)
    initDir(fileSystem,hdfsUtil,DataETL.TF_PATH)
    initDir(fileSystem,hdfsUtil,DataETL.IDF_PATH)

    if (!hdfsUtil.exists(fileSystem, DataETL.DATA_TRAIN_PATH)) {
      new RuntimeException(DataETL.DATA_TRAIN_PATH.concat("is not exists")).printStackTrace()
      return
    }

    val spark: SparkSession = SparkSession.builder.appName("NaiveBayes").master("local").getOrCreate

    val train: Dataset[Row] = spark.read.json(DataETL.HDFS_PATH.concat(DataETL.DATA_TRAIN_PATH))

    //词频
    val hashingTF: HashingTF = new HashingTF().setNumFeatures(500000).setInputCol("text").setOutputCol("rawFeatures")
    val featurizedData: Dataset[Row] = hashingTF.transform(train)

    //count tf-idf
    val idf: IDF = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel: IDFModel = idf.fit(featurizedData)
    val rescaledData: Dataset[Row] = idfModel.transform(featurizedData)

    val trainDataRdd = rescaledData.select("category", "features").rdd.map((v1: Row) => {
      def foo(v1: Row) = {
        val category:Long = v1.getAs("category")
        val features: SparseVector = v1.getAs("features")
        val featuresVector: Vector = Vectors.dense(features.toArray)
        new LabeledPoint(category, featuresVector)
      }

      foo(v1)
    })

    System.out.println("============开始训练============")
    val model: NaiveBayesModel = NaiveBayes.train(trainDataRdd)
    model.save(spark.sparkContext, DataETL.HDFS_PATH.concat(DataETL.MODEL_PATH)) //save model

    hashingTF.save(DataETL.HDFS_PATH.concat(DataETL.TF_PATH))

    idfModel.save(DataETL.HDFS_PATH.concat(DataETL.IDF_PATH))


    System.out.println("============训练成功============")
    System.out.println("=======================================================")
    System.out.println("modelPath:" + DataETL.HDFS_PATH.concat(DataETL.MODEL_PATH))
    System.out.println("tfPath:" + DataETL.HDFS_PATH.concat(DataETL.TF_PATH))
    System.out.println("idfPath:" + DataETL.HDFS_PATH.concat(DataETL.IDF_PATH))
    System.out.println("=======================================================")
  }
}
