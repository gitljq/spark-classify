package com.ljq.spark.classify.service

import java.io.{BufferedReader, File, FileInputStream, InputStream, InputStreamReader}
import java.util

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.ljq.spark.classify.utils.{AnsjUtils, FileUtil, HbaseUtil, HdfsUtil}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable.ListBuffer
import scala.tools.nsc.classpath.FileUtils


/**
 * @Description: 分词处理，单线程，按类型读取多个hdfs文件，分词后写到hdfs的两个文件，一个是训练集，一个是测试集
 * @Author: ljq_dmr
 * @Date: 2020/11/28 20:11
 */
object DataETL {
  val HDFS_PATH = "hdfs://192.168.80.101:9000"

  val MAIN_PATH = "/sparkdata/classify/"

  val STOP_WORD_PATH: String = "D:\\IdeaProjects\\spark-classify\\src\\main\\resources\\data\\stopWord.txt"

  val NEWS_DATA_PATH: String = MAIN_PATH + "news/"

  val DATA_TRAIN_PATH: String = MAIN_PATH + "data-train"

  val DATA_TEST_PATH: String = MAIN_PATH + "data-test"

  val MODEL_PATH: String = MAIN_PATH + "models/category-4"

  val LABEL_PATH: String = MAIN_PATH + "models/labels.txt"

  val TF_PATH: String = MAIN_PATH + "models/tf"

  val IDF_PATH: String = MAIN_PATH + "models/idf"

  val col:Array[String] = Array("id","text","split","category")

  def initFile(fileSystem: FileSystem,hdfsUtil: HdfsUtil,path:String): Unit = {
    if (hdfsUtil.exists(fileSystem, path)) {
      hdfsUtil.deleteFile(fileSystem, path)
      hdfsUtil.createFile(fileSystem,path)
    } else{
      hdfsUtil.createFile(fileSystem,path)
    }
  }

  def main(args: Array[String]): Unit = {
    val fileUtil = new FileUtil
    val stopWordList = fileUtil.readLine(new FileInputStream(new File(STOP_WORD_PATH)))
    val spiltRate = 0.8 //solit rate

    val configuration: Configuration = new Configuration()
    configuration.set("fs.defaultFS", HDFS_PATH)
    configuration.setBoolean("dfs.support.append", true)
    val fileSystem: FileSystem = FileSystem.get(configuration)
    val hdfs = new HdfsUtil

    initFile(fileSystem, hdfs, DATA_TRAIN_PATH)
    initFile(fileSystem,hdfs,DATA_TEST_PATH)
    initFile(fileSystem, hdfs, LABEL_PATH)

    val hbaseUtil = new HbaseUtil

    val fileNameList: ListBuffer[String] = hdfs.getHDFSFilesName(fileSystem, NEWS_DATA_PATH)

    val labels = new java.util.HashMap[Integer, String]

    var count:Integer = 0
    var allCount:Integer = 0
    fileNameList.foreach(fileName=> {
      println("正在对".concat(fileName).concat("分词处理"))
      count = count+1
      labels.put(count, fileName.replace(".txt", ""))
      val inputStream: InputStream = hdfs.getFile(fileSystem, NEWS_DATA_PATH+fileName)
      val lineCount: Long = fileUtil.getLineNumber(inputStream)
      inputStream.close()
      val spilt = lineCount * spiltRate

      val stream: InputStream = hdfs.getFile(fileSystem, NEWS_DATA_PATH + fileName)
      val inReader = new InputStreamReader(stream)
      val reader = new BufferedReader(inReader)
      var line = reader.readLine();
      var currentLine = 0
      var trainDataList = new ListBuffer[String]
      var testDataList = new ListBuffer[String]
      while (line!=null) {
        currentLine= currentLine+1
        allCount = allCount+1
        //去除停用词
        import scala.collection.JavaConversions._
        for (stopWord <- stopWordList) {
          line = line.replaceAll(stopWord, "")
        }

        if (!StringUtils.isBlank(line)){
          //分词
          val jsonObject: JSONObject = new JSONObject
          val splitWords: util.List[String] = AnsjUtils.participle(line)
          jsonObject.put("id",allCount)
          jsonObject.put("text", splitWords)
          jsonObject.put("category", count)
          val colData:Array[String] = Array(allCount+"",line+"",splitWords+"",count+"")
          hbaseUtil.putData("news:classify",allCount+"","f",col,colData)
          //写入分词后文件
          if (currentLine > spilt) {
//            hdfs.appendText(fileSystem,new Path(DATA_TEST_PATH), jsonObject.toJSONString + "\n")
            testDataList.add(jsonObject.toJSONString+"\n")
          } else {
//            hdfs.appendText(fileSystem,new Path(DATA_TRAIN_PATH), jsonObject.toJSONString + "\n")
            trainDataList.add(jsonObject.toJSONString+"\n")
          }
        }
        line=reader.readLine()
      }
      hdfs.appendBatchText(fileSystem, new Path(DATA_TEST_PATH), testDataList)
      hdfs.appendBatchText(fileSystem, new Path(DATA_TRAIN_PATH), trainDataList)
      reader.close()
      inReader.close()
      stream.close()
    })

    hdfs.appendText(fileSystem,new Path(LABEL_PATH),JSON.toJSONString(labels, SerializerFeature.EMPTY: _*))
    println("Data processing successfully !")
    println("=======================================================")
    println("trainData:" + DATA_TRAIN_PATH)
    println("testData:" + DATA_TEST_PATH)
    println("labes:" + LABEL_PATH)
    println("=======================================================")

    fileSystem.close()
  }
}
