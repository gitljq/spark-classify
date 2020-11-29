package com.ljq.spark.classify

import com.ljq.spark.classify.service.{DataETL, NaiveBayesTest}
import com.ljq.spark.classify.utils.{HbaseUtil, HdfsUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem

/**
 * @Description:
 * @Author: ljq_dmr
 * @Date: 2020/11/28 20:17
 */
object main {
  def main(args: Array[String]): Unit = {
    val hbaseUtil = new HbaseUtil
//    hbaseUtil.createTable("news","classify_test","f",96)
    println(hbaseUtil.getRowData("news:classify_test", "11709", "f", NaiveBayesTest.col))
  }
}
