package com.ljq.spark.classify.utils

import java.io._
import java.util

/**
 * @Description: 文件处理类
 * @Author: ljq_dmr
 * @Date: 2020/11/29 11:41
 */
class FileUtil {

  /**
   * 根据流按行读取
   * @param inputStream
   * @return
   */
  def readLine(inputStream: InputStream):util.ArrayList[String]= {
    val lineList = new util.ArrayList[String]()
    val inReader = new InputStreamReader(inputStream)
    val reader = new BufferedReader(inReader)
    var line = reader.readLine();
   while (line!=null) {
     lineList.add(line)
     line=reader.readLine()
   }
    reader.close()
    inReader.close()
    lineList
  }

  /**
   * 获取全部行数
   * @param inputStream
   * @return
   */
  def getLineNumber(inputStream: InputStream): Long = {
    try {
      val reader = new InputStreamReader(inputStream)
      val lineNumberReader = new LineNumberReader(reader)
      lineNumberReader.skip(Integer.MAX_VALUE)
      val lines: Long = lineNumberReader.getLineNumber + 1
      reader.close()
      lineNumberReader.close()
      return lines
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
    0
  }
/*
  def readLine( filePath: String, charsetName: String): util.List[String] = {
    val textList = new util.ArrayList[String]
    try {
      val in = new FileInputStream(filePath)
      val inReader = new InputStreamReader(in, charsetName)
      val bufReader = new BufferedReader(inReader)
      var line: String = null
      while ( {
        (line = bufReader.readLine) != null
      }) {
        val text: String = bufReader.readLine()
        if (text != null) textList.add(text)
      }
      bufReader.close()
      inReader.close()
      in.close()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    textList
  }*/
}
