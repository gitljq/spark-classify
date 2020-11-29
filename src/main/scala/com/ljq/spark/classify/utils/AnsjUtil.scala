package com.ljq.spark.classify.utils

import java.util
import java.util.{ArrayList, List}

import org.ansj.domain.{Result, Term}
import org.ansj.splitWord.analysis.NlpAnalysis

/**
 * @Description:
 * @Author: ljq_dmr
 * @Date: 2020/11/29 15:43
 */
class AnsjUtil {
  def participle(text: String): util.List[String] = {
    val wordList = new util.ArrayList[String]
    val result: Result = NlpAnalysis.parse(text)
    val terms: util.List[Term] = result.getTerms
    import scala.collection.JavaConversions._
    for (term <- terms) {
      val name: String = term.getName
      wordList.add(name)
    }
    wordList
  }

  def participleText(text: String): String = {
    val wordText = new StringBuffer
    val result: Result = NlpAnalysis.parse(text)
    val terms: util.List[Term] = result.getTerms
    for (i <- 0 until terms.size) {
      val term: Term = terms.get(i)
      val word: String = term.getName
      if (i != 0) wordText.append(" ")
      wordText.append(word)
    }
    wordText.toString
  }

}
