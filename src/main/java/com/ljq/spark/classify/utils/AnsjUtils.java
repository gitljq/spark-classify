package com.ljq.spark.classify.utils;

import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.NlpAnalysis;

import java.util.ArrayList;
import java.util.List;


/**
 * @Description:
 * @Author: ljq_dmr
 * @Date: 2020/11/26 20:20
 */

public class AnsjUtils {

    public static void main(String[] args) {
        String text = "我是一枚苦逼程序员！";

        List<String> wordList = participle(text);
        String data = participleText(text);
        System.out.println(data);

    }

    public static List<String> participle(String text){
        List<String> wordList = new ArrayList<String>();
        Result result = NlpAnalysis.parse(text);
        List<Term> terms = result.getTerms();
        for(Term term:terms){
            String name = term.getName();
            wordList.add(name);
        }
        return wordList;
    }

    public static String participleText(String text) {
        StringBuffer wordText = new StringBuffer();
        Result result = NlpAnalysis.parse(text);
        List<Term> terms = result.getTerms();

        for (int i = 0; i < terms.size(); i++) {
            Term term = terms.get(i);
            String word = term.getName();
            if(i!=0){
                wordText.append(" ");
            }
            wordText.append(word);
        }

        return wordText.toString();
    }

}