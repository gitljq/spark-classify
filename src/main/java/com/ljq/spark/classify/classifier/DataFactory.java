package com.ljq.spark.classify.classifier;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ljq.spark.classify.utils.AnsjUtils;
import com.ljq.spark.classify.utils.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * 1
 */
public class DataFactory {

    public static final String CLASS_PATH = "D:\\IdeaProjects\\spark-classify\\src\\main\\resources";

    public static final String STOP_WORD_PATH = CLASS_PATH+"\\data\\stopWord.txt";

    public static final String NEWS_DATA_PATH = CLASS_PATH+"\\data\\news";

    public static final String DATA_TRAIN_PATH = CLASS_PATH+"\\data\\data-train.txt";

    public static final String DATA_TEST_PATH = CLASS_PATH+"\\data\\data-test.txt";

    public static final String MODELS = CLASS_PATH+"\\models";

    public static final String MODEL_PATH = CLASS_PATH+"\\models\\category-4";

    public static final String LABEL_PATH = CLASS_PATH+"\\models\\labels.txt";

    public static final String TF_PATH = CLASS_PATH+"\\models\\tf";

    public static final String IDF_PATH = CLASS_PATH+"\\models\\idf";

    public static void main(String[] args) throws IOException {

        List<String> stopWords = FileUtils.readLine(line -> line,STOP_WORD_PATH);
        File file = new File(MODELS);
        if(!file.exists()){
            file.mkdirs();
        }
        FileUtils.deleteFile(DATA_TRAIN_PATH);
        FileUtils.deleteFile(DATA_TEST_PATH);
        FileUtils.deleteFile(LABEL_PATH);

        Double spiltRate = 0.8;//solit rate

        Map<Integer,String> labels = new HashMap<>();
        String[] data = new File(NEWS_DATA_PATH).list();
        if(null==data || data.length==0){
            new Exception("data is null").printStackTrace();
            return;
        }
        int i = 0;
        for (String datum : data) {
            System.out.println("正在对" + datum + "分词");
            labels.put(i++, datum.substring(0, datum.indexOf(".")));
            String fileDirPath = String.format("%s\\%s", NEWS_DATA_PATH, datum);
            InputStreamReader inr = new InputStreamReader(new FileInputStream(fileDirPath));

            BufferedReader bf = new BufferedReader(inr);
            String dataLine;
            int spilt = Double.valueOf(FileUtils.getLineNumber(new File(fileDirPath)) * spiltRate).intValue();

            int count = 0;
            while ((dataLine = bf.readLine()) != null) {
                count++;
                if (count % 10000 == 0) {
                    System.out.println();
                }
                for (String stopWord : stopWords) {
                    dataLine = dataLine.replaceAll(stopWord, "");
                }
                if (StringUtils.isBlank(dataLine)) {
                    continue;
                }

                JSONObject jsonObject = new JSONObject();
                jsonObject.put("text", AnsjUtils.participle(dataLine));
                jsonObject.put("category", Double.valueOf(i));

                if (count > spilt) {
                    //test data
                    FileUtils.appendText(DATA_TEST_PATH, jsonObject.toJSONString() + "\n");
                } else {
                    //train data
                    FileUtils.appendText(DATA_TRAIN_PATH, jsonObject.toJSONString() + "\n");
                }

            }
        }

        FileUtils.writer(LABEL_PATH, JSON.toJSONString(labels));//data labels

        System.out.println("Data processing successfully !");
        System.out.println("=======================================================");
        System.out.println("trainData:"+DATA_TRAIN_PATH);
        System.out.println("testData:"+DATA_TEST_PATH);
        System.out.println("labes:"+LABEL_PATH);
        System.out.println("=======================================================");

    }

}
