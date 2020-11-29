package com.ljq.spark.classify.utils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class FileUtils {
    /**
     * get project path
     * @return
     */
    public static String getClassPath(){
        String classPath = FileUtils.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        return new File(classPath).getPath();
    }

    /**
     * 先根遍历序递归删除文件夹
     *
     * @param dirFile 要被删除的文件或者目录
     * @return 删除成功返回true, 否则返回false
     */
    public static boolean deleteFile(File dirFile) {
        // 如果dir对应的文件不存在，则退出
        if (!dirFile.exists()) {
            return false;
        }

        if (dirFile.isFile()) {
            return dirFile.delete();
        } else {

            for (File file : dirFile.listFiles()) {
                deleteFile(file);
            }
        }

        return dirFile.delete();
    }

    /**
     * delete file
     * @param filePath filePath
     */
    public static void deleteFile(String filePath){
        File file = new File(filePath);
        deleteFile(file);
        System.out.println("delete file success ->" + file.getAbsolutePath());
    }

    /**
     * read file
     * @param filePath filePath
     * @return result
     */
    public static String readFile(String filePath) {
        File file = new File(filePath);
        FileReader reader = null;
        try {
            reader = new FileReader(filePath);
            int fileLen = (int) file.length();
            char[] chars = new char[fileLen];
            try {
                reader.read(chars);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            String text = String.valueOf(chars);
            return text;
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    /**
     * writer file-append content
     * @param filePath filePath
     * @param content append content
     */
    public static void appendText(String filePath, String content) {
        try {
            FileWriter writer = new FileWriter(filePath, true);
            writer.write(content);
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * writer file
     * @param filePath filePath
     * @param content file content
     */
    public static void writer(String filePath, String content) {
        File file = new File(filePath);

        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(content);
            bw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * read file（line）line read
     * @param fileRead callback function
     * @param filePath filePath
     * @param charsetName charsetName
     * @return result
     */
    public static List<String> readLine(FileReadFunction fileRead, String filePath, String charsetName) {
        List<String> textList = new ArrayList<>();
        try {
            FileInputStream in = new FileInputStream(filePath);
            InputStreamReader inReader = new InputStreamReader(in, charsetName);
            BufferedReader bufReader = new BufferedReader(inReader);
            String line = null;
            while ((line = bufReader.readLine()) != null) {
                String text = fileRead.readLine(line);
                if (text != null) {
                    textList.add(text);
                }
            }
            bufReader.close();
            inReader.close();
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return textList;
    }

    /**
     * read file（line）line read
     * @param fileRead callback function
     * @param filePath filePath
     * @return result
     */
    public static List<String> readLine(FileReadFunction fileRead, String filePath) {
        return readLine(fileRead, filePath, "UTF-8");
    }

    public static long getLineNumber(File file) {
        if (file.exists()) {
            try {
                FileReader fileReader = new FileReader(file);
                LineNumberReader lineNumberReader = new LineNumberReader(fileReader);
                lineNumberReader.skip(Long.MAX_VALUE);
                long lines = lineNumberReader.getLineNumber() + 1;
                fileReader.close();
                lineNumberReader.close();
                return lines;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return 0;
    }

}
