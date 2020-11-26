package com.ljq.spark.classify.utils;

import java.io.Serializable;

/**
 *
 */
public interface FileReadFunction extends Serializable {

    String readLine(String line);

}
