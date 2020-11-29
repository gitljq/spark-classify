package com.ljq.spark.classify.dto;

/**
 *DTO数据传输对象
 */
public class Result {

    private Long category;//text category

    private Long predict;//predict category

    public boolean isCorrect(){
        return category.equals(predict);
    }

    public Result() {
    }

    public Result(Long category, Long predict) {
        this.category = category;
        this.predict = predict;
    }

    public Long getCategory() {
        return category;
    }

    public void setCategory(Long category) {
        this.category = category;
    }

    public Long getPredict() {
        return predict;
    }

    public void setPredict(Long predict) {
        this.predict = predict;
    }
}
