package com.zuoshangs.spark.domain.model;

/**
 * Created by weike on 2018/1/17.
 */
public class User {
    private String outerUserId;
    private String bizUserId;

    public String getOuterUserId() {
        return outerUserId;
    }

    public void setOuterUserId(String outerUserId) {
        this.outerUserId = outerUserId;
    }

    public String getBizUserId() {
        return bizUserId;
    }

    public void setBizUserId(String bizUserId) {
        this.bizUserId = bizUserId;
    }
}
