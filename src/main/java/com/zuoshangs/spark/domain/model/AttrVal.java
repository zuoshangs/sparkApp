package com.zuoshangs.spark.domain.model;

import java.util.Date;

/**
 * Created by weike on 2018/4/11.
 */
public class AttrVal {
    private Integer id;

    private String name;

    private String rule;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getRule() {
        return rule;
    }

    public void setRule(String rule) {
        this.rule = rule;
    }
}
