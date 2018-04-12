package com.zuoshangs.spark.domain.model;

import java.util.List;

/**
 * Created by weike on 2018/4/11.
 */
public class Attr {
    private int id;
    private String name;
    private List<AttrVal> attrValList;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<AttrVal> getAttrValList() {
        return attrValList;
    }

    public void setAttrValList(List<AttrVal> attrValList) {
        this.attrValList = attrValList;
    }
}
