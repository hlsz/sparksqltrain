package com.data.model;

import java.lang.reflect.Array;

public class TrainningFeature {

    private String category;
    private String shopId;
    private String shopName;
    private Integer vectorSize;
    private String vectorIndices;
    private String vectorValues;

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getShopId() {
        return shopId;
    }

    public void setShopId(String shopId) {
        this.shopId = shopId;
    }

    public String getShopName() {
        return shopName;
    }

    public void setShopName(String shopName) {
        this.shopName = shopName;
    }

    public Integer getVectorSize() {
        return vectorSize;
    }

    public void setVectorSize(Integer vectorSize) {
        this.vectorSize = vectorSize;
    }

    public String getVectorIndices() {
        return vectorIndices;
    }

    public void setVectorIndices(String vectorIndices) {
        this.vectorIndices = vectorIndices;
    }

    public String getVectorValues() {
        return vectorValues;
    }

    public void setVectorValues(String vectorValues) {
        this.vectorValues = vectorValues;
    }
}
