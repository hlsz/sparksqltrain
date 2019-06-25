package com.data.model;

import org.apache.spark.ml.linalg.Vector;

import java.lang.reflect.Array;
import java.util.List;

public class MiniTrainningData {

    private Long eleShopId;
    private Long category;
    private Vector features;

    public Long getEleShopId() {
        return eleShopId;
    }

    public void setEleShopId(Long eleShopId) {
        this.eleShopId = eleShopId;
    }

    public Long getCategory() {
        return category;
    }

    public void setCategory(Long category) {
        this.category = category;
    }

    public Vector getFeatures() {
        return features;
    }

    public void setFeatures(Vector features) {
        this.features = features;
    }
}
