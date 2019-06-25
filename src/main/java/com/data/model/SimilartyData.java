package com.data.model;

public class SimilartyData {
    private Long mtShopId;
    private Long eleShopId;
    private Double similarty;
    private Long categoryId;

    public Long getMtShopId() {
        return mtShopId;
    }

    public void setMtShopId(Long mtShopId) {
        this.mtShopId = mtShopId;
    }

    public Long getEleShopId() {
        return eleShopId;
    }

    public void setEleShopId(Long eleShopId) {
        this.eleShopId = eleShopId;
    }

    public Double getSimilarty() {
        return similarty;
    }

    public void setSimilarty(Double similarty) {
        this.similarty = similarty;
    }

    public Long getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(Long categoryId) {
        this.categoryId = categoryId;
    }
}
