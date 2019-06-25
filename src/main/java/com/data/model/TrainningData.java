package com.data.model;

public class TrainningData {
    private Long shopId;
    private String shopName;
    private Long category;
    private String goodsSegment;

    public String getGoodsSegment() {
        return goodsSegment;
    }

    public void setGoodsSegment(String goodsSegment) {
        this.goodsSegment = goodsSegment;
    }

    public Long getShopId() {
        return shopId;
    }

    public void setShopId(Long shopId) {
        this.shopId = shopId;
    }

    public Long getCategory() {
        return category;
    }

    public void setCategory(Long category) {
        this.category = category;
    }

    public String getShopName() {
        return shopName;
    }

    public void setShopName(String shopName) {
        this.shopName = shopName;
    }
}
