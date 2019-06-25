package com.data.model;

public class MeiTuanData {
    private Long shopId;
    private String shopName;
    private String GoodsSegment;

    public String getGoodsSegment() {
        return GoodsSegment;
    }

    public void setGoodsSegment(String goodsSegment) {
        GoodsSegment = goodsSegment;
    }

    public Long getShopId() {
        return shopId;
    }

    public void setShopId(Long shopId) {
        this.shopId = shopId;
    }

    public String getShopName() {
        return shopName;
    }

    public void setShopName(String shopName) {
        this.shopName = shopName;
    }
}
