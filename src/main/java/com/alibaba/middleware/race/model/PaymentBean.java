package com.alibaba.middleware.race.model;

/**
 * Created by iceke on 16/7/1.
 */
public class PaymentBean {
    private double totalPrice;

    //0 or 1
    private short  platform;

    public PaymentBean(double totalPrice, short platform){
        this.totalPrice = totalPrice;
        this.platform = platform;
    }

    public void setPlatform(short paltform) {
        this.platform = paltform;
    }

    public void setTotalPrice(double totalPrice) {
        this.totalPrice = totalPrice;
    }

    public double getTotalPrice() {
        return totalPrice;
    }

    public short getPlatform() {
        return platform;
    }
}
